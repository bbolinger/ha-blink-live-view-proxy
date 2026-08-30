"""Minimal RTSP client for Blink cameras that do not speak IMMI.

Why this exists
---------------
Blink hands roughly half its camera models an `rtsps://` URL instead of the
proprietary `immis://` transport. Feeding that URL straight to ffmpeg fails with
`CSeq 2 expected, 1 received`, and the camera never even wakes.

Probing the server by hand showed why: it is a non-compliant RTSP
implementation (`a=tool:immedia_isi108`) that **always answers with `CSeq: 1`**
regardless of the sequence number in the request. ffmpeg validates CSeq strictly
and aborts the session. Nothing else is wrong: the server answers OPTIONS and
DESCRIBE happily to an anonymous client, with no token, no command polling and
no keepalive required.

So this module does the handshake itself, ignoring the bogus CSeq, and relays
the media as a plain MPEG-TS byte stream on a local TCP socket. That is exactly
the shape the IMMI path already produces, so the HLS pipeline consumes it
unchanged.

The SDP advertises `m=video <port> RTP/AVP 33` with `a=rtpmap:33 MP2T/90000`, so
each RTP packet's payload is raw MPEG-TS. Strip the RTP header, concatenate the
payloads, and the result is a valid transport stream.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import urllib.parse
from typing import Any

from .constants import LOGGER_NAME

LOGGER = logging.getLogger(LOGGER_NAME)

RTP_HEADER_BYTES = 12
RTP_EXTENSION_FLAG = 0x10
RTP_CSRC_MASK = 0x0F
INTERLEAVE_MARKER = 0x24  # '$'
KEEPALIVE_SECONDS = 20

# A consumer that stops reading must not be allowed to buffer without bound.
# Live video is better dropped than queued, so a client whose socket backs up
# past this is disconnected rather than allowed to grow the heap.
MAX_CLIENT_BACKLOG_BYTES = 4 * 1024 * 1024

# Retry the RTSP session if it drops early, which covers a transient network
# blip. Blink's liveview is a finite session, so a stream that ran for a while
# has simply reached its end and reconnecting to the same URL cannot work.
RECONNECT_ATTEMPTS = 3
RECONNECT_BACKOFF_SECONDS = 1.0
SESSION_SETTLED_SECONDS = 30.0


def _parse_sdp_control(sdp: str, base_url: str) -> str:
    """Return the absolute control URL for the first media track."""
    control = None
    for line in sdp.splitlines():
        line = line.strip()
        if line.startswith("a=control:"):
            value = line.split(":", 1)[1].strip()
            if value and value != "*":
                control = value
                break
    if not control:
        return base_url
    if control.startswith(("rtsp://", "rtsps://")):
        return control
    separator = "" if base_url.endswith("/") else "/"
    # The URL carries a query string that the server needs, so the track has to
    # be spliced in before it rather than appended to the end.
    parsed = urllib.parse.urlsplit(base_url)
    path = f"{parsed.path}{separator}{control}"
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, path, parsed.query, parsed.fragment)
    )


def _strip_rtp_header(packet: bytes) -> bytes:
    """Return the payload of one RTP packet, or b"" if it is malformed."""
    if len(packet) < RTP_HEADER_BYTES:
        return b""
    first = packet[0]
    offset = RTP_HEADER_BYTES + (first & RTP_CSRC_MASK) * 4
    if first & RTP_EXTENSION_FLAG:
        if len(packet) < offset + 4:
            return b""
        extension_words = int.from_bytes(packet[offset + 2 : offset + 4], "big")
        offset += 4 + extension_words * 4
    if len(packet) <= offset:
        return b""
    return packet[offset:]


class BlinkRtspRelay:
    """Pulls an RTSP stream from Blink and republishes it as local MPEG-TS."""

    def __init__(self, url: str, config: dict[str, Any]):
        self.url = url
        self.config = config
        self.server: asyncio.AbstractServer | None = None
        self.clients: list[asyncio.StreamWriter] = []
        self.pump_task: asyncio.Task[None] | None = None
        self.upstream_writer: asyncio.StreamWriter | None = None
        self.session_id: str | None = None
        self._host = "127.0.0.1"
        self._port = 0
        self._sequence = 0
        self.playing = asyncio.Event()
        self._first_media_logged = False
        self._play_time = 0.0
        self._closing = False

    # ---------------- local side ----------------

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self._on_client, self._host, 0
        )
        self._port = int(self.server.sockets[0].getsockname()[1])
        self.pump_task = asyncio.create_task(self._pump(), name="blink-rtsp-pump")
        # Block until PLAY has actually succeeded. Returning early meant ffmpeg
        # was launched against a socket with nothing on it yet and burned its
        # whole probe window on silence, which cost several seconds before the
        # first HLS segment could be written.
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.playing.wait(), timeout=25)

    @property
    def tcp_url(self) -> str:
        return f"tcp://{self._host}:{self._port}"

    async def _on_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self.clients.append(writer)
        try:
            # ffmpeg only reads; wait until it goes away.
            await reader.read()
        except Exception:
            pass
        finally:
            with contextlib.suppress(ValueError):
                self.clients.remove(writer)
            with contextlib.suppress(Exception):
                writer.close()

    def _broadcast(self, payload: bytes) -> None:
        for writer in list(self.clients):
            if writer.is_closing():
                continue
            # Never await here. Draining would stall the read loop, Blink would
            # keep sending, and the upstream socket would back up instead. For
            # live video the right answer is to drop the slow consumer.
            transport = writer.transport
            if transport is not None:
                backlog = transport.get_write_buffer_size()
                if backlog > MAX_CLIENT_BACKLOG_BYTES:
                    LOGGER.warning(
                        "RTSP relay: consumer is %d bytes behind, dropping it",
                        backlog,
                    )
                    with contextlib.suppress(ValueError):
                        self.clients.remove(writer)
                    with contextlib.suppress(Exception):
                        writer.close()
                    continue
            try:
                writer.write(payload)
            except Exception:
                with contextlib.suppress(ValueError):
                    self.clients.remove(writer)

    # ---------------- upstream side ----------------

    def _next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence

    async def _frame_or_response(
        self, reader: asyncio.StreamReader
    ) -> tuple[str, int, bytes]:
        """Read exactly one unit from the shared connection.

        Control and data share the socket in interleaved TCP mode: a `$` byte
        introduces a data frame, anything else begins an RTSP response. Reading
        blindly for a CRLF pair swallows video and loses framing, because Blink
        starts sending as soon as SETUP is answered, sometimes while the rest of
        the handshake is still in flight.

        Credit to @fritzzetik, who identified this in PR #4 upstream; this
        mirrors the shape of that fix.
        """
        first = await reader.readexactly(1)
        if first[0] == INTERLEAVE_MARKER:
            header = await reader.readexactly(3)
            channel = header[0]
            length = int.from_bytes(header[1:3], "big")
            return "frame", channel, await reader.readexactly(length)
        rest = await reader.readuntil(b"\r\n\r\n")
        return "response", 0, first + rest

    async def _await_response(self, reader: asyncio.StreamReader) -> bytes:
        """Return the next RTSP response, relaying any data frames in between."""
        while True:
            kind, channel, data = await self._frame_or_response(reader)
            if kind == "response":
                return data
            self._dispatch_frame(channel, data)

    def _dispatch_frame(self, channel: int, packet: bytes) -> None:
        """Channel 0 is video; channel 1 is RTCP and is discarded."""
        if channel != 0:
            return
        payload = _strip_rtp_header(packet)
        if not payload:
            return
        if not self._first_media_logged:
            self._first_media_logged = True
            LOGGER.info(
                "RTSP relay: first media byte %.2fs after PLAY "
                "(this gap is the camera waking, not the pipeline)",
                asyncio.get_running_loop().time() - self._play_time,
            )
        self._broadcast(payload)

    async def _request(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        method: str,
        url: str,
        extra: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], str]:
        headers = {"CSeq": str(self._next_sequence()), "User-Agent": "blink-liveview-proxy"}
        headers.update(extra or {})
        if self.session_id:
            headers.setdefault("Session", self.session_id)
        request = f"{method} {url} RTSP/1.0\r\n"
        request += "".join(f"{k}: {v}\r\n" for k, v in headers.items())
        request += "\r\n"
        writer.write(request.encode())
        await writer.drain()

        head = await asyncio.wait_for(self._await_response(reader), timeout=15)
        text = head.decode(errors="replace")
        status_line, _, rest = text.partition("\r\n")
        match = re.match(r"RTSP/1\.\d\s+(\d+)", status_line)
        status = int(match.group(1)) if match else 0
        parsed: dict[str, str] = {}
        for line in rest.split("\r\n"):
            if ":" in line:
                key, _, value = line.partition(":")
                parsed[key.strip().lower()] = value.strip()
        body = ""
        length = int(parsed.get("content-length", "0") or 0)
        if length:
            raw = await asyncio.wait_for(reader.readexactly(length), timeout=15)
            body = raw.decode(errors="replace")
        # NOTE: the CSeq in the reply is deliberately NOT checked. Blink's server
        # always answers "CSeq: 1", which is what makes ffmpeg refuse the stream.
        return status, parsed, body

    async def _pump(self) -> None:
        """Run the RTSP session, retrying if it drops before it has settled."""
        loop = asyncio.get_running_loop()
        for attempt in range(1, RECONNECT_ATTEMPTS + 1):
            if self._closing:
                break
            started = loop.time()
            try:
                await self._session_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("RTSP relay attempt %d failed", attempt)

            if self._closing:
                break
            elapsed = loop.time() - started
            if elapsed >= SESSION_SETTLED_SECONDS:
                LOGGER.info(
                    "RTSP relay: stream ended after %.0fs, not reconnecting "
                    "(Blink live view sessions are finite)",
                    elapsed,
                )
                break
            if attempt >= RECONNECT_ATTEMPTS:
                LOGGER.warning("RTSP relay: giving up after %d attempts", attempt)
                break
            delay = RECONNECT_BACKOFF_SECONDS * attempt
            LOGGER.warning(
                "RTSP relay: dropped after %.1fs, reconnecting in %.0fs",
                elapsed,
                delay,
            )
            await asyncio.sleep(delay)

        # Never leave start() blocked on a session that is not coming back.
        self.playing.set()

    async def _session_once(self) -> None:
        parsed = urllib.parse.urlparse(self.url)
        host = parsed.hostname
        port = parsed.port or 322
        if not host:
            LOGGER.error("RTSP relay: cannot parse host from liveview URL")
            return

        # A Session id from a previous attempt is meaningless on a new
        # connection and must not be sent as a header.
        self.session_id = None
        # Stamp now, not after PLAY: media can arrive during the handshake and
        # the "first media byte" timing would otherwise be nonsense.
        self._play_time = asyncio.get_running_loop().time()
        reader = writer = None
        try:
            reader, writer = await asyncio.open_connection(host, port, ssl=True)
            self.upstream_writer = writer

            status, _, sdp = await self._request(reader, writer, "DESCRIBE", self.url,
                                                 {"Accept": "application/sdp"})
            if status != 200:
                LOGGER.error("RTSP relay: DESCRIBE returned %s", status)
                return

            control_url = _parse_sdp_control(sdp, self.url)
            status, headers, _ = await self._request(
                reader, writer, "SETUP", control_url,
                {"Transport": "RTP/AVP/TCP;unicast;interleaved=0-1"},
            )
            if status != 200:
                LOGGER.error("RTSP relay: SETUP returned %s", status)
                return
            session = headers.get("session", "")
            self.session_id = session.split(";")[0].strip() or None

            status, _, _ = await self._request(reader, writer, "PLAY", self.url,
                                               {"Range": "npt=now-"})
            if status != 200:
                LOGGER.error("RTSP relay: PLAY returned %s", status)
                return

            self._play_time = asyncio.get_running_loop().time()
            LOGGER.info("RTSP relay: playing, session=%s", self.session_id)
            self.playing.set()
            keepalive = asyncio.create_task(self._keepalive(reader, writer))
            try:
                await self._read_interleaved(reader)
            finally:
                keepalive.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await keepalive
        finally:
            # Deliberately NOT setting self.playing here. start() must stay
            # blocked across a reconnect, otherwise ffmpeg is launched against a
            # socket that is about to be reconnected. _pump sets it once the
            # session is genuinely playing or the retries are exhausted.
            if writer is not None:
                with contextlib.suppress(Exception):
                    writer.close()

    async def _keepalive(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Blink tears the session down after continue_interval without traffic."""
        while True:
            await asyncio.sleep(KEEPALIVE_SECONDS)
            headers = {
                "CSeq": str(self._next_sequence()),
                "User-Agent": "blink-liveview-proxy",
            }
            if self.session_id:
                headers["Session"] = self.session_id
            request = f"OPTIONS {self.url} RTSP/1.0\r\n"
            request += "".join(f"{k}: {v}\r\n" for k, v in headers.items())
            request += "\r\n"
            writer.write(request.encode())
            await writer.drain()

    async def _read_interleaved(self, reader: asyncio.StreamReader) -> None:
        """Relay data frames, absorbing the keepalive replies in between."""
        while True:
            kind, channel, data = await self._frame_or_response(reader)
            if kind == "frame":
                self._dispatch_frame(channel, data)
                continue
            # A keepalive answer. Consume any body so the stream stays aligned.
            text = data.decode(errors="replace")
            match = re.search(r"content-length:\s*(\d+)", text, re.I)
            if match and int(match.group(1)):
                await reader.readexactly(int(match.group(1)))

    # ---------------- teardown ----------------

    async def close(self) -> None:
        self._closing = True
        if self.pump_task is not None:
            self.pump_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.pump_task
            self.pump_task = None
        for writer in list(self.clients):
            with contextlib.suppress(Exception):
                writer.close()
        self.clients.clear()
        if self.server is not None:
            self.server.close()
            with contextlib.suppress(Exception):
                await self.server.wait_closed()
            self.server = None

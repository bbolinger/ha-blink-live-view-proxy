# Stub-server tests for BlinkRtspRelay. No Blink account or camera involved.
# The stub mimics Blink, including always answering "CSeq: 1".
#
# Run: copy blink_proxy/ next to this file, then
#   tar -cf - . | docker run --rm -i python:3.12-alpine sh -c \
#     "mkdir /w && cd /w && tar xf - && pip install -q aiohttp blinkpy==0.25.9 && python -u t.py"
#
# Covers:
#   1  reconnect after an early drop
#   2  data frames arriving mid-handshake are not lost or corrupted
#   3  NO reconnect once a session has settled
#   4  a consumer that stops reading is dropped at the backlog limit
import asyncio
import contextlib
import logging
import sys

logging.basicConfig(level=logging.INFO, format="  %(levelname)s %(message)s")

import blink_proxy.rtsp as R

URL = "rtsps://stub.invalid:443/session__IMDS_1?client_id=82&blinkRTSP=true"

SDP = ("v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=name\r\nc=IN IP4 127.0.0.1\r\n"
       "t=0 0\r\na=tool:immedia_isi108 0.0.1\r\na=control:*\r\n"
       "m=video 5002 RTP/AVP 33\r\na=rtpmap:33 MP2T/90000\r\n"
       "a=control:trackID=1\r\na=range:npt=now-\r\n")

OK = b"RTSP/1.0 200 OK\r\nCSeq: 1\r\n\r\n"          # Blink always says CSeq: 1


def frame(seq):
    """One interleaved RTP frame carrying a single 188-byte MPEG-TS packet."""
    ts = b"\x47" + bytes(187)
    rtp = bytes([0x80, 33]) + (seq % 65536).to_bytes(2, "big") + bytes(8) + ts
    return b"$" + bytes([0]) + len(rtp).to_bytes(2, "big") + rtp


def describe_reply():
    body = SDP.encode()
    return (b"RTSP/1.0 200 OK\r\nCSeq: 1\r\nContent-Type: application/sdp\r\n"
            + f"Content-Length: {len(body)}\r\n\r\n".encode() + body)


class Stub:
    """Minimal Blink-like RTSP server.

    frames_after_play  how many frames to emit once PLAY is answered
    drop_after_first   close the first connection abruptly after those frames
    hold_before_drop   keep the connection open this long before dropping
    frames_after_setup emit frames BEFORE play, which is what breaks a naive reader
    stream_seconds     keep emitting frames for this long instead of stopping
    send_session       include a Session header on SETUP
    """

    def __init__(self, frames_after_play=5, drop_after_first=False,
                 hold_before_drop=0.0, frames_after_setup=0,
                 stream_seconds=0.0, send_session=True):
        self.connections = 0
        self.frames_after_play = frames_after_play
        self.drop_after_first = drop_after_first
        self.hold_before_drop = hold_before_drop
        self.frames_after_setup = frames_after_setup
        self.stream_seconds = stream_seconds
        self.send_session = send_session
        self.server = None

    async def handle(self, reader, writer):
        self.connections += 1
        mine = self.connections
        try:
            while True:
                head = await reader.readuntil(b"\r\n\r\n")
                method = head.split(b" ", 1)[0].decode()

                if method == "DESCRIBE":
                    writer.write(describe_reply())
                elif method == "SETUP":
                    if self.send_session:
                        writer.write(b"RTSP/1.0 200 OK\r\nCSeq: 1\r\n"
                                     b"Session: immedia0\r\n"
                                     b"Transport: RTP/AVP/TCP;interleaved=0-1\r\n\r\n")
                    else:
                        writer.write(OK)          # no Session, no Transport
                    await writer.drain()
                    for i in range(self.frames_after_setup):
                        writer.write(frame(i))    # video before PLAY
                elif method == "PLAY":
                    writer.write(OK)
                    await writer.drain()
                    for i in range(self.frames_after_play):
                        writer.write(frame(i))
                    await writer.drain()
                    if self.stream_seconds:
                        deadline = asyncio.get_running_loop().time() + self.stream_seconds
                        i = 0
                        while asyncio.get_running_loop().time() < deadline:
                            writer.write(frame(i))
                            i += 1
                            if i % 20 == 0:
                                await writer.drain()
                                await asyncio.sleep(0.02)
                    if mine == 1 and self.drop_after_first:
                        if self.hold_before_drop:
                            await asyncio.sleep(self.hold_before_drop)
                        writer.close()            # abrupt mid-stream drop
                        return
                    await asyncio.sleep(3)
                    return
                else:
                    writer.write(OK)
                await writer.drain()
        except Exception:
            pass
        finally:
            with contextlib.suppress(Exception):
                writer.close()


async def start_stub(stub):
    stub.server = await asyncio.start_server(stub.handle, "127.0.0.1", 0)
    return int(stub.server.sockets[0].getsockname()[1])


def patch_connect(port):
    """Force the relay's outbound connection to the stub, in plain TCP."""
    real = asyncio.open_connection

    async def fake(host=None, p=None, ssl=None, **kw):
        return await real("127.0.0.1", port)

    asyncio.open_connection = fake
    return real


async def collect(real, relay, seconds):
    """Attach a consumer and return the bytes it receives."""
    got = bytearray()
    reader, _ = await real("127.0.0.1", relay._port)

    async def drain():
        with contextlib.suppress(Exception):
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                got.extend(chunk)

    task = asyncio.create_task(drain())
    await asyncio.sleep(seconds)
    task.cancel()
    return got


def ts_clean(buf):
    """True if every 188-byte packet starts with the MPEG-TS sync byte."""
    return (len(buf) > 0 and len(buf) % 188 == 0
            and all(buf[i] == 0x47 for i in range(0, len(buf), 188)))


async def test_reconnect():
    print("\n== TEST 1: drops mid-stream, should reconnect ==")
    stub = Stub(frames_after_play=5, drop_after_first=True)
    port = await start_stub(stub)
    real = patch_connect(port)
    try:
        relay = R.BlinkRtspRelay(URL, {})
        await relay.start()
        got = await collect(real, relay, 6)
        await relay.close()
        print(f"  upstream connections: {stub.connections}   bytes relayed: {len(got)}")
        ok = stub.connections >= 2
        print(f"  {'PASS' if ok else 'FAIL'}  reconnect after early drop")
        return ok
    finally:
        asyncio.open_connection = real
        stub.server.close()


async def test_frames_during_handshake():
    """Blink starts sending as soon as SETUP is answered, sometimes before PLAY.

    A reader that blindly scans for CRLFCRLF eats those frames and loses
    framing. This stub also omits Session and Transport, as PR #4's account does.
    """
    print("\n== TEST 2: data frames arrive mid-handshake ==")
    stub = Stub(frames_after_setup=8, frames_after_play=0,
                stream_seconds=2.5, send_session=False)
    port = await start_stub(stub)
    real = patch_connect(port)
    try:
        relay = R.BlinkRtspRelay(URL, {})
        await relay.start()
        played = relay.playing.is_set() and relay.session_id is None
        got = await collect(real, relay, 2.5)
        await relay.close()
        print(f"  handshake survived early data: {played}")
        print(f"  bytes relayed: {len(got)}  ({len(got)//188} TS packets)")
        print(f"  every packet starts 0x47:     {ts_clean(got)}")
        ok = played and ts_clean(got) and len(got) > 20 * 188
        print(f"  {'PASS' if ok else 'FAIL'}  no frames lost or corrupted mid-handshake")
        return ok
    finally:
        asyncio.open_connection = real
        stub.server.close()


async def test_no_reconnect_when_settled():
    print("\n== TEST 3: settled session ends, should NOT reconnect ==")
    R.SESSION_SETTLED_SECONDS = 0.5
    stub = Stub(frames_after_play=5, drop_after_first=True, hold_before_drop=2.0)
    port = await start_stub(stub)
    real = patch_connect(port)
    try:
        relay = R.BlinkRtspRelay(URL, {})
        await relay.start()
        await asyncio.sleep(4)
        await relay.close()
        print(f"  upstream connections: {stub.connections}")
        ok = stub.connections == 1
        print(f"  {'PASS' if ok else 'FAIL'}  no reconnect once settled")
        return ok
    finally:
        asyncio.open_connection = real
        R.SESSION_SETTLED_SECONDS = 30.0
        stub.server.close()


async def test_backpressure():
    print("\n== TEST 4: consumer never reads, should be dropped ==")
    R.MAX_CLIENT_BACKLOG_BYTES = 50_000
    stub = Stub(frames_after_play=40_000)
    port = await start_stub(stub)
    real = patch_connect(port)
    try:
        relay = R.BlinkRtspRelay(URL, {})
        await relay.start()
        _reader, _writer = await real("127.0.0.1", relay._port)   # never reads
        await asyncio.sleep(5)
        remaining = len(relay.clients)
        await relay.close()
        print(f"  clients still attached: {remaining}")
        ok = remaining == 0
        print(f"  {'PASS' if ok else 'FAIL'}  slow consumer dropped")
        return ok
    finally:
        asyncio.open_connection = real
        R.MAX_CLIENT_BACKLOG_BYTES = 4 * 1024 * 1024
        stub.server.close()


async def main():
    results = [
        await test_reconnect(),
        await test_frames_during_handshake(),
        await test_no_reconnect_when_settled(),
        await test_backpressure(),
    ]
    print("\n%d/%d passed" % (sum(1 for r in results if r), len(results)))
    return 0 if all(results) else 1


sys.exit(asyncio.run(main()))

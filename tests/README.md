# RTSP relay stub tests

Reference material for discussion on PR #4, not a proposal to merge.

`addon/proxy/blink_proxy/rtsp.py` here is my own RTSP relay, written before #4
existed. It is kept only so these tests have something to run against. #4's
implementation is the better one, in particular its single-unit reader, which I
have since adopted.

What is worth stealing is the tests. They run against a stub RTSP server that
mimics Blink, including always answering `CSeq: 1`, so no Blink account, no
camera and no network are needed.

    cp -r addon/proxy/blink_proxy tests/blink_proxy
    cd tests && python -u test_rtsp_relay.py

Covers:

1. reconnect after an early drop
2. data frames arriving mid-handshake are not lost or corrupted
3. NO reconnect once a session has settled
4. a consumer that stops reading is dropped at the backlog limit

Test 2 is the one that matters most: it is the failure mode described in #4,
where a reader scanning for CRLFCRLF swallows video during the handshake.

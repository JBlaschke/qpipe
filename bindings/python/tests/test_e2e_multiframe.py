#!/usr/bin/env python

# SPDX-License-Identifier: AGPL-3.0-or-later

"""Multi-frame e2e tests: messages larger than one frame, end to end.

Producer.send() transparently splits payloads > MAX_FRAME_SIZE into chunk
frames; the orchestrator routes every chunk of a message to whichever
consumer claimed its first chunk; Consumer.recv() reassembles and returns
complete messages in COMPLETION order. None of that machinery is visible in
the Python API — these tests confirm it stays invisible.

Uses the same `orchestrator` / `fresh_orchestrator` conftest fixtures as
test_e2e.py. Expect a few hundred MiB of loopback traffic across the module;
sizes are chosen to hit chunking boundaries, not to benchmark.

Requires Python >= 3.9 (random.Random.randbytes).
"""

import hashlib
import random
import socket
import struct
import threading

import pytest
import qpipe

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(60)]

# Framing constants come straight from the bindings — nothing hardcoded.
MAX_FRAME_SIZE    = qpipe.MAX_FRAME_SIZE
CHUNK_HEADER_LEN  = qpipe.CHUNK_HEADER_LEN
MAX_CHUNK_PAYLOAD = qpipe.MAX_CHUNK_PAYLOAD
FRAME_FLAG_CHUNK  = qpipe.FRAME_FLAG_CHUNK


def payload_for(tag: str, size: int) -> bytes:
    """Deterministic, unique-per-tag pseudorandom bytes. (Seeding Random with
    a str is PYTHONHASHSEED-independent.) Random content means chunk
    reordering or substitution can't cancel out the way a repeating pattern
    could."""
    return random.Random(f"{tag}:{size}").randbytes(size)


def digest(data: bytes) -> tuple[int, str]:
    """(length, sha256). Tests compare digests so a failing assert doesn't
    ask pytest to render a 40 MiB diff."""
    return (len(data), hashlib.sha256(data).hexdigest())


# ---- size boundary matrix ----------------------------------------------

SIZES = [
    # Single-frame ceiling: must still take the original one-frame path.
    pytest.param(MAX_FRAME_SIZE, id="exactly-one-frame"),
    # Smallest possible multi-frame message (2 chunks: full + 25 bytes).
    pytest.param(MAX_FRAME_SIZE + 1, id="smallest-two-chunk"),
    # Last chunk exactly full vs. a 1-byte tail chunk.
    pytest.param(2 * MAX_CHUNK_PAYLOAD, id="two-full-chunks"),
    pytest.param(2 * MAX_CHUNK_PAYLOAD + 1, id="two-chunks-plus-tail"),
    # A comfortable 3-chunk message.
    pytest.param(40 * 1024 * 1024, id="three-chunks-40MiB"),
]


@pytest.mark.parametrize("size", SIZES)
def test_large_payload_roundtrip(orchestrator, size):
    payload = payload_for(f"roundtrip-{size}", size)
    want = digest(payload)

    with qpipe.Producer.connect(orchestrator, codec="raw") as p:
        p.send(payload)
    del payload  # keep peak memory to one copy

    with qpipe.Consumer.connect(orchestrator, codec="raw") as c:
        got = c.recv()
    assert digest(got) == want


def test_multiframe_is_codec_transparent(orchestrator):
    # The codec layer only touches payload bytes, never framing: a JSON
    # object whose encoding exceeds MAX_FRAME_SIZE chunks and reassembles
    # like any other byte stream.
    blob = "x" * MAX_FRAME_SIZE  # + JSON overhead => guaranteed 2 chunks
    obj = {"kind": "bulk", "blob": blob}

    with qpipe.Producer.connect(orchestrator, codec="json") as p:
        p.send(obj)
    with qpipe.Consumer.connect(orchestrator, codec="json") as c:
        got = c.recv()

    assert got["kind"] == "bulk"
    assert len(got["blob"]) == len(blob)
    assert got["blob"] == blob


# ---- completion order: a stuck multi-frame message must not block recv --

def _recv_exactly(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        part = sock.recv(n - len(buf))
        if not part:
            raise ConnectionError("socket closed mid-handshake")
        buf += part
    return bytes(buf)


def _inject_orphan_chunk(
    addr: str, msg_id: int, idx: int, count: int, payload: bytes
) -> None:
    """Speak the wire protocol directly to plant one chunk of a message that
    will never complete (we claim `count` chunks, send one, and hang up —
    a clean EOF at a frame boundary, exactly like a producer crashing
    between chunks).

    Handshake (mirrors Producer::connect): control port -> role byte b'P'
    -> reply (u16 BE ephemeral port, 16-byte token) -> connect ephemeral
    port -> token -> frame -> one ACK byte b'A'.
    Chunk frame: [u32 BE FLAG|body_len][u128 BE id][u32 BE idx]
    [u32 BE count][payload], body_len = 24 + len(payload).
    """
    host, port_s = addr.rsplit(":", 1)
    with socket.create_connection((host, int(port_s)), timeout=5) as ctrl:
        ctrl.sendall(b"P")
        port = struct.unpack(">H", _recv_exactly(ctrl, 2))[0]
        token = _recv_exactly(ctrl, 16)

    with socket.create_connection((host, port), timeout=5) as data:
        data.sendall(token)
        body_len = CHUNK_HEADER_LEN + len(payload)
        data.sendall(struct.pack(">I", FRAME_FLAG_CHUNK | body_len))
        data.sendall(msg_id.to_bytes(16, "big"))
        data.sendall(struct.pack(">II", idx, count))
        data.sendall(payload)
        assert _recv_exactly(data, 1) == b"A"


@pytest.mark.timeout(30)
def test_orphaned_partial_does_not_block_recv(fresh_orchestrator):
    """The Python-visible form of the non-lockup guarantee: recv() buffers
    chunks of incomplete messages internally and keeps reading, returning
    whichever message completes — so a forever-incomplete message is
    invisible apart from the memory it holds (reclaimable via a partials GC).

    With a single consumer the frame order is fully deterministic (FIFO):
    orphan chunk, then the big message's chunks, then the small single.
    recv() absorbs the orphan, completes and returns the big message, then
    the small one."""
    addr = fresh_orchestrator

    with qpipe.Consumer.connect(addr, codec="raw") as c:
        _inject_orphan_chunk(
            addr, msg_id=0xDEAD_BEEF, idx=0, count=2, payload=b"never done"
        )

        big = payload_for("post-orphan-big", MAX_FRAME_SIZE + 1)
        want_big = digest(big)
        with qpipe.Producer.connect(addr, codec="raw") as p:
            p.send(big)
            p.send(b"small after big")
        del big

        assert digest(c.recv()) == want_big
        assert c.recv() == b"small after big"

        # The orphan partial is visible — one message, holding exactly the
        # injected chunk's bytes — and reclaimable.
        assert c.pending_partials() == (1, len(b"never done"))
        assert c.gc_partials(3600.0) == 0  # fresh enough to keep
        assert c.gc_partials(0.0) == 1     # any idle time at all: dropped
        assert c.pending_partials() == (0, 0)


# ---- mixed-size MPMC: exactly-once across claim/redirect routing --------

@pytest.mark.timeout(120)  # heavier than the others; overrides module default
def test_mixed_size_mpmc_exactly_once(fresh_orchestrator):
    """N producers interleave small singles with multi-frame messages; M
    consumers drain concurrently. Every message is received exactly once
    across all consumers, with content intact — exercising the
    claim-at-first-chunk + redirect path under real contention (chunks of
    different messages and unrelated singles interleave in the shared
    queue, so consumers routinely pop chunks owned by someone else).

    Termination via poison pills, as in test_e2e.py. Pill validity with
    multi-frame traffic rests on two facts:
      1. Pills are enqueued after every real frame (send() returns only
         after its ACK and producers are joined first), and the shared
         queue is FIFO — so no pill is popped while real frames queue
         ahead of it.
      2. A chunk popped by the "wrong" consumer is redirected into its
         owner's directed queue, which pop services BEFORE the shared
         queue — so a consumer mid-message drains everything it owes
         before it can ever see a pill.
    Each of the M pills therefore stops exactly one consumer. As in the
    existing MPMC test, no assertion is made about HOW work spreads across
    consumers — a work queue doesn't promise fairness."""
    addr = fresh_orchestrator
    N_PRODUCERS = 2
    M_CONSUMERS = 3          # deliberately != N_PRODUCERS
    SMALL_EACH = 25
    BIG_EACH = 2
    BIG_SIZE = MAX_FRAME_SIZE + 1  # smallest 2-chunk message
    TOTAL = N_PRODUCERS * (SMALL_EACH + BIG_EACH)

    PILL = b"\x00__qpipe_pill__\x00"

    def small_payload(p: int, s: int) -> bytes:
        return f"p{p}-small-{s}".encode()

    def big_payload(p: int, s: int) -> bytes:
        return payload_for(f"p{p}-big-{s}", BIG_SIZE)

    # Expected multiset of digests. Payloads are unique by construction, so
    # multiset equality also proves no duplication. Generate big payloads one
    # at a time to keep peak memory at a single copy.
    expected = []
    for p in range(N_PRODUCERS):
        for s in range(SMALL_EACH):
            expected.append(digest(small_payload(p, s)))
        for s in range(BIG_EACH):
            expected.append(digest(big_payload(p, s)))

    received_slots = [None] * M_CONSUMERS
    consumer_errors = [None] * M_CONSUMERS
    producer_errors = [None] * N_PRODUCERS

    def consumer_worker(idx):
        local = []
        try:
            with qpipe.Consumer.connect(addr, codec="raw") as cons:
                while True:
                    frame = cons.recv()
                    if frame == PILL:
                        break  # stop on the FIRST pill; do not re-recv
                    local.append(digest(frame))  # digest now, drop the bytes
        except Exception as e:  # captured; re-raised in the main thread
            consumer_errors[idx] = e
        finally:
            received_slots[idx] = local

    def producer_worker(idx):
        try:
            with qpipe.Producer.connect(addr, codec="raw") as prod:
                # Interleave the big messages among the smalls so chunk
                # frames and singles genuinely mix in the shared queue.
                for s in range(SMALL_EACH):
                    prod.send(small_payload(idx, s))
                    if s < BIG_EACH:
                        prod.send(big_payload(idx, s))
        except Exception as e:
            producer_errors[idx] = e

    consumers = [
        threading.Thread(target=consumer_worker, args=(i,), daemon=True)
        for i in range(M_CONSUMERS)
    ]
    for t in consumers:
        t.start()

    producers = [
        threading.Thread(target=producer_worker, args=(i,), daemon=True)
        for i in range(N_PRODUCERS)
    ]
    for t in producers:
        t.start()
    for t in producers:
        t.join(timeout=60)

    for i, e in enumerate(producer_errors):
        if e is not None:
            raise AssertionError(f"producer {i} failed: {e!r}")
    assert all(not t.is_alive() for t in producers), "a producer thread hung"

    with qpipe.Producer.connect(addr, codec="raw") as pill_prod:
        for _ in range(M_CONSUMERS):
            pill_prod.send(PILL)

    for t in consumers:
        t.join(timeout=60)
    assert all(not t.is_alive() for t in consumers), \
        "a consumer thread hung (pill not received?)"
    for i, e in enumerate(consumer_errors):
        if e is not None:
            raise AssertionError(f"consumer {i} failed: {e!r}")

    received = [d for slot in received_slots for d in (slot or [])]

    assert len(received) == TOTAL, \
        f"expected {TOTAL} messages, got {len(received)}"
    assert len(set(received)) == len(received), \
        "a message was delivered more than once"
    assert sorted(received) == sorted(expected), \
        "received multiset != sent multiset"

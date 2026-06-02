#!/usr/bin/env python

# SPDX-License-Identifier: AGPL-3.0-or-later

"""real frames through the real orchestrator"""

import threading
import pytest
import qpipe

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(15)]


def test_raw_bytes_roundtrip(orchestrator):
    payload = b"\x00\x01 hello \xff\xfe"
    with qpipe.Producer.connect(orchestrator, codec="raw") as p:
        p.send(payload)
    with qpipe.Consumer.connect(orchestrator, codec="raw") as c:
        assert c.recv() == payload


def test_json_roundtrip(orchestrator):
    job = {"bucket": "my-bucket", "name": "obj-001", "tier": "Archive"}
    with qpipe.Producer.connect(orchestrator, codec="json") as p:
        p.send(job)
    with qpipe.Consumer.connect(orchestrator, codec="json") as c:
        assert c.recv() == job


def test_msgpack_roundtrip(orchestrator):
    pytest.importorskip("msgpack")
    job = {"id": 7, "names": ["a", "b"], "blob": b"\x00\xff"}
    with qpipe.Producer.connect(orchestrator, codec="msgpack") as p:
        p.send(job)
    with qpipe.Consumer.connect(orchestrator, codec="msgpack") as c:
        assert c.recv() == job


def test_multiple_frames_fifo(orchestrator):
    sent = [{"i": i} for i in range(10)]
    with qpipe.Producer.connect(orchestrator, codec="json") as p:
        for obj in sent:
            p.send(obj)
    got = []
    with qpipe.Consumer.connect(orchestrator, codec="json") as c:
        for _ in range(len(sent)):
            got.append(c.recv())
    # Single producer + single consumer => FIFO order preserved.
    assert got == sent


def test_cross_codec_wire_compat(orchestrator):
    # A msgpack producer's frame is readable by recv_bytes + manual unpack,
    # proving the codec only touches payload bytes, not framing.
    msgpack = pytest.importorskip("msgpack")
    with qpipe.Producer.connect(orchestrator, codec="msgpack") as p:
        p.send({"k": "v"})
    with qpipe.Consumer.connect(orchestrator, codec="raw") as c:
        raw = c.recv_bytes()
    assert msgpack.unpackb(raw, raw=False) == {"k": "v"}


@pytest.mark.timeout(60)  # heavier than the others; overrides the module default
def test_mpmc_no_loss_no_duplication(fresh_orchestrator):
    # NOTE: when split into a real tests/test_e2e.py, this docstring uses plain
    # triple quotes. (In this bundled artifact the surrounding block is itself a
    # triple-quoted string, so the quotes are written with a leading marker only
    # for display — use ordinary triple quotes in the actual file.)
    """N producers, M consumers, one orchestrator. Every frame sent is received
    exactly once across all consumers (work-queue semantics: each frame goes to
    exactly one consumer). Order across consumers is NOT guaranteed; the multiset
    of all received frames is.

    Termination via poison pills. After every producer finishes, we enqueue
    exactly M sentinel frames; each consumer stops on the first pill it sees.

    Correctness rests on two facts:
      1. All real frames are enqueued before any pill is sent. send() returns
         only after its ACK, and the producer threads are joined before the
         pills go out, so every real frame is in the queue first.
      2. The queue is FIFO, so the pills (enqueued last) are dequeued last. No
         consumer can pull a pill while real frames remain. This is the same
         FIFO assumption the single-consumer round-trip test already relies on.
    With M pills and M consumers each consuming exactly one, every consumer is
    guaranteed to terminate and no pill is left in the queue.
    """
    addr = fresh_orchestrator
    N_PRODUCERS = 3
    M_CONSUMERS = 4          # deliberately != N_PRODUCERS
    FRAMES_EACH = 100
    total = N_PRODUCERS * FRAMES_EACH

    sent = [(p, s) for p in range(N_PRODUCERS) for s in range(FRAMES_EACH)]

    # Threads can't return values; give each a result slot and an error slot.
    received_slots = [None] * M_CONSUMERS
    consumer_errors = [None] * M_CONSUMERS
    producer_errors = [None] * N_PRODUCERS

    def consumer_worker(idx):
        local = []
        try:
            with qpipe.Consumer.connect(addr, codec="json") as cons:
                while True:
                    frame = cons.recv()
                    if isinstance(frame, dict) and frame.get("__pill__"):
                        break          # stop on the FIRST pill; do not re-recv
                    local.append((frame["p"], frame["s"]))
        except Exception as e:         # captured; re-raised in the main thread
            consumer_errors[idx] = e
        finally:
            received_slots[idx] = local

    def producer_worker(idx):
        try:
            with qpipe.Producer.connect(addr, codec="json") as prod:
                for s in range(FRAMES_EACH):
                    prod.send({"p": idx, "s": s})
        except Exception as e:
            producer_errors[idx] = e

    # Start consumers first so they're ready, then producers. Both sets run
    # concurrently — the GIL is released inside send()/recv() (py.detach in the
    # bindings), so these threads genuinely overlap on the blocking socket I/O.
    # daemon=True so a hung worker can't wedge process exit on failure.
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
        t.join(timeout=30)

    # Surface producer failures before sending pills.
    for i, e in enumerate(producer_errors):
        if e is not None:
            raise AssertionError(f"producer {i} failed: {e!r}")
    assert all(not t.is_alive() for t in producers), "a producer thread hung"

    # Every real frame is now enqueued. Enqueue one pill per consumer to stop
    # them. Even with a small queue CAPACITY this can't deadlock: pills drain as
    # consumers consume them, and the M-pills/M-consumers bijection holds however
    # the enqueue/dequeue interleaves.
    with qpipe.Producer.connect(addr, codec="json") as pill_prod:
        for _ in range(M_CONSUMERS):
            pill_prod.send({"__pill__": True})

    for t in consumers:
        t.join(timeout=30)
    assert all(not t.is_alive() for t in consumers), \
        "a consumer thread hung (pill not received?)"
    for i, e in enumerate(consumer_errors):
        if e is not None:
            raise AssertionError(f"consumer {i} failed: {e!r}")

    received = [item for slot in received_slots for item in (slot or [])]

    # The three properties: no loss, no duplication, nothing fabricated.
    assert len(received) == total, f"expected {total} frames, got {len(received)}"
    assert len(set(received)) == len(received), "a frame was delivered more than once"
    assert sorted(received) == sorted(sent), "received multiset != sent multiset"

    # NOTE: intentionally no assertion on HOW frames were distributed across
    # consumers. A work queue does not promise fairness — one consumer grabbing
    # everything is a legal (if unlucky) outcome, so asserting a spread would
    # test a property qpipe doesn't guarantee and would flake. Per-consumer
    # counts are available in received_slots for debugging if a run looks off.

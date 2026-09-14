#!/usr/bin/env python

# SPDX-License-Identifier: AGPL-3.0-or-later

"""
qpipe.work harness on in-process fake pipes (fast tier: no binary, no sockets).

The fakes keep the one transport property the harness has to live with: an
orchestrator's queue is bounded, and a producer's send() BLOCKS — the ACK is
withheld — while the queue is full (Router::push in orchestrator.rs). Drain
and shutdown EOF consumers the way the real pipes do.

Regression: coordinator deadlock on a wide beget. _run_coordinator used to call
the blocking work.send() from inside the loop that reads the completions pipe.
A beget with more children than the work pipe could absorb parked the
coordinator in send(); the workers kept draining work and posting done/beget
frames until the completions pipe was full too, then parked in control.send();
each side waited for the other to consume. First seen walking a filesystem
whose BFS level 5 held 21 000 directories against the 10 000-frame default —
with 1 and with 8 worker threads alike.
"""

import threading
from collections import deque

import pytest
import qpipe
from qpipe.work import Pipes

from conftest import HARNESS_CAPACITY as CAPACITY, HARNESS_FANOUT as FANOUT


class FakePipe:
    """A bounded FIFO with orchestrator semantics: put() blocks while full,
    get() blocks while empty, drain() EOFs consumers once empty, shutdown()
    fails everyone immediately."""

    def __init__(self, capacity):
        self.capacity = capacity
        self.frames = deque()
        self.cv = threading.Condition()
        self.draining = False
        self.closed = False

    def put(self, frame):
        with self.cv:
            while len(self.frames) >= self.capacity and not self.closed:
                self.cv.wait()
            if self.closed:
                raise qpipe.QpipeError("send failed: pipe shut down")
            self.frames.append(frame)
            self.cv.notify_all()

    def get(self):
        with self.cv:
            while not self.frames and not (self.draining or self.closed):
                self.cv.wait()
            if not self.frames:
                raise qpipe.QpipeError("orchestrator closed consumer connection")
            frame = self.frames.popleft()
            self.cv.notify_all()
            return frame

    def drain(self):
        with self.cv:
            self.draining = True
            self.cv.notify_all()

    def shutdown(self):
        with self.cv:
            self.closed = True
            self.cv.notify_all()


@pytest.fixture
def fake_pipes(monkeypatch):
    """
    make(capacity) -> addr. Wires FakePipe into the module-level API the
    harness uses (qpipe.Producer/Consumer/wait_until_healthy/request_drain/
    request_shutdown), so qpipe.work runs unmodified. Teardown shuts every
    pipe down, which unparks whatever a failed test left blocked.
    """
    registry: dict[str, FakePipe] = {}

    class Producer:
        def __init__(self, pipe, codec):
            self._pipe, self._codec = pipe, codec

        @classmethod
        def connect(cls, addr, codec="raw"):
            return cls(registry[addr], qpipe._resolve_codec(codec))

        def send(self, obj):
            self._pipe.put(self._codec.encode(obj))

        def close(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class Consumer(Producer):
        def recv(self):
            return self._codec.decode(self._pipe.get())

        def __iter__(self):
            return self

        def __next__(self):
            try:
                return self.recv()
            except qpipe.QpipeError:
                raise StopIteration from None

    monkeypatch.setattr(qpipe, "Producer", Producer)
    monkeypatch.setattr(qpipe, "Consumer", Consumer)
    monkeypatch.setattr(qpipe, "wait_until_healthy",
                        lambda addr, timeout=None: registry[addr])
    monkeypatch.setattr(qpipe, "request_drain",
                        lambda addr: registry[addr].drain())
    monkeypatch.setattr(qpipe, "request_shutdown",
                        lambda addr: registry[addr].shutdown())

    def make(capacity):
        addr = f"fake:{len(registry)}"
        registry[addr] = FakePipe(capacity)
        return addr

    try:
        yield make
    finally:
        for pipe in registry.values():
            pipe.shutdown()


def test_fake_pipe_blocks_when_full():
    """The fake must model the real backpressure, or the test below is moot."""
    pipe = FakePipe(capacity=2)
    pipe.put(b"a")
    pipe.put(b"b")
    t = threading.Thread(target=pipe.put, args=(b"c",), daemon=True)
    t.start()
    t.join(0.2)
    assert t.is_alive(), "put() must block on a full pipe"
    assert pipe.get() == b"a"
    t.join(2.0)
    assert not t.is_alive(), "put() must resume once a frame is consumed"


@pytest.mark.parametrize("threads", [1, 4])
def test_beget_wider_than_pipe_capacity_terminates(fake_pipes, fanout_pipeline,
                                                   run_pipeline, threads):
    """
    One seed begets FANOUT leaves through pipes bounded at CAPACITY frames,
    FANOUT > 2 * CAPACITY. The coordinator can only ever get about
    CAPACITY(work) + CAPACITY(completions) frames out while it is not reading
    completions, so a coordinator that blocks in work.send() inside its
    completions loop wedges here deterministically; a correct one finishes
    every task.
    """
    pipes = Pipes(work=fake_pipes(CAPACITY), completions=fake_pipes(CAPACITY),
                  results=fake_pipes(CAPACITY), wait=1.0)
    coordinator, worker, processed = fanout_pipeline(FANOUT)

    rc = run_pipeline(pipes, coordinator, worker, threads, deadline=10.0)

    assert rc == 0
    assert len(processed) == FANOUT + 1

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

import re
import threading
from collections import deque

import pytest
import qpipe
from qpipe.work import Coordinator, Pipes, Worker

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

    def make(capacity, cls=FakePipe):
        addr = f"fake:{len(registry)}"
        registry[addr] = cls(capacity)
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


@pytest.mark.parametrize("depth_first", [True, False], ids=["depth", "breadth"])
@pytest.mark.parametrize("threads", [1, 4])
def test_beget_wider_than_pipe_capacity_terminates(fake_pipes, fanout_pipeline,
                                                   run_pipeline, threads,
                                                   depth_first):
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

    rc = run_pipeline(pipes, coordinator, worker, threads, deadline=10.0,
                      depth_first=depth_first)

    assert rc == 0
    assert len(processed) == FANOUT + 1


# ---------------------------------------------------------------------------
# dispatch order: the outbox is the frontier the coordinator holds in memory

def binary_tree_pipeline(depth, dedup="global", begets=1):
    """
    Strategy: one root (id 1); every node with id < 2**depth begets 2n and
    2n+1, so the tree is complete with 2**(depth+1) - 1 directories and its
    widest level holds 2**depth of them. Every processed id is recorded.
    `begets` > 1 reports the same children that many times — what two live
    attempts of one task do.
    """
    processed = []

    def seeds():
        yield {"id": 1}

    def expand(parent, disc):
        return ({"id": c} for c in disc["children"])

    def process(_state, job, result, discover):
        n = job.spec["id"]
        if n < 2 ** depth:
            for _ in range(begets):
                discover({"children": [2 * n, 2 * n + 1]})
        processed.append(n)

    return (Coordinator(seeds=seeds, expand=expand, key_of=lambda s: s["id"],
                        dedup=dedup),
            Worker(setup=lambda: None, process=process),
            processed)


@pytest.mark.parametrize("depth_first, lo, hi",
                         [(True, 0, 150), (False, 300, 10 ** 9)],
                         ids=["depth", "breadth"])
def test_dispatch_order_sets_the_outbox_peak(fake_pipes, run_pipeline, capsys,
                                             depth_first, lo, hi):
    """
    A complete binary tree of depth 10 (2047 directories) through an
    8-frame work pipe with one worker. Breadth-first parks most of the
    widest level in the coordinator's outbox — one parked beget per parent
    at the level above, ~600 entries; depth-first keeps it to a few dozen.
    Both walk every directory exactly once.
    """
    pipes = Pipes(work=fake_pipes(8), completions=fake_pipes(CAPACITY),
                  results=fake_pipes(CAPACITY), wait=1.0)
    coordinator, worker, processed = binary_tree_pipeline(10)

    rc = run_pipeline(pipes, coordinator, worker, 1, deadline=30.0,
                      depth_first=depth_first)

    assert rc == 0
    assert sorted(processed) == list(range(1, 2 ** 11))
    m = re.search(r"outbox peak (\d+)", capsys.readouterr().err)
    assert m, "the summary line reports the outbox peak"
    assert lo < int(m.group(1)) < hi, m.group(0)


@pytest.mark.parametrize("dedup", ["global", "parent"])
def test_repeated_begets_are_deduped_at_either_scope(fake_pipes, run_pipeline,
                                                     capsys, dedup):
    """
    Every directory reports its children twice. Both scopes must walk the
    127-directory tree exactly once; "parent" does so without keeping a
    single key past its parent's retirement.
    """
    pipes = Pipes(work=fake_pipes(CAPACITY), completions=fake_pipes(CAPACITY),
                  results=fake_pipes(CAPACITY), wait=1.0)
    coordinator, worker, processed = binary_tree_pipeline(6, dedup=dedup,
                                                          begets=2)

    rc = run_pipeline(pipes, coordinator, worker, 2, deadline=20.0)

    assert rc == 0
    assert sorted(processed) == list(range(1, 2 ** 7))
    assert "127 tasks, 127 completed, 0 failed" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# a dead work pipe must still let the run end

class DyingPipe(FakePipe):
    """put() succeeds `budget` times, then fails for good — an orchestrator
    that went away mid-run. get()/drain() keep working."""

    def __init__(self, capacity, budget=1):
        super().__init__(capacity)
        self.budget = budget

    def put(self, frame):
        if self.budget <= 0:
            raise qpipe.QpipeError("send failed: connection reset")
        self.budget -= 1
        super().put(frame)


def test_dead_work_pipe_fails_undispatched_tasks(fake_pipes, fanout_pipeline,
                                                 run_pipeline, capsys):
    """
    The work pipe dies after delivering the seed. Its 10 children can never
    be dispatched, and an undispatched task has no deadline, so the sender
    must fail the one child it had drawn terminally and drop the parked
    beget for outstanding to reach zero: the run ends partial (rc 1)
    instead of hanging.
    """
    pipes = Pipes(work=fake_pipes(CAPACITY, cls=DyingPipe),
                  completions=fake_pipes(CAPACITY),
                  results=fake_pipes(CAPACITY), wait=1.0)
    coordinator, worker, processed = fanout_pipeline(10)

    rc = run_pipeline(pipes, coordinator, worker, 1, deadline=10.0)

    assert rc == 1
    assert processed == [1]                     # the seed, nothing else
    err = capsys.readouterr().err
    assert "work pipe send failed" in err
    assert "2 tasks, 1 completed, 1 failed" in err     # seed + the drawn child
    assert "1 begets dropped unexpanded" in err


# ---------------------------------------------------------------------------
# parked begets: a wide discovery costs the outbox one entry, not one per child

def test_parked_begets_keep_the_outbox_flat(fake_pipes, fanout_pipeline,
                                            run_pipeline, capsys):
    """
    One beget of 5000 children through an 8-frame work pipe, one worker.
    The outbox holds the parked beget rather than 5000 frames, so its peak
    stays in single digits while every child is still walked exactly once.
    """
    pipes = Pipes(work=fake_pipes(8), completions=fake_pipes(CAPACITY),
                  results=fake_pipes(CAPACITY), wait=1.0)
    coordinator, worker, processed = fanout_pipeline(5000)

    rc = run_pipeline(pipes, coordinator, worker, 1, deadline=60.0)

    assert rc == 0
    assert len(processed) == 5001
    m = re.search(r"outbox peak (\d+)", capsys.readouterr().err)
    assert m and int(m.group(1)) < 10, m.group(0)


def test_expand_failure_drops_the_rest_of_that_beget(fake_pipes, run_pipeline,
                                                     capsys):
    """
    expand() raising mid-beget must not hang the run: the children drawn so
    far are walked, the rest of that beget is dropped, and the run ends
    rc 1 saying so.
    """
    processed = []

    def seeds():
        yield {"id": 0}

    def expand(parent, disc):
        for c in disc["children"]:
            if c == 3:
                raise ValueError("bad child")
            yield {"id": c}

    def process(_state, job, result, discover):
        if job.spec["id"] == 0:
            discover({"children": [1, 2, 3, 4, 5]})
        processed.append(job.spec["id"])

    pipes = Pipes(work=fake_pipes(CAPACITY), completions=fake_pipes(CAPACITY),
                  results=fake_pipes(CAPACITY), wait=1.0)
    rc = run_pipeline(pipes, Coordinator(seeds=seeds, expand=expand),
                      Worker(setup=lambda: None, process=process), 1,
                      deadline=10.0)

    assert rc == 1
    assert sorted(processed) == [0, 1, 2]
    err = capsys.readouterr().err
    assert "expand() failed for task 1" in err
    assert "1 begets could not be expanded" in err

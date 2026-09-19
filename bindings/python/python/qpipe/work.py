#!/usr/bin/env python
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
qpipe.work — a work-distribution harness over qpipe.

qpipe gives you pipes; this gives you work distributed over pipes. A pipeline
author supplies two strategy objects and gets the whole harness — coordinator,
worker pool, collector, and orchestrator supervisor — for free:

    inputs ─▶ coordinator ──work──▶ worker pool ──completions──▶ coordinator
                                        │
                                        └──results──▶ collect / downstream

The two patterns this replaces are one pattern
  - work-generating (e.g. a recursive bucket listing): a seed begets more work
    as workers discover it.
  - pipelining (e.g. a copy or status-check stream): a list of inputs feeds
    straight through, nothing begets.
  They differ in exactly one thing — whether worker output feeds back as new
  work — so they differ in exactly one hook: Coordinator.expand. Its default
  returns nothing (pipelining); override it and you have work-generation. The
  coordinator loop, the ledger, and the termination algebra are identical
  either way and never branch on "which pattern".

The contract (two structs of functions, no inheritance, state explicit)
  Coordinator
    seeds()                 -> Iterator[Spec]      the inputs
    expand(parent, disc)    -> Iterable[Spec]      discovery -> new specs
                                                   (default: none; drawn
                                                   lazily, one child per
                                                   dispatch)
    key_of(spec)            -> Hashable | None     dedup identity
                                                   (default: monotonic ids)
    dedup                   "global" | "parent"    how long a key is kept
                                                   (default: global — the run)
  Worker
    setup()                 -> S                   per-thread state (clients…)
    process(S, job, result, discover) -> None      do one task
      result(rec)   sends a record to the results pipe (data plane — yours)
      discover(d)   reports discovered work to the coordinator (a beget)
      return        success;  raise Permanent(...)  fails without retry;
                    raise anything else            retries up to --max-attempts
  Spec and the discovery dict are opaque to the harness — meaningful only to
  your strategies. The harness owns the control plane; the results pipe is
  entirely yours (put a nushell-flat schema there).

Wire protocol (harness-owned; do not emit these shapes yourself)
  work    {"task":int, "attempt":int, "spec":<your spec dict>}
  beget   {"k":"beget", "task":int, "spec":<your discovery dict>}
  done    {"k":"done",  "task":int, "attempt":int, "worker":str,
           "duration":float}
  error   {"k":"error", "task":int, "worker":str, "why":str,
           "permanent":bool}

What the harness guarantees so your worker can't get it wrong
  - result-before-done: the wrapper sends `done` only after process() returns,
    and every result()/discover() call ACKs before it does — so when the
    coordinator sees a task's `done`, that task's results are already enqueued.
    This is what makes the post-zero drain of the results pipe race-free.
  - beget-before-done: begets and the done ride the same producer on the
    completions pipe (FIFO), so a task's child registrations are always applied
    before its retirement — which is what keeps termination sound.
  - at-least-once with timeout/permanent retry, the drain cascade, the watchdog
    hammer, SIGTERM teardown, Ctrl-C → 130.

Termination (one counter algebra, with the input source as the root task)
  outstanding starts at 1 — the source token. Each registered spec +1, each
  completion or terminal failure -1, source exhaustion seals the token (-1).
  A beget holds a token of its own (+1) from arrival until its last child has
  been registered (-1), so children still parked unexpanded can never let
  the count reach zero. outstanding == 0 therefore means the source is
  sealed AND every task is terminal — for both patterns, and nothing can hit
  zero before seal, so coordinator thread start order is a don't-care.

Flow control (--in-flight)
  Seeds are gated: the feeder blocks once --in-flight tasks are pending, so a
  supply-paced stream (stdin) can't out-dispatch the workers. Begets are NOT
  gated — they are self-paced by completions. A task's deadline is armed when
  the sender actually writes its frame, never at registration, so time spent
  queued behind a full work pipe cannot masquerade as a timeout and re-queue
  the task on top of itself.

  The one thing the coordinator must never do is block on the work pipe while
  it owns the completions pipe: the orchestrator's queue is bounded (10 000
  frames) and a full queue withholds the ACK, so a coordinator stuck in
  work.send() stops draining completions, the workers fill THAT queue with
  begets/dones and stall in control.send(), and nobody can move — a deadlock
  the moment one BFS level of the discovery tree is wider than the queue. Sends
  therefore go through an unbounded in-process outbox drained by a dedicated
  sender thread; the completions loop only ever blocks in recv().

Dispatch order and memory (--dispatch)
  Everything discovered but not yet dispatched waits in the coordinator's
  outbox (the overflow beyond the work pipe's capacity), and everything
  pending has a record in the ledger: together they are the frontier of the
  walk, and the coordinator's memory follows it. Drained FIFO (--dispatch
  breadth) the walk is breadth-first and the frontier is the widest level of
  the tree — millions of directories on a large filesystem. Drained LIFO
  (--dispatch depth, the default) it is depth-first and the frontier stays
  near the work pipe's capacity plus workers × depth. Terminal tasks leave
  the ledger, so per completed task the coordinator retains only its dedup
  key, and only under Coordinator.dedup="global" — ~100 bytes for the life
  of the run if key_of returns a digest rather than a path. Tree-shaped
  discovery can use dedup="parent" instead: a key lives only in the scope of
  the parent that begot it, and a completed task leaves nothing behind.
  A beget is parked in the outbox as received — the parent's spec, the
  discovery dict, the parent's dedup scope — and expanded one child per
  dispatch by the sender, which puts it back beneath whatever the workers
  have discovered meanwhile. A directory with a million subdirectories thus
  costs the coordinator the names it was sent, not a million specs at once;
  the stat line's deferred= counts the begets still parked.

Effect convention (the house rule)
  A side effect is legitimate only if (a) it is the function's stated job —
  named I/O at the edge: run, send_loop, log, *_pipes, finish,
  stop_orchestrators — or (b) the docstring carries a "Side effects:" line
  saying what it buys.
  The harness never imports oci (or any domain SDK): Worker.setup returns
  opaque state, so clients live entirely in your code, and collect/bus stay
  dependency-free.

Requires Python ≥ 3.10 (dataclass slots, structural pattern matching).
"""

from __future__ import annotations

import os
import sys
import math
import time
import signal
import socket
import argparse
import threading
import subprocess

from collections import deque

from collections.abc import Callable, Hashable, Iterable, Iterator, Sequence
from dataclasses     import dataclass
from typing          import Any

import qpipe

__all__ = [
    "Permanent", "Spec", "Discovery", "Emit", "Discover", "Job", "Coordinator",
    "Worker", "Pipeline", "Pipes", "run"
]


def log(msg: str) -> None:
    """Write one diagnostic line to stderr, unbuffered (frames go to pipes)."""
    print(msg, file=sys.stderr, flush=True)


def _short(spec: "Spec", limit: int = 100) -> str:
    """Truncated repr of an opaque spec, for failure logs."""
    text = repr(spec)
    return text if len(text) <= limit else text[: limit - 1] + "…"


# ---------------------------------------------------------------------------
# the contract — what a pipeline author supplies

class Permanent(Exception):
    """
    Raise from Worker.process for a failure that must NOT be retried (e.g. an
    HTTP 4xx). Anything else process raises is retried up to --max-attempts.
    """


Spec = dict[str, Any]                       # opaque to the harness
Discovery = dict[str, Any]                  # opaque to the harness
Emit = Callable[[dict[str, Any]], None]     # result(rec)
Discover = Callable[[dict[str, Any]], None] # discover(disc)


@dataclass(frozen=True, slots=True)
class Job:
    """The harness's view of one dispatch, handed to Worker.process."""

    task: int       # stable task id
    attempt: int    # 1-based send ordinal (echo into result records to dedup)
    spec: Spec      # your opaque payload


@dataclass(frozen=True, slots=True)
class Coordinator:
    """
    The supply + branching strategy. `expand`'s default makes this a pipelining
    coordinator; override it for work-generation. `key_of`'s default gives
    every spec a fresh id; provide it to dedup by identity, at one of two
    scopes:
      "global"  a key is remembered for the whole run — exact for any shape
                of discovery (DAGs, repeated seeds) at ~100-200 B per task
                ever registered; make keys compact (a 128-bit digest beats a
                path).
      "parent"  a key is remembered only in the scope of the parent that
                begot it, which lives while the parent is pending or any of
                its begets is still being expanded — exact for tree-shaped
                discovery (each child has one parent: a filesystem walk that
                does not follow symlinks), and nothing is retained per
                completed task. Seeds are not deduped.
    `seeds` runs in the coordinator's feeder thread and `expand` in its
    sender thread, one child at a time as children are dispatched; neither
    may assume the other's thread.
    """

    seeds: Callable[[], Iterator[Spec]]
    expand: Callable[[Spec, Discovery], Iterable[Spec]] = lambda parent, d: ()
    key_of: Callable[[Spec], Hashable] | None = None
    dedup: str = "global"


@dataclass(frozen=True, slots=True)
class Worker:
    """The per-task work strategy. `setup` builds per-thread state once."""

    setup: Callable[[], Any]
    process: Callable[[Any, Job, Emit, Discover], None]


@dataclass(frozen=True, slots=True)
class Pipeline:
    """
    Everything `run` needs to stand up a pipeline. make_coordinator and
    make_worker are the ONLY place argparse.Namespace is seen — they close the
    parsed config into the strategy callables, exactly as a *.from_args
    constructor does. add_*_args contribute role-specific CLI flags.
    """

    name: str
    describe: str
    default_pipes: "Pipes"
    make_coordinator: Callable[[argparse.Namespace], Coordinator]
    make_worker: Callable[[argparse.Namespace], Worker]
    add_coordinator_args: Callable[[argparse.ArgumentParser], None] = \
        lambda p: None
    add_worker_args: Callable[[argparse.ArgumentParser], None] = \
        lambda p: None


# ---------------------------------------------------------------------------
# configuration — frozen values built once at the CLI edge

@dataclass(frozen=True, slots=True)
class Pipes:
    """Addresses of the three qpipe orchestrators, plus the startup-wait."""

    work: str
    completions: str
    results: str
    wait: float

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "Pipes":
        """Lift the pipe addresses out of parsed args."""
        return cls(
            work=args.work, completions=args.completions, results=args.results,
            wait=args.wait
        )


@dataclass(frozen=True, slots=True)
class CoordinatorCfg:
    """
    Harness-owned coordinator policy: retries, flow control, timers, and the
    dispatch order.
    """

    task_timeout: float
    max_attempts: int
    in_flight: int
    watchdog_tick: float
    report_every: float
    hammer: float
    depth_first: bool = True    # outbox order: LIFO (depth-first) or FIFO

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "CoordinatorCfg":
        """Lift the common coordinator flags out of parsed args."""
        return cls(
            task_timeout=args.task_timeout, max_attempts=args.max_attempts,
            in_flight=args.in_flight, watchdog_tick=args.watchdog_tick,
            report_every=args.report_every, hammer=args.hammer,
            depth_first=(args.dispatch == "depth")
        )


# ---------------------------------------------------------------------------
# pipe plumbing

def wait_for_pipes(addrs: Sequence[str], timeout: float) -> None:
    """
    Block until every listed orchestrator passes healthcheck (raises on
    timeout).
    """
    for addr in addrs:
        qpipe.wait_until_healthy(addr, timeout=timeout)


def shutdown_pipes(addrs: Sequence[str]) -> None:
    """
    Best-effort request_shutdown on every listed orchestrator (Ctrl-C /
    hammer).
    """
    for addr in addrs:
        try:
            qpipe.request_shutdown(addr)
        except Exception:  # noqa: BLE001 — going down anyway
            pass


# ---------------------------------------------------------------------------
# ledger state — generic, sans-I/O, sans-strategy

@dataclass(slots=True)
class _Task:
    """
    One PENDING task's record. Lives inside Ledger only while the task is
    pending — completion or terminal failure deletes it, so the ledger's
    footprint follows the frontier of the walk, not its history (see Dispatch
    order and memory). Mutated only under the ledger's lock.

    `deadline` is +inf until the sender reports the frame written (sent()):
    a task waiting in the outbox cannot time out.
    """

    task_id: int
    spec: Spec
    attempts: int = 0
    deadline: float = math.inf
    children: set[Hashable] | None = None   # dedup scope (dedup="parent")


@dataclass(frozen=True, slots=True)
class Stats:
    """A consistent point-in-time snapshot of the ledger's counters."""

    outstanding: int
    tasks: int                              # registered (excludes deduped)
    done: int                               # completed
    failed: tuple[tuple[int, Spec], ...]    # (task id, spec)
    deferred: int                           # begets parked unexpanded
    sealed: bool


# Decisions — the ledger's entire output vocabulary. Send/Retry carry the
# spec so the coordinator can build the work frame; the spec is treated as
# immutable (never mutated by harness or pipeline), so the snapshot is safe
# even though the ledger holds the same reference.

@dataclass(frozen=True, slots=True)
class Send:
    """Decision: dispatch this task's first attempt to the work pipe."""

    task: int
    attempt: int
    spec: Spec


@dataclass(frozen=True, slots=True)
class Retry:
    """Decision: re-dispatch a task that timed out or errored retryably."""

    task: int
    attempt: int
    spec: Spec
    why: str


@dataclass(frozen=True, slots=True)
class Failed:
    """Decision: a task is terminally failed; report it, send nothing."""

    task: int
    attempts: int
    spec: Spec
    why: str


@dataclass(frozen=True, slots=True)
class Defer:
    """
    Decision: park a beget; its children are registered and dispatched
    lazily, one at a time, deduped in `scope` (None under dedup="global").
    """

    task: int
    spec: Spec
    disc: Discovery
    scope: set[Hashable] | None


Decision = Send | Retry | Failed | Defer


class Ledger:
    """
    Bookkeeping for the at-least-once protocol — generic, and nothing else.

    Owns the counter algebra from the module docstring, with the input source
    as the root task: outstanding starts at 1, register() +1, completion or
    terminal failure -1, seal() -1 once at source exhaustion, begets are just
    more register() calls. outstanding == 0 is global done — after seal.

    Holds only PENDING tasks: a task's record is deleted the moment it
    completes or fails terminally, so late frames for it are no-ops (done,
    error) or stale (a beget — defer returns []). What survives per
    completed task is its dedup key under dedup="global", and nothing under
    dedup="parent" (see Coordinator) — a parent's scope set is shared with
    its parked begets and lives exactly as long as one of them does.

    Sans-I/O AND sans-strategy: frames-worth-of-data go in, frozen Decision
    values come out; it never writes a pipe, logs, or calls user code (`key_of`
    is the single closure it holds, for dedup identity only). Its lock-guarded
    mutation is the documented exception. Time is injected — sent() arms a
    deadline, expired() sweeps against a clock — so retry/timeout/termination
    tests are equality assertions on Decision lists.
    """

    def __init__(
            self, key_of: Callable[[Spec], Hashable] | None,
            task_timeout: float, max_attempts: int, dedup: str = "global"
        ) -> None:
        """
        Set dedup identity + scope and policy; outstanding starts at 1
        (source). `dedup` is "global" or "parent" (see Coordinator).
        """
        if dedup not in ("global", "parent"):
            raise ValueError(
                f"dedup must be 'global' or 'parent', got {dedup!r}")
        self._lock = threading.Lock()
        self._key_of = key_of
        self._dedup = dedup
        self._tasks: dict[int, _Task] = {}     # PENDING tasks only
        self._seen: set[Hashable] = set()       # run-wide keys (dedup="global")
        self._next = 0
        self._registered = 0
        self._deferred = 0                      # begets parked unexpanded
        self._outstanding = 1                   # the source token
        self._sealed = False
        self._done = 0
        self._failed: list[tuple[int, Spec]] = []
        self._task_timeout = task_timeout
        self._max_attempts = max_attempts

    # -- public protocol -----------------------------------------------------

    def register(
            self, spec: Spec, scope: set[Hashable] | None = None
        ) -> list[Decision]:
        """
        Register one spec — a seed (no scope) or a begotten child (the scope
        its Defer carries); returns the Send to apply, or [] if key_of dedups
        it: against every key of the run (dedup="global"), or against the
        keys already in `scope` (dedup="parent" — seeds have no scope and
        are not deduped).
        """
        with self._lock:
            if self._key_of is not None and self._is_dup(spec, scope):
                return []
            self._next += 1
            tid = self._next
            task = _Task(task_id=tid, spec=spec)
            self._tasks[tid] = task
            self._registered += 1
            self._outstanding += 1
            self._stamp(task)
            return [Send(task=tid, attempt=task.attempts, spec=spec)]

    def defer(self, task_id: int, disc: Discovery) -> list[Decision]:
        """
        Park a beget of the PENDING task `task_id`: one Defer to apply, or []
        if the task has retired (a stale beget). The beget holds an
        outstanding token until release(), so the run cannot end while
        children wait unexpanded, and it carries the parent's dedup scope so
        late expansion still dedups against everything the parent begot —
        even after the parent itself has retired.
        """
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return []
            if self._dedup == "parent" and task.children is None:
                task.children = set()
            self._outstanding += 1
            self._deferred += 1
            return [Defer(task=task_id, spec=task.spec, disc=disc,
                          scope=task.children)]

    def release(self) -> None:
        """A parked beget is fully expanded (or dropped): return its token."""
        with self._lock:
            self._outstanding -= 1
            self._deferred -= 1

    def sent(self, task_id: int, attempt: int, now: float) -> None:
        """
        The sender wrote this attempt's frame: arm its re-dispatch deadline.
        No-op for a task no longer pending, or for a stale attempt (a later
        one was already decided) — there is nothing left to arm.
        """
        with self._lock:
            task = self._tasks.get(task_id)
            if task is not None and task.attempts == attempt:
                task.deadline = now + self._task_timeout

    def seal(self) -> None:
        """
        Mark the source exhausted: the root token completes (-1). Idempotent.
        """
        with self._lock:
            if not self._sealed:
                self._sealed = True
                self._outstanding -= 1

    def spec_of(self, task_id: int) -> Spec | None:
        """
        The spec of a PENDING task, or None — used to expand a beget's
        parent; None makes a beget for an already-retired task stale.
        """
        with self._lock:
            task = self._tasks.get(task_id)
            return task.spec if task is not None else None

    def complete(self, task_id: int) -> list[Decision]:
        """
        Retire a task on its `done` frame (no new work) and forget it.
        Late/dup no-op.
        """
        with self._lock:
            if self._tasks.pop(task_id, None) is not None:
                self._outstanding -= 1
                self._done += 1
            return []

    def fail_or_retry(
            self, task_id: int, why: str, *, permanent: bool
        ) -> list[Decision]:
        """
        Apply an `error` frame: one Retry, or one Failed. Late/dup no-op.
        """
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return []
            return self._retry_or_fail(task, why, permanent=permanent)

    def expired(self, now: float) -> list[Decision]:
        """
        Sweep overdue tasks; return Retry/Failed decisions. Only a task whose
        frame was actually sent has a finite deadline, so time spent queued
        in the outbox never counts.
        """
        with self._lock:
            overdue = [t for t in self._tasks.values() if now > t.deadline]
            decisions: list[Decision] = []
            for task in overdue:                # may delete from _tasks
                decisions.extend(
                    self._retry_or_fail(task, "timeout", permanent=False))
            return decisions

    def pending(self) -> int:
        """
        PENDING task count (the source token excluded) — feeder backpressure.
        """
        with self._lock:
            return self._outstanding - (0 if self._sealed else 1)

    def done(self) -> bool:
        """
        True once outstanding == 0 — sealed source AND all tasks terminal.
        """
        with self._lock:
            return self._outstanding == 0

    def stats(self) -> Stats:
        """Consistent snapshot for reporting and the final summary."""
        with self._lock:
            return Stats(
                outstanding=self._outstanding, tasks=self._registered,
                done=self._done, failed=tuple(self._failed),
                deferred=self._deferred, sealed=self._sealed
            )

    # -- internals (call only with self._lock held) ---------------------------

    def _is_dup(self, spec: Spec, scope: set[Hashable] | None) -> bool:
        """Consult, and update, the key set in scope; True if seen before."""
        assert self._key_of is not None
        key = self._key_of(spec)
        if self._dedup == "global":
            seen = self._seen
        elif scope is None:
            return False                    # a seed: nothing to dedup against
        else:
            seen = scope
        if key in seen:
            return True
        seen.add(key)
        return False

    def _retry_or_fail(
            self, task: _Task, why: str, *, permanent: bool
        ) -> list[Decision]:
        """Decide: another attempt (Retry) or terminal failure (Failed)."""
        if not permanent and task.attempts < self._max_attempts:
            self._stamp(task)
            return [Retry(task=task.task_id, attempt=task.attempts,
                          spec=task.spec, why=why)]

        del self._tasks[task.task_id]
        self._outstanding -= 1
        self._failed.append((task.task_id, task.spec))
        return [
            Failed(
                task=task.task_id, attempts=task.attempts, spec=task.spec,
                why=f"{why} (permanent)" if permanent else why
            )
        ]

    def _stamp(self, task: _Task) -> None:
        """
        Account for one dispatch decision: bump attempts and disarm the
        deadline until sent() reports the frame written.
        """
        task.attempts += 1
        task.deadline = math.inf


# ---------------------------------------------------------------------------
# coordinator — stateless wiring around the ledger and the strategy

@dataclass(slots=True)
class _Expansion:
    """A parked beget in the outbox, expanded one child per pop by the
    sender; `children` is expand()'s iterator once drawing has begun."""

    defer: Defer
    children: Iterator[Spec] | None = None


_Frame = tuple[int, int, Spec]              # (task, attempt, spec)
_Item = _Frame | _Expansion


class _Outbox:
    """
    The coordinator's unbounded in-process queue of work frames and parked
    begets, drained by the sender thread (see Flow control). Its order IS
    the dispatch policy: LIFO makes the walk depth-first, FIFO breadth-first
    (see Dispatch order and memory). `peak` records the deepest it ever got
    — the frontier the run had to hold in memory beyond the work pipe, a
    parked beget counting once however many children it still holds.
    """

    def __init__(self, depth_first: bool) -> None:
        self._items: deque[_Item] = deque()
        self._cv = threading.Condition()
        self._closed = False
        self._depth_first = depth_first
        self.peak = 0

    def __len__(self) -> int:
        with self._cv:
            return len(self._items)

    def push(self, item: _Item) -> None:
        """Queue one frame or parked beget. Never blocks: it is unbounded."""
        with self._cv:
            self._items.append(item)
            if len(self._items) > self.peak:
                self.peak = len(self._items)
            self._cv.notify()

    def pop(self) -> _Item | None:
        """
        The next item per the dispatch policy; blocks while empty. None
        once closed AND empty — the sender's signal to stop.
        """
        with self._cv:
            while not self._items and not self._closed:
                self._cv.wait()
            if not self._items:
                return None
            if self._depth_first:
                return self._items.pop()
            return self._items.popleft()

    def close(self) -> None:
        """No more frames will matter: let pop() return None once empty."""
        with self._cv:
            self._closed = True
            self._cv.notify_all()


def _coordinator_watchdog(
        *, name: str, ledger: Ledger, apply: Callable[[Decision], None],
        finish: Callable[[], None], pipes: Pipes, tick: float, hammer: float,
        loop_done: threading.Event, finishing: threading.Event
    ) -> None:
    """
    Timer half of the coordinator. Every `tick`: apply timeout decisions and,
    if a sweep drains outstanding to zero, fire the cascade (the completions
    loop is blocked in recv(); draining its pipe is what wakes it). Then the
    hammer: if the loop's EOF hasn't arrived `hammer` seconds after the cascade
    fired, escalate to shutdown on work+completions (results is left for a slow
    collector).
    """
    while not loop_done.wait(tick):
        for decision in ledger.expired(time.monotonic()):
            try:
                apply(decision)
            except Exception as e:  # noqa: BLE001 — pipe dying mid-retry
                log(f"[{name}] applying {decision!r} failed: {e}")
                return
        if ledger.done():
            finish()
            break

    if finishing.is_set() and not loop_done.wait(hammer):
        log(f"[{name}] no EOF {hammer:.0f}s after drain — "
            f"escalating to shutdown")
        shutdown_pipes((pipes.work, pipes.completions))


def _summarize(
        name: str, stats: Stats, elapsed: float, outbox_peak: int,
        botched_begets: int
    ) -> int:
    """Final log lines + exit code: 1 if anything failed or never finished."""
    log(f"[{name}] done: {stats.tasks} tasks, {stats.done} completed, "
        f"{len(stats.failed)} failed, outbox peak {outbox_peak}, "
        f"{elapsed:.1f}s")

    if botched_begets:
        log(f"[{name}] {botched_begets} begets could not be expanded "
            f"(expand()/key_of raised) — their children were never walked")

    if stats.outstanding > 0:
        pending = stats.outstanding - (0 if stats.sealed else 1)
        note = "" if stats.sealed else " and the source still open"
        log(f"[{name}] completions closed with {pending} tasks "
            f"outstanding{note} — results are partial")
        return 1

    if stats.failed:
        for tid, spec in stats.failed[:10]:
            log(f"[{name}]   FAILED task {tid}: {_short(spec)}")
        if len(stats.failed) > 10:
            log(f"[{name}]   ... +{len(stats.failed) - 10} more")
        return 1

    return 1 if botched_begets else 0


def _run_coordinator(
        name: str, pipes: Pipes, cfg: CoordinatorCfg, coordinator: Coordinator
    ) -> int:
    """
    Coordinator entry point: feed seeds, apply the ledger's decisions until
    outstanding hits zero, fire the drain cascade, summarize.

    Exit codes: 0 clean, 1 partial / failed, 130 interrupted.
    """
    wait_for_pipes(
        (pipes.work, pipes.completions, pipes.results), timeout=pipes.wait
    )
    log(f"[{name}] coordinator up")

    ledger = Ledger(
        key_of=coordinator.key_of, task_timeout=cfg.task_timeout,
        max_attempts=cfg.max_attempts, dedup=coordinator.dedup
    )

    t0 = time.monotonic()
    last_report = t0
    loop_done = threading.Event()
    finishing = threading.Event()
    finish_lock = threading.Lock()
    outbox = _Outbox(depth_first=cfg.depth_first)
    botched_begets = 0

    with qpipe.Producer.connect(pipes.work, codec="json") as work, \
         qpipe.Consumer.connect(pipes.completions, codec="json") as control:

        def draw(x: _Expansion) -> Spec | None:
            """The next child of a parked beget, or None once exhausted."""
            if x.children is None:
                x.children = iter(
                    coordinator.expand(x.defer.spec, x.defer.disc))
            return next(x.children, None)

        def send_loop() -> None:
            """
            The one effect on the work pipe. Drains the outbox in dispatch
            order; the blocking work.send() (a full orchestrator queue
            withholds its ACK) is confined here, where blocking is harmless,
            and a frame's deadline is armed only once its send has returned.

            A parked beget is expanded here, one child per pop: the beget
            goes back into the outbox first — beneath whatever the workers
            discover meanwhile, so the walk stays depth-first — then the
            child is registered (deduped in its parent's scope) and sent.
            Its token is released once it is exhausted; if expand()/key_of
            raise, the rest of that beget is dropped, loudly, and the run
            ends with rc 1.

            Ends when the outbox is closed and empty. Should the pipe die,
            every frame from then on is failed terminally and every parked
            beget dropped, instead of sent: an unsent task has no deadline,
            so nothing else would ever retire it, and the run still has to
            reach outstanding == 0 to end.
            """
            nonlocal botched_begets
            dead: str | None = None
            failed = dropped = 0
            while (item := outbox.pop()) is not None:
                if isinstance(item, _Expansion):
                    if dead is not None:
                        ledger.release()
                        dropped += 1
                        continue
                    try:
                        child = draw(item)
                        decisions = [] if child is None else \
                            ledger.register(child, scope=item.defer.scope)
                    except Exception as e:  # noqa: BLE001 — strategy code
                        log(f"[{name}] expand() failed for task "
                            f"{item.defer.task} ({_short(item.defer.spec)}): "
                            f"{type(e).__name__}: {e} — dropping the rest of "
                            f"this beget")
                        ledger.release()
                        botched_begets += 1
                        continue
                    if child is None:               # exhausted
                        ledger.release()
                        if ledger.done():
                            finish()
                        continue
                    outbox.push(item)               # back, under new arrivals
                    if not decisions:               # a duplicate child
                        continue
                    [send] = decisions
                    frame: _Frame = (send.task, send.attempt, send.spec)
                else:
                    frame = item
                task, attempt, spec = frame
                if dead is None:
                    try:
                        work.send(
                            {"task": task, "attempt": attempt, "spec": spec})
                    except Exception as e:  # noqa: BLE001 — pipe going away
                        dead = f"work pipe send failed: {e}"
                        log(f"[{name}] {dead} — failing every task still to "
                            f"be dispatched")
                    else:
                        ledger.sent(task, attempt, time.monotonic())
                        continue
                ledger.fail_or_retry(task, dead, permanent=True)
                failed += 1
            if failed:
                log(f"[{name}] {failed} tasks failed undispatched")
            if dropped:
                log(f"[{name}] {dropped} begets dropped unexpanded")

        def apply(decision: Decision) -> None:
            """Interpret one ledger Decision — the algebra's only consumer."""
            match decision:
                case Send(task=task, attempt=attempt, spec=spec):
                    outbox.push((task, attempt, spec))
                case Retry(task=task, attempt=attempt, spec=spec, why=why):
                    log(f"[{name}] retry {attempt}/{cfg.max_attempts} "
                        f"task {task} ({_short(spec)}): {why}")
                    outbox.push((task, attempt, spec))
                case Failed(task=task, attempts=attempts, spec=spec, why=why):
                    log(f"[{name}] FAILED task {task} after {attempts} "
                        f"attempts: {why} — {_short(spec)}")
                case Defer():
                    outbox.push(_Expansion(decision))

        def handle(msg: dict[str, Any]) -> list[Decision]:
            """
            Turn one completions-pipe frame into Decisions. A beget becomes
            a Defer — the strategy's expand runs later, in the sender.
            """
            kind = msg.get("k")
            if kind == "done":
                return ledger.complete(msg.get("task"))
            if kind == "error":
                return ledger.fail_or_retry(
                    msg.get("task"), str(msg.get("why", "")),
                    permanent=bool(msg.get("permanent")))
            if kind == "beget":         # [] if stale: parent already retired
                return ledger.defer(msg.get("task"), msg.get("spec") or {})
            return []

        def finish() -> None:
            """
            Fire the drain cascade exactly once, whichever thread is first.
            """
            with finish_lock:
                if finishing.is_set():
                    return
                finishing.set()
            for addr in (pipes.work, pipes.completions, pipes.results):
                try:
                    qpipe.request_drain(addr)
                except Exception as e:  # noqa: BLE001 — best-effort
                    log(f"[{name}] drain({addr}) failed: {e}")

        def maybe_report() -> None:
            """
            Throttled progress line.

            Side effects: stderr + its own throttle timestamp — buying rate
            limiting without threading a clock through the recv loop.
            """
            nonlocal last_report
            now = time.monotonic()
            if now - last_report < cfg.report_every:
                return
            last_report = now
            s = ledger.stats()
            el = max(now - t0, 1e-9)
            log(f"[{name}] outstanding={s.outstanding} tasks={s.tasks} "
                f"done={s.done} outbox={len(outbox)} deferred={s.deferred} "
                f"source={'sealed' if s.sealed else 'open'} "
                f"({s.done / el:.0f}/s, {el:.0f}s)")

        def feed() -> None:
            """
            The supply side: pull from coordinator.seeds(), gate on
            --in-flight, register, and seal at exhaustion. If everything
            already finished by seal time (empty/tiny source), fires the
            cascade itself — otherwise the watchdog catches it within a tick.

            A seeds() that raises is treated as a fatal source: log, seal (the
            run ends partial). Per-item soft rejects are the strategy's to
            log-and-skip.
            """
            n = 0
            try:
                for spec in coordinator.seeds():
                    while ledger.pending() >= cfg.in_flight:
                        time.sleep(0.05)        # backpressure — see docstring
                    for decision in ledger.register(spec):
                        apply(decision)
                    n += 1
            except Exception as e:  # noqa: BLE001 — pipes/source dying
                log(f"[{name}] seed feed aborted: {type(e).__name__}: {e}")
            finally:
                ledger.seal()
                log(f"[{name}] source sealed: {n} seeds")
                if ledger.done():
                    finish()

        feeder = threading.Thread(target=feed, daemon=True)
        sender = threading.Thread(target=send_loop, daemon=True)
        watchdog = threading.Thread(
            target=_coordinator_watchdog,
            kwargs=dict(
                name=name, ledger=ledger, apply=apply, finish=finish,
                pipes=pipes, tick=cfg.watchdog_tick, hammer=cfg.hammer,
                loop_done=loop_done, finishing=finishing
            ),
            daemon=True,
        )

        try:
            sender.start()
            feeder.start()
            watchdog.start()

            for msg in control:     # EOFs only once drained or shut down
                for decision in handle(msg):
                    apply(decision)
                maybe_report()
                if ledger.done():
                    break

        except KeyboardInterrupt:
            log(f"[{name}] interrupted — shutting all pipes down")
            loop_done.set()
            shutdown_pipes((pipes.work, pipes.completions, pipes.results))
            return 130

        finally:
            loop_done.set()
            outbox.close()
            sender.join(timeout=5.0)    # flush stragglers before the drain

    finish()                        # no-op if the watchdog beat us to it
    return _summarize(
        name, ledger.stats(), elapsed=time.monotonic() - t0,
        outbox_peak=outbox.peak, botched_begets=botched_begets
    )


# ---------------------------------------------------------------------------
# worker — drive the strategy, own the protocol envelopes

def _worker_loop(name: str, pipes: Pipes, worker: Worker, wid: str) -> None:
    """
    One consume → process → report loop. The result-before-done and
    beget-before-done orderings live here, not in process(): the `done` frame
    is sent only after process() returns, and result()/discover() have already
    ACKed. QpipeError propagates (pipes going away); Permanent becomes a
    non-retryable error frame, anything else a retryable one.
    """
    state = worker.setup()
    n = 0

    with qpipe.Consumer.connect(pipes.work, codec="json") as tasks, \
         qpipe.Producer.connect(pipes.results, codec="json") as results, \
         qpipe.Producer.connect(pipes.completions, codec="json") as control:

        for frame in tasks:         # EOFs when the work pipe drains
            job = Job(task=frame["task"], attempt=frame["attempt"],
                      spec=frame["spec"])
            t0 = time.monotonic()

            def discover(disc: dict[str, Any]) -> None:
                """Report discovered work (a beget) for this job."""
                control.send({"k": "beget", "task": job.task, "spec": disc})

            try:
                worker.process(state, job, results.send, discover)
            except qpipe.QpipeError:
                raise               # pipes are going away — stop
            except Permanent as e:
                control.send({
                    "k": "error",
                    "task": job.task,
                    "worker": wid,
                    "why": str(e),
                    "permanent": True
                })
                continue
            except Exception as e:  # noqa: BLE001 — report, keep serving
                control.send({
                    "k": "error",
                    "task": job.task,
                    "worker": wid,
                    "why": f"{type(e).__name__}: {e}",
                    "permanent": False
                })
                continue

            control.send({
                "k": "done",
                "task": job.task,
                "attempt": job.attempt,
                "worker": wid,
                "duration": round(time.monotonic() - t0, 3)
            })
            n += 1

    log(f"[{name} worker {wid}] {n} tasks")


def _run_worker(name: str, pipes: Pipes, worker: Worker, threads: int) -> int:
    """
    Worker entry point: spin `threads` independent loops and wait them out.
    Exit codes: 0 clean, 130 interrupted.
    """
    wait_for_pipes(
        (pipes.work, pipes.completions, pipes.results), timeout=pipes.wait
    )
    base = f"{socket.gethostname()}:{os.getpid()}"

    def boot(i: int) -> None:
        """Run one loop; downgrade expected shutdown races to a log line."""
        wid = f"{base}.{i}"
        try:
            _worker_loop(name, pipes, worker, wid)
        except qpipe.QpipeError as e:
            log(f"[{name} worker {wid}] pipe closed: {e}")
        except Exception as e:      # noqa: BLE001
            log(f"[{name} worker {wid}] fatal: {type(e).__name__}: {e}")

    pool = [threading.Thread(target=boot, args=(i,), daemon=True)
            for i in range(threads)]
    for t in pool:
        t.start()
    try:
        for t in pool:
            t.join()
    except KeyboardInterrupt:
        return 130
    return 0


# ---------------------------------------------------------------------------
# collect — drain the results pipe to JSONL

def _run_collect(results_addr: str, wait: float, output: str | None) -> int:
    """
    Collect entry point: stream the results pipe to JSONL on stdout or
    `output`.

    Side effects: opens `output` with "wb" — an existing file is truncated.

    Frames are already compact, newline-free JSON (the json codec guarantees
    it), so the raw codec passes the bytes straight through.
    """
    wait_for_pipes((results_addr,), timeout=wait)
    out = sys.stdout.buffer if output in (None, "-") else open(output, "wb")
    n = 0
    t0 = time.monotonic()

    try:
        with qpipe.Consumer.connect(results_addr, codec="raw") as recs:
            for frame in recs:      # EOFs via the drain cascade
                out.write(frame)
                out.write(b"\n")
                n += 1
                if n % 100_000 == 0:
                    el = time.monotonic() - t0
                    log(f"[collect] {n} results ({n / el:.0f}/s)")
    finally:
        if out is not sys.stdout.buffer:
            out.close()

    log(f"[collect] {n} results")
    return 0


# ---------------------------------------------------------------------------
# bus — spawn and supervise the pipe orchestrators

# Per-pipe stderr tags, padded to a common width so the prefixes line up in
# the terminal. The tag names each pipe's CONSUMER — work is drained by the
# workers, completions by the coordinator, results by collect — so flip these
# if you'd rather think of the pipes by producer.
_BUS_TAGS = {"work": "work", "completions": "coord", "results": "col"}


def stop_orchestrators(
        procs: dict[str, subprocess.Popen[str]], grace: float = 10.0
    ) -> None:
    """
    SIGTERM every still-running orchestrator, then SIGKILL the stragglers.
    Idempotent and best-effort — safe from both the supervisor and teardown.
    """
    for p in procs.values():
        if p.poll() is None:
            p.terminate()

    deadline = time.monotonic() + grace
    for name, p in procs.items():
        try:
            p.wait(timeout=max(deadline - time.monotonic(), 0.1))
        except subprocess.TimeoutExpired:
            log(f"[bus] {name} ignored SIGTERM after {grace:g}s — killing")
            p.kill()
            p.wait()


def _pump(
        tag: str, proc: subprocess.Popen[str], write_lock: threading.Lock
    ) -> None:
    """
    Forward one orchestrator's stderr to our stderr, one tagged line at a time.
    The child's stdout is not ours to touch — see the spawn site.

    Side effects (its entire job): reads proc.stderr to EOF and writes each
    line to sys.stderr under `write_lock`, so lines from the three pumps never
    interleave mid-line. It needs no stop signal — the child closing the pipe
    (i.e. exiting) is its EOF, which ends the loop.
    """
    if proc.stderr is None:             # unreachable with stderr=PIPE; for mypy
        return
    prefix = f"[{tag:<5}] "
    for line in proc.stderr:            # text mode: str, already newline-ended
        with write_lock:
            sys.stderr.write(prefix + line)
            sys.stderr.flush()


def _bus_healthy(
        procs: dict[str, subprocess.Popen[str]], addrs: dict[str, str],
        timeout: float
    ) -> bool:
    """
    Gate on every orchestrator's healthcheck within a shared budget,
    interleaving the probe with a liveness check so a child that dies
    immediately (bound port, bad flags) fails in ~1 s. Returns False on death
    or timeout; the caller owns the teardown.
    """
    deadline = time.monotonic() + timeout
    for name, addr in addrs.items():
        while True:
            rc = procs[name].poll()
            if rc is not None:
                log(f"[bus] {name} died before its healthcheck (rc={rc}) — "
                    f"its last stderr is tagged above")
                return False
            budget = deadline - time.monotonic()
            if budget <= 0:
                log(f"[bus] {name} not healthy after {timeout:g}s")
                return False
            try:
                qpipe.wait_until_healthy(addr, timeout=min(1.0, budget))
                break
            except qpipe.QpipeError:
                continue
    return True


def _supervise_bus(procs: dict[str, subprocess.Popen[str]]) -> int:
    """
    Wait for every orchestrator; the first NONZERO exit tears the survivors
    down. Clean exits may stagger — the drain cascade shuts pipes down at
    different times, and results must outlive work/completions for slow
    collectors.
    """
    rcs: dict[str, int] = {}
    while len(rcs) < len(procs):
        time.sleep(0.5)
        for name, p in procs.items():
            if name in rcs:
                continue
            rc = p.poll()
            if rc is None:
                continue
            rcs[name] = rc
            log(f"[bus] {name} exited rc={rc}")
            if rc != 0:
                log("[bus] nonzero exit — half a bus is worse than none, "
                    "stopping the rest")
                stop_orchestrators(procs)
                return 1
    return 0


def _run_bus(pipes: Pipes, rust_log: str, orchestrator: str) -> int:
    """
    Bus entry point: spawn one orchestrator per pipe, gate on their
    healthchecks (--wait budget), supervise until they exit.

    Each child's stderr is forwarded to THIS process's stderr, every line
    prefixed by a per-pipe tag ([work ] / [coord] / [col  ]). Each child's
    stdout is left inherited: the data plane passes straight through to the
    bus's own stdout, untouched and untagged, so results stay pipelineable.
    Assumes orchestrators exit once their pipe is drained/shut down; if they
    are run-forever servers, the bus ends only via signal.

    Side effects beyond the stated job: installs a SIGTERM handler converting
    the signal to SystemExit — a teardown path under systemd/Slurm/k8s stops,
    which otherwise orphan the children on their ports.

    Exit codes: 0 all clean, 1 spawn/health/crash, 130 interrupted; SIGTERM
    propagates as 143 after teardown.
    """
    env = os.environ.copy()
    env["RUST_LOG"] = rust_log

    def on_sigterm(signum: int, frame: Any) -> None:
        """Turn SIGTERM into an exception so `finally` runs the teardown."""
        log("[bus] SIGTERM — shutting the bus down")
        sys.exit(143)

    signal.signal(signal.SIGTERM, on_sigterm)

    addrs = {
        "work": pipes.work,
        "completions": pipes.completions,
        "results": pipes.results
    }
    procs: dict[str, subprocess.Popen[str]] = {}
    pumps: list[threading.Thread] = []
    write_lock = threading.Lock()

    try:
        for name, addr in addrs.items():
            # Stream discipline across the process boundary: stderr is the
            # diagnostic plane — ours to adopt, so PIPE it and pump it to our
            # own stderr, tagged. stdout is the data plane — not ours to touch:
            # left inherited (stdout=None, spelled out), the child writes
            # straight through to whatever the bus's stdout is connected to,
            # with no tag and no Python thread in the data path. text=True
            # shapes only the stderr pipe we read.
            proc = subprocess.Popen(
                [orchestrator, addr], env=env, stdout=None,
                stderr=subprocess.PIPE, text=True
            )
            procs[name] = proc
            pump = threading.Thread(
                target=_pump, args=(_BUS_TAGS[name], proc, write_lock),
                daemon=True
            )
            pump.start()
            pumps.append(pump)
            log(f"[bus] {name} pid={proc.pid} on {addr}")

        if not _bus_healthy(procs, addrs, timeout=pipes.wait):
            return 1

        log("[bus] all orchestrators healthy — supervising")
        return _supervise_bus(procs)

    except FileNotFoundError as e:
        log(f"[bus] cannot spawn {orchestrator!r}: {e}")
        return 1
    except KeyboardInterrupt:
        log("[bus] interrupted — stopping orchestrators")
        return 130
    finally:
        stop_orchestrators(procs)       # children die -> pipes EOF
        for pump in pumps:              # drain the tails, then the pumps end
            pump.join(timeout=2.0)


# ---------------------------------------------------------------------------
# CLI edge — argparse lives here and in the pipeline's make_*/add_* hooks

def add_pipe_args(
        p: argparse.ArgumentParser, defaults: Pipes, *names: str
    ) -> None:
    """
    Register --work/--completions/--results overrides (defaults from the
    pipeline) plus the shared --wait.
    """
    for n in names:
        d = getattr(defaults, n)
        p.add_argument(
            f"--{n}", default=d, metavar="HOST:PORT",
            help=f"{n} pipe orchestrator (default {d})"
        )
    p.add_argument(
        "--wait", type=float, default=defaults.wait,
        help=f"seconds to wait for pipes (default {defaults.wait:g})"
    )


def _add_coordinator_common(p: argparse.ArgumentParser) -> None:
    """The harness-owned coordinator policy flags."""
    p.add_argument(
        "--task-timeout", type=float, default=300.0,
        help="seconds before a task is re-dispatched (default 300)"
    )
    p.add_argument("--max-attempts", type=int, default=3)
    p.add_argument(
        "--in-flight", type=int, default=1000,
        help="feeder backpressure: max PENDING tasks; keep "
             "in_flight/throughput below --task-timeout (default 1000)"
    )
    p.add_argument("--watchdog-tick", type=float, default=5.0)
    p.add_argument("--report-every", type=float, default=5.0)
    p.add_argument(
        "--hammer", type=float, default=60.0,
        help="seconds after drain before escalating to shutdown"
    )
    p.add_argument(
        "--dispatch", choices=("depth", "breadth"), default="depth",
        help="outbox order: depth (LIFO; bounds the frontier of a "
             "work-generating walk) or breadth (FIFO; input order) "
             "(default depth)"
    )


def _build_parser(pipeline: Pipeline) -> argparse.ArgumentParser:
    """Assemble the CLI: four roles, harness flags + the pipeline's extras."""
    d = pipeline.default_pipes
    ap = argparse.ArgumentParser(description=pipeline.describe)
    sub = ap.add_subparsers(dest="role", required=True)

    c = sub.add_parser("coordinator", help="feed, track, terminate")
    _add_coordinator_common(c)
    pipeline.add_coordinator_args(c)
    add_pipe_args(c, d, "work", "completions", "results")

    w = sub.add_parser("worker", help="consume tasks, do the work")
    w.add_argument(
        "--threads", type=int, default=4,
        help="independent worker loops in this process (default 4)"
    )
    pipeline.add_worker_args(w)
    add_pipe_args(w, d, "work", "completions", "results")

    g = sub.add_parser("collect", help="drain the results pipe to JSONL")
    g.add_argument(
        "--output", "-o", help="file (default stdout, '-' works too)"
    )
    add_pipe_args(g, d, "results")

    b = sub.add_parser("bus", help="spawn + supervise the pipe orchestrators")
    b.add_argument(
        "--rust-log", default="debug",
        help="RUST_LOG for the orchestrators (default debug)"
    )
    b.add_argument(
        "--orchestrator", default="orchestrator", metavar="BIN",
        help="orchestrator binary to spawn (default from PATH)"
    )
    add_pipe_args(b, d, "work", "completions", "results")

    return ap


def run(pipeline: Pipeline, argv: list[str] | None = None) -> int:
    """
    The pipeline's single entry point. Parses the CLI and dispatches to the
    requested role. Each branch reads only the flags its own subparser
    registered — collect's parser deliberately knows just --results/--wait,
    so Pipes is constructed per-branch, never hoisted above the dispatch (a
    shared Pipes.from_args up here is the bug that silently asserts "every
    subparser defines every pipe flag"). argparse.Namespace dies here and in
    the pipeline's make_*/add_* hooks.
    """
    args = _build_parser(pipeline).parse_args(argv)

    if args.role == "coordinator":
        return _run_coordinator(
            pipeline.name, Pipes.from_args(args), CoordinatorCfg.from_args(args),
            pipeline.make_coordinator(args)
        )

    if args.role == "worker":
        return _run_worker(
            pipeline.name, Pipes.from_args(args), pipeline.make_worker(args),
            args.threads
        )

    if args.role == "collect":
        return _run_collect(args.results, args.wait, args.output)

    if args.role == "bus":
        return _run_bus(
            Pipes.from_args(args), args.rust_log, args.orchestrator
        )

    raise AssertionError(f"unhandled role {args.role!r}")  # unreachable

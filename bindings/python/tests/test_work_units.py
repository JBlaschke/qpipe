#!/usr/bin/env python

# SPDX-License-Identifier: AGPL-3.0-or-later

"""
qpipe.work sans-I/O units: the Ledger's decision algebra and the coordinator's
outbox order. No pipes, no threads — time is passed in, and every expectation
is an equality on a list of Decisions.
"""

import pytest
from qpipe.work import Defer, Failed, Ledger, Retry, Send, _Outbox

T = 10.0    # task timeout used throughout


def ledger(**overrides):
    kw = dict(key_of=None, task_timeout=T, max_attempts=3)
    kw.update(overrides)
    return Ledger(**kw)


# ---------------------------------------------------------------------------
# registration, dedup, retirement

def test_register_sends_first_attempt_and_dedups_by_key():
    L = ledger(key_of=lambda s: s["path"])
    a = {"path": "/a"}
    assert L.register(a) == [Send(task=1, attempt=1, spec=a)]
    assert L.register({"path": "/a"}) == []         # same key, other object
    b = {"path": "/b"}
    assert L.register(b) == [Send(task=2, attempt=1, spec=b)]
    s = L.stats()
    assert (s.tasks, s.outstanding, s.done, s.failed, s.sealed) == \
        (2, 3, 0, (), False)                        # 3 = two tasks + source


def test_complete_retires_and_forgets_the_task():
    L = ledger()
    L.register({"n": 1})
    assert L.spec_of(1) == {"n": 1}
    assert L.complete(1) == []
    assert L.spec_of(1) is None                     # a later beget is stale
    assert L.complete(1) == []                      # dup done: no double retire
    assert L.pending() == 0
    s = L.stats()
    assert (s.tasks, s.done, s.outstanding) == (1, 1, 1)    # source still open
    assert not L.done()
    L.seal()
    assert L.done()


def test_dedup_key_survives_completion():
    """The key is the one thing kept after a task retires: a re-discovery of
    a finished directory (a retried parent's late beget) must still dedup."""
    L = ledger(key_of=lambda s: s["path"])
    L.register({"path": "/a"})
    L.complete(1)
    assert L.register({"path": "/a"}) == []
    assert L.stats().tasks == 1


def test_parent_scoped_dedup_lives_with_the_begets():
    """dedup="parent": a child is checked only against the scope of the
    parent that begot it. The scope is shared between the parent's record
    and its parked begets, so it outlives the parent's retirement while a
    beget is still expanding, and the ledger keeps no run-wide set at all."""
    L = ledger(key_of=lambda s: s["path"], dedup="parent")
    root = {"path": "/"}
    assert L.register(root) == [Send(task=1, attempt=1, spec=root)]
    disc = {"children": ["/a", "/b"]}
    [d1] = L.defer(1, disc)
    assert d1 == Defer(task=1, spec=root, disc=disc, scope=set())
    [d2] = L.defer(1, disc)                     # a retry's duplicate beget
    assert d2.scope is d1.scope                 # one scope object per parent

    a = {"path": "/a"}
    assert L.register(a, scope=d1.scope) == [Send(task=2, attempt=1, spec=a)]
    assert L.register({"path": "/a"}, scope=d2.scope) == []    # across begets
    L.complete(1)                               # root retires, begets parked
    b = {"path": "/b"}
    assert L.register(b, scope=d1.scope) == [Send(task=3, attempt=1, spec=b)]
    # scope outlived it
    assert L.register({"path": "/b"}, scope=d2.scope) == []
    # a seed: no scope
    assert L.register({"path": "/a"}) == \
        [Send(task=4, attempt=1, spec={"path": "/a"})]
    assert L.register({"path": "/a"}, scope=set()) == \
        [Send(task=5, attempt=1, spec={"path": "/a"})]         # another parent
    # nothing run-wide
    assert not L._seen
    assert L.stats().tasks == 5


def test_global_dedup_ignores_the_scope():
    L = ledger(key_of=lambda s: s["path"])                     # dedup="global"
    L.register({"path": "/"})
    [d] = L.defer(1, {})
    # no per-parent sets
    assert d.scope is None
    a = {"path": "/a"}
    assert L.register(a, scope=set()) == [Send(task=2, attempt=1, spec=a)]
    assert L.register({"path": "/a"}, scope=set()) == []
    assert L.register({"path": "/a"}) == []


def test_parked_beget_holds_a_token_until_released():
    L = ledger()
    L.register({"n": 1})
    # stale: unknown parent
    assert L.defer(99, {}) == []
    [d] = L.defer(1, {"children": [2, 3]})
    assert d == Defer(task=1, spec={"n": 1}, disc={"children": [2, 3]},
                      scope=None)
    s = L.stats()
    assert (s.outstanding, s.deferred, L.pending()) == (3, 1, 2)
    L.complete(1)
    L.seal()
    assert not L.done()                         # the parked beget holds a token
    L.register({"n": 2}, scope=d.scope)         # one child drawn from it...
    L.release()                                 # ...and it is exhausted
    # child 2 pending
    assert (L.stats().deferred, L.done()) == (0, False)
    L.complete(2)
    assert L.done()


def test_dedup_scope_is_validated():
    with pytest.raises(ValueError):
        ledger(key_of=lambda s: s["path"], dedup="sometimes")


# ---------------------------------------------------------------------------
# deadlines: armed by sent(), never by register()

def test_deadline_is_armed_by_sent_not_by_register():
    L = ledger()
    spec = {"n": 1}
    L.register(spec)
    assert L.expired(now=1e9) == []                 # never sent: never overdue

    L.sent(1, attempt=1, now=100.0)
    assert L.expired(now=100.0 + T) == []           # due, not yet overdue
    assert L.expired(now=100.0 + T + 1) == \
        [Retry(task=1, attempt=2, spec=spec, why="timeout")]
    assert L.expired(now=1e9) == []                 # the retry is unsent

    L.sent(1, attempt=2, now=200.0)
    assert L.expired(now=200.0 + T + 1) == \
        [Retry(task=1, attempt=3, spec=spec, why="timeout")]
    L.sent(1, attempt=3, now=300.0)
    assert L.expired(now=300.0 + T + 1) == \
        [Failed(task=1, attempts=3, spec=spec, why="timeout")]
    assert L.spec_of(1) is None
    assert L.pending() == 0
    assert L.stats().failed == ((1, spec),)


def test_sent_ignores_unknown_task_and_stale_attempt():
    L = ledger()
    L.sent(99, attempt=1, now=0.0)                  # unknown: silently ignored
    spec = {"n": 1}
    L.register(spec)
    L.sent(1, attempt=1, now=0.0)
    assert L.expired(now=T + 1) == \
        [Retry(task=1, attempt=2, spec=spec, why="timeout")]
    L.sent(1, attempt=1, now=50.0)                  # stale attempt: no re-arm
    assert L.expired(now=1e9) == []
    L.complete(1)
    L.sent(1, attempt=2, now=60.0)                  # retired: nothing to arm
    assert L.expired(now=1e9) == []


def test_sweep_retires_many_tasks_at_once():
    """expired() deletes terminal tasks while sweeping — it must not trip
    over its own iteration, and must leave the ledger empty."""
    L = ledger(max_attempts=1)
    specs = [{"n": i} for i in range(1000)]
    for spec in specs:
        L.register(spec)
    for tid in range(1, 1001):
        L.sent(tid, attempt=1, now=0.0)
    assert L.expired(now=T + 1) == [
        Failed(task=i + 1, attempts=1, spec=specs[i], why="timeout")
        for i in range(1000)
    ]
    assert L.pending() == 0
    L.seal()
    assert L.done()


# ---------------------------------------------------------------------------
# error frames

def test_error_frames_retry_then_fail():
    L = ledger(max_attempts=2)
    spec = {"n": 1}
    L.register(spec)
    assert L.fail_or_retry(1, "boom", permanent=False) == \
        [Retry(task=1, attempt=2, spec=spec, why="boom")]
    assert L.fail_or_retry(1, "boom", permanent=False) == \
        [Failed(task=1, attempts=2, spec=spec, why="boom")]
    assert L.fail_or_retry(1, "boom", permanent=False) == []     # gone

    other = {"n": 2}
    L.register(other)
    assert L.fail_or_retry(2, "404", permanent=True) == \
        [Failed(task=2, attempts=1, spec=other, why="404 (permanent)")]
    assert L.fail_or_retry(3, "never registered", permanent=True) == []
    assert L.stats().failed == ((1, spec), (2, other))
    assert L.pending() == 0


# ---------------------------------------------------------------------------
# the outbox: order is the dispatch policy

@pytest.mark.parametrize("depth_first, expected",
                         [(True, [3, 2, 1]), (False, [1, 2, 3])],
                         ids=["depth", "breadth"])
def test_outbox_order_and_peak(depth_first, expected):
    ob = _Outbox(depth_first=depth_first)
    for t in (1, 2, 3):
        ob.push((t, 1, {"t": t}))
    assert len(ob) == 3 and ob.peak == 3
    assert [ob.pop()[0] for _ in range(3)] == expected
    assert len(ob) == 0 and ob.peak == 3
    ob.close()
    assert ob.pop() is None


def test_closed_outbox_still_serves_what_is_queued():
    ob = _Outbox(depth_first=True)
    ob.push((1, 1, {}))
    ob.push((2, 1, {}))
    ob.close()
    assert ob.pop() == (2, 1, {})
    assert ob.pop() == (1, 1, {})
    assert ob.pop() is None


# ---------------------------------------------------------------------------
# CLI edge

def test_dispatch_flag_maps_to_cfg():
    from qpipe.work import CoordinatorCfg, Pipeline, Pipes, _build_parser
    pipes = Pipes("a", "b", "c", 1.0)
    pl = Pipeline(name="t", describe="", default_pipes=pipes,
                  make_coordinator=lambda a: None, make_worker=lambda a: None)
    ap = _build_parser(pl)
    assert CoordinatorCfg.from_args(ap.parse_args(["coordinator"])).depth_first
    assert not CoordinatorCfg.from_args(
        ap.parse_args(["coordinator", "--dispatch", "breadth"])).depth_first

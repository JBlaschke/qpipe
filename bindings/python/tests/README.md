# Python Bindings Tests for qpipe

Some of these also double as a test harness for the qpipe executable.

## Test Descriptions

* Run fast tier only:   `uv run pytest -m "not e2e"`
* Run everything:       `maturin develop --uv && uv run pytest`

### test_codecs.py
Pure-Python codec layer. No .so rebuild, no orchestrator, runs in milliseconds.
This is the bulk of binding-test value.

### conftest.py + test_e2e.py

End-to-end through the real `orchestrator` binary. Requires `maturin develop
--uv` first and the binary on PATH (or set QPIPE_ORCHESTRATOR_BIN). Marked so
they can be deselected.

### test_work_units.py

`qpipe.work` sans-I/O units: the `Ledger`'s decision algebra (register /
dedup at either scope / complete / retry / fail, deadlines armed by `sent()`
rather than by registration, terminal tasks leaving the ledger, a parked
beget's `Defer` token and shared dedup scope) and the coordinator's `_Outbox`
(LIFO = depth-first vs FIFO = breadth-first). Time is passed in; every
expectation is an equality on a list of Decisions. Fast tier.

### test_work.py

`qpipe.work` harness (coordinator / worker roles) driven through in-process
fake pipes that keep the orchestrator's one load-bearing property: a bounded
queue whose `send()` blocks while full. Fast tier — no binary, no sockets.
Regression for the coordinator deadlock on a beget wider than the pipes
(blocking `work.send()` inside the completions-reading loop); fails within
seconds with a listing of where each harness thread is parked instead of
hanging the run. Also: the outbox peak a breadth-first vs depth-first walk
of a binary tree makes the coordinator hold, repeated begets deduped at
either scope, a 5000-child beget parked as one outbox entry, an `expand()`
that raises mid-beget, and a work pipe that dies mid-run failing its
undispatched tasks so the run still ends.

### test_e2e_work.py

The same regression against three real `orchestrator ADDR 50` processes (the
`orchestrator_factory` fixture in conftest.py passes the CAPACITY positional
argument), so the queue-full path is reached by a 400-child beget rather than
a 20 000-directory tree. e2e-marked like the rest.



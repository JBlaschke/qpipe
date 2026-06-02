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



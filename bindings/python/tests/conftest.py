#!/usr/bin/env python

# SPDX-License-Identifier: AGPL-3.0-or-later

"""orchestrator fixture for e2e"""

import os, socket, subprocess, time, pytest

def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port

def _wait_until_listening(host, port, timeout=5.0):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            socket.create_connection((host, port), timeout=0.2).close()
            return
        except OSError as e:
            last = e
            time.sleep(0.02)
    raise RuntimeError(f"orchestrator never came up on {host}:{port} ({last})")

def _start_orchestrator():
    # Binary name is `orchestrator` (per README). Override via env if it's not
    # on PATH, e.g. QPIPE_ORCHESTRATOR_BIN=../../target/debug/orchestrator
    binary = os.environ.get("QPIPE_ORCHESTRATOR_BIN", "orchestrator")
    port = _free_port()
    addr = f"127.0.0.1:{port}"
    # Positional arg = LISTEN_ADDR (orchestrator [LISTEN_ADDR] [CAPACITY] [STATS]).
    proc = subprocess.Popen(
        [binary, addr],
        env={**os.environ, "RUST_LOG": "warn"},
    )
    try:
        _wait_until_listening("127.0.0.1", port)
    except Exception:
        proc.terminate()
        raise
    return proc, addr


def _stop_orchestrator(proc):
    # No Python binding for request_shutdown; terminate the process.
    # (If you later expose a `shutdown` helper in the extension, prefer it.)
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@pytest.fixture(scope="module")
def orchestrator():
    # Shared across a module's tests. Fine for the single-producer/single-consumer
    # tests, which leave no residual queue state.
    proc, addr = _start_orchestrator()
    try:
        yield addr
    finally:
        _stop_orchestrator(proc)


@pytest.fixture
def fresh_orchestrator():
    # Function-scoped: a pristine orchestrator per test. Used by the MPMC test so
    # no leftover frames/pills can leak into (or out of) other tests.
    proc, addr = _start_orchestrator()
    try:
        yield addr
    finally:
        _stop_orchestrator(proc)

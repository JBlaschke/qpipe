#!/usr/bin/env python

# SPDX-License-Identifier: AGPL-3.0-or-later

"""orchestrator fixtures for e2e, plus a driver for qpipe.work harness tests"""

import os, socket, subprocess, sys, threading, time, traceback, pytest

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

def _start_orchestrator(capacity=None):
    # Binary name is `orchestrator` (per README). Override via env if it's not
    # on PATH, e.g. QPIPE_ORCHESTRATOR_BIN=../../target/debug/orchestrator
    binary = os.environ.get("QPIPE_ORCHESTRATOR_BIN", "orchestrator")
    port = _free_port()
    addr = f"127.0.0.1:{port}"
    # Positional args = LISTEN_ADDR [CAPACITY] [STATS]. CAPACITY is the queue
    # bound in frames (binary default 10 000); a small one lets a test reach
    # the queue-full path — where send() blocks on the withheld ACK — cheaply.
    cmd = [binary, addr] + ([str(capacity)] if capacity is not None else [])
    proc = subprocess.Popen(
        cmd,
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


@pytest.fixture
def orchestrator_factory():
    """Function-scoped factory: start(capacity=None) -> addr. Lets a test stand
    up several pipes (work / completions / results) with a chosen queue bound;
    every orchestrator it started is terminated at teardown."""
    procs = []

    def start(capacity=None):
        proc, addr = _start_orchestrator(capacity)
        procs.append(proc)
        return addr

    try:
        yield start
    finally:
        for proc in procs:
            _stop_orchestrator(proc)


# ---------------------------------------------------------------------------
# qpipe.work harness driver — shared by test_work.py (fake pipes) and
# test_e2e_work.py (real orchestrators)

HARNESS_CAPACITY = 50                  # frames per pipe under test
HARNESS_FANOUT = 8 * HARNESS_CAPACITY  # one beget wider than work+completions


@pytest.fixture
def fanout_pipeline():
    """
    Strategy factory: make(fanout) -> (Coordinator, Worker, processed). One
    seed whose processing begets `fanout` leaves in a single discover() call;
    every task (seed and leaves) appends its id to `processed`. Emits no
    results, so the results pipe stays out of the picture.
    """
    from qpipe.work import Coordinator, Worker

    def make(fanout):
        processed = []                  # list.append is atomic under the GIL

        def seeds():
            yield {"id": 0, "leaf": False}

        def expand(parent, disc):
            return ({"id": i, "leaf": True} for i in disc["children"])

        def process(_state, job, result, discover):
            if not job.spec["leaf"]:
                discover({"children": list(range(1, fanout + 1))})
            processed.append(job.task)

        return (Coordinator(seeds=seeds, expand=expand,
                            key_of=lambda s: s["id"]),
                Worker(setup=lambda: None, process=process),
                processed)

    return make


@pytest.fixture
def run_pipeline():
    """
    Driver: run(pipes, coordinator, worker, threads, deadline) -> coordinator
    rc. Runs the real _run_worker and _run_coordinator in daemon threads. If
    the coordinator has not returned within `deadline` seconds the test FAILS
    (instead of hanging the run) with the innermost frames of every live
    harness thread — a deadlock shows up as the coordinator parked in
    work.send() and the workers parked in control.send().
    """
    from qpipe.work import CoordinatorCfg, _run_coordinator, _run_worker

    def where_parked():
        frames = sys._current_frames()
        lines = []
        for t in threading.enumerate():
            f = frames.get(t.ident)
            if f is None or t is threading.current_thread():
                continue
            lines.append(f"  [{t.name}]")
            for fr in traceback.extract_stack(f)[-4:]:
                lines.append(f"      {os.path.basename(fr.filename)}:"
                             f"{fr.lineno} in {fr.name}")
        return "\n".join(lines)

    def run(pipes, coordinator, worker, threads, deadline):
        cfg = CoordinatorCfg(task_timeout=300.0, max_attempts=3,
                             in_flight=1000, watchdog_tick=0.2,
                             report_every=1e9, hammer=5.0)
        outcome = []

        def coordinate():
            try:
                outcome.append(_run_coordinator("t", pipes, cfg, coordinator))
            except BaseException as e:  # noqa: BLE001 — surfaced via outcome
                outcome.append(e)

        w = threading.Thread(target=_run_worker, args=("t", pipes, worker, threads),
                             name="worker", daemon=True)
        c = threading.Thread(target=coordinate, name="coordinator", daemon=True)
        w.start()
        c.start()
        c.join(deadline)
        if c.is_alive():
            pytest.fail(f"coordinator still running after {deadline:.0f}s — "
                        f"deadlock? threads are parked at:\n{where_parked()}")
        w.join(deadline)
        if isinstance(outcome[0], BaseException):
            raise outcome[0]
        return outcome[0]

    return run

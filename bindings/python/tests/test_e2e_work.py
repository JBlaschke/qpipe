#!/usr/bin/env python

# SPDX-License-Identifier: AGPL-3.0-or-later

"""
qpipe.work harness through real orchestrators with a small queue bound.

Same regression as test_work.py — coordinator deadlock on a beget wider than
the pipes — but against the real transport: three `orchestrator ADDR 50`
processes stand in for the default 10 000-frame pipes, so the queue-full path
that took a 20 000-directory tree to reach in production is hit by a
400-child beget in well under a second.
"""

import pytest
from qpipe.work import Pipes

from conftest import HARNESS_CAPACITY as CAPACITY, HARNESS_FANOUT as FANOUT

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(90)]


@pytest.fixture
def pipes(orchestrator_factory):
    return Pipes(work=orchestrator_factory(CAPACITY),
                 completions=orchestrator_factory(CAPACITY),
                 results=orchestrator_factory(CAPACITY),
                 wait=10.0)


@pytest.mark.parametrize("depth_first", [True, False], ids=["depth", "breadth"])
@pytest.mark.parametrize("threads", [1, 4])
def test_beget_wider_than_pipe_capacity_terminates(pipes, fanout_pipeline,
                                                   run_pipeline, threads,
                                                   depth_first):
    coordinator, worker, processed = fanout_pipeline(FANOUT)

    rc = run_pipeline(pipes, coordinator, worker, threads, deadline=20.0,
                      depth_first=depth_first)

    assert rc == 0
    assert len(processed) == FANOUT + 1

"""Tests for LineagePipeline.run_forever() loop semantics.

After dropping APScheduler, the run_forever loop pattern is:
  1. run a cycle (run_once)
  2. wait poll_interval_seconds AFTER the cycle finishes (Event.wait)
  3. repeat until stop() is called

The wait is interruptible via stop() so SIGTERM handlers wake it immediately.
"""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

from openlineage_confluent.config import (
    AppConfig,
    ConfluentConfig,
    OpenLineageConfig,
    PipelineConfig,
)
from openlineage_confluent.pipeline import LineagePipeline


def _make_app_cfg(*, interval: int = 1) -> AppConfig:
    return AppConfig(
        confluent=ConfluentConfig(
            CONFLUENT_CLOUD_API_KEY="x",
            CONFLUENT_CLOUD_API_SECRET="y",
        ),
        openlineage=OpenLineageConfig(OPENLINEAGE_TRANSPORT="console"),
        pipeline=PipelineConfig(PIPELINE_POLL_INTERVAL=interval),
    )


def _make_stub_pipeline(cfg: AppConfig) -> LineagePipeline:
    """LineagePipeline with all collaborators stubbed — only the loop is real."""
    with patch("openlineage_confluent.pipeline.ConfluentLineageClient"), \
         patch("openlineage_confluent.pipeline.ConfluentOpenLineageMapper"), \
         patch("openlineage_confluent.pipeline.LineageEmitter"), \
         patch("openlineage_confluent.pipeline.StateStore"):
        return LineagePipeline(cfg)


def test_stop_called_before_run_forever_short_circuits_loop():
    """If SIGTERM arrives during startup, run_forever() must honor it and not run any cycle."""
    pipe = _make_stub_pipeline(_make_app_cfg(interval=1))
    pipe.run_once = MagicMock()    # type: ignore[method-assign]

    pipe.stop()
    pipe.run_forever()

    assert pipe.run_once.call_count == 0


def test_stop_during_wait_wakes_loop_immediately():
    pipe = _make_stub_pipeline(_make_app_cfg(interval=60))    # long sleep
    pipe.run_once = MagicMock(return_value={"cycle": 1})    # type: ignore[method-assign]

    # Run the loop in a thread; stop it from outside after the first cycle.
    done = threading.Event()
    def _runner():
        pipe.run_forever()
        done.set()
    t = threading.Thread(target=_runner, daemon=True)
    t.start()

    # Give the loop time to enter the wait after cycle 1
    time.sleep(0.2)
    assert pipe.run_once.call_count == 1
    assert not done.is_set()    # still waiting

    t0 = time.monotonic()
    pipe.stop()
    assert done.wait(timeout=2.0), "stop() did not wake the loop"
    elapsed = time.monotonic() - t0
    assert elapsed < 0.5, f"stop took too long to wake the loop ({elapsed:.2f}s)"

    # Only one cycle ran — stop hit during the wait, not mid-cycle
    assert pipe.run_once.call_count == 1


def test_loop_runs_multiple_cycles_with_short_interval():
    pipe = _make_stub_pipeline(_make_app_cfg(interval=1))
    call_count = {"n": 0}

    def _stub_cycle():
        call_count["n"] += 1
        if call_count["n"] >= 3:
            pipe.stop()
        return {"cycle": call_count["n"]}

    pipe.run_once = _stub_cycle    # type: ignore[method-assign]

    # Bypass real waits — patch the Event so wait returns immediately.
    pipe._stop_event.wait = MagicMock(return_value=False)    # type: ignore[method-assign]

    pipe.run_forever()
    assert call_count["n"] == 3


def test_cycle_exception_does_not_break_loop():
    pipe = _make_stub_pipeline(_make_app_cfg(interval=1))
    call_count = {"n": 0}

    def _stub_cycle():
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("fetch failed")
        if call_count["n"] >= 2:
            pipe.stop()
        return {"cycle": call_count["n"]}

    pipe.run_once = _stub_cycle    # type: ignore[method-assign]
    pipe._stop_event.wait = MagicMock(return_value=False)    # type: ignore[method-assign]

    pipe.run_forever()    # must NOT propagate the RuntimeError
    assert call_count["n"] == 2

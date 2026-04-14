"""Tests for LineageEmitter diff-tracking logic."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent, RunState

from openlineage_confluent.emitter.emitter import LineageEmitter, _event_fingerprint


def _make_event(job_name: str, inputs: list[str], outputs: list[str]) -> RunEvent:
    return RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        job=Job(namespace="kafka-connect://env-test", name=job_name),
        run=Run(runId="00000000-0000-0000-0000-000000000001"),
        inputs=[InputDataset(namespace="kafka://broker:9092", name=t) for t in inputs],
        outputs=[OutputDataset(namespace="kafka://broker:9092", name=t) for t in outputs],
        producer="test/0.0.1",
    )


def test_fingerprint_same_for_same_event() -> None:
    e1 = _make_event("job-a", ["topic-in"], ["topic-out"])
    e2 = _make_event("job-a", ["topic-in"], ["topic-out"])
    assert _event_fingerprint(e1) == _event_fingerprint(e2)


def test_fingerprint_differs_when_outputs_change() -> None:
    e1 = _make_event("job-a", ["in"], ["out-1"])
    e2 = _make_event("job-a", ["in"], ["out-2"])
    assert _event_fingerprint(e1) != _event_fingerprint(e2)


def test_emit_sends_event(ol_cfg) -> None:
    emitter = LineageEmitter(ol_cfg)  # console transport
    event = _make_event("job-a", ["in"], ["out"])
    result = emitter.emit(event, force=True)
    assert result is True


def test_emit_skips_unchanged_on_second_call(ol_cfg) -> None:
    emitter = LineageEmitter(ol_cfg)
    event = _make_event("job-b", ["in"], ["out"])
    emitter.emit(event, force=True)   # first — always emits
    result = emitter.emit(event)       # second — should skip (same fingerprint)
    assert result is False


def test_emit_resends_after_lineage_change(ol_cfg) -> None:
    emitter = LineageEmitter(ol_cfg)
    event_v1 = _make_event("job-c", ["in"], ["out-old"])
    event_v2 = _make_event("job-c", ["in"], ["out-new"])
    emitter.emit(event_v1, force=True)
    result = emitter.emit(event_v2)
    assert result is True


def test_emit_batch_returns_counts(ol_cfg) -> None:
    emitter = LineageEmitter(ol_cfg)
    events = [
        _make_event("job-x", ["in"], ["out"]),
        _make_event("job-y", ["a"], ["b"]),
    ]
    emitted, skipped = emitter.emit_batch(events, force=True)
    assert emitted == 2
    assert skipped == 0


def test_emit_batch_skips_unchanged_on_second_batch(ol_cfg) -> None:
    emitter = LineageEmitter(ol_cfg)
    events = [_make_event("job-z", ["in"], ["out"])]
    emitter.emit_batch(events, force=True)
    emitted, skipped = emitter.emit_batch(events)
    assert emitted == 0
    assert skipped == 1


def test_reset_state_forces_re_emit(ol_cfg) -> None:
    emitter = LineageEmitter(ol_cfg)
    event = _make_event("job-r", ["in"], ["out"])
    emitter.emit(event, force=True)
    emitter.reset_state()
    result = emitter.emit(event)  # after reset, diff state cleared → will emit
    assert result is True

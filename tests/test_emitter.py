"""Tests for LineageEmitter diff-tracking and removal-detection logic."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent, RunState

from openlineage_confluent.emitter.emitter import LineageEmitter, _event_fingerprint


def _make_event(
    job_name: str,
    inputs: list[str],
    outputs: list[str],
    namespace: str = "kafka-connect://env-test",
) -> RunEvent:
    return RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        job=Job(namespace=namespace, name=job_name),
        run=Run(runId="00000000-0000-0000-0000-000000000001"),
        inputs=[InputDataset(namespace="kafka://broker:9092", name=t) for t in inputs],
        outputs=[OutputDataset(namespace="kafka://broker:9092", name=t) for t in outputs],
        producer="test/0.0.1",
    )


# ── Fingerprint ────────────────────────────────────────────────────────────────

def test_fingerprint_same_for_same_event() -> None:
    e1 = _make_event("job-a", ["topic-in"], ["topic-out"])
    e2 = _make_event("job-a", ["topic-in"], ["topic-out"])
    assert _event_fingerprint(e1) == _event_fingerprint(e2)


def test_fingerprint_differs_when_outputs_change() -> None:
    e1 = _make_event("job-a", ["in"], ["out-1"])
    e2 = _make_event("job-a", ["in"], ["out-2"])
    assert _event_fingerprint(e1) != _event_fingerprint(e2)


# ── Single emit ────────────────────────────────────────────────────────────────

def test_emit_sends_event(ol_cfg) -> None:
    emitter = LineageEmitter(ol_cfg)
    event = _make_event("job-a", ["in"], ["out"])
    assert emitter.emit(event, force=True) is True


def test_emit_skips_unchanged_on_second_call(ol_cfg) -> None:
    emitter = LineageEmitter(ol_cfg)
    event = _make_event("job-b", ["in"], ["out"])
    emitter.emit(event, force=True)
    assert emitter.emit(event) is False


def test_emit_resends_after_lineage_change(ol_cfg) -> None:
    emitter = LineageEmitter(ol_cfg)
    emitter.emit(_make_event("job-c", ["in"], ["out-old"]), force=True)
    assert emitter.emit(_make_event("job-c", ["in"], ["out-new"])) is True


# ── Batch emit ─────────────────────────────────────────────────────────────────

def test_emit_batch_returns_counts(ol_cfg) -> None:
    emitter = LineageEmitter(ol_cfg)
    events = [
        _make_event("job-x", ["in"], ["out"]),
        _make_event("job-y", ["a"], ["b"]),
    ]
    emitted, skipped, removed = emitter.emit_batch(events, force=True)
    assert emitted == 2
    assert skipped == 0
    assert removed == 0


def test_emit_batch_skips_unchanged_on_second_batch(ol_cfg) -> None:
    emitter = LineageEmitter(ol_cfg)
    events = [_make_event("job-z", ["in"], ["out"])]
    emitter.emit_batch(events, force=True)
    emitted, skipped, removed = emitter.emit_batch(events)
    assert emitted == 0
    assert skipped == 1
    assert removed == 0


# ── Removal detection ──────────────────────────────────────────────────────────

def test_removal_detected_when_job_disappears(ol_cfg) -> None:
    emitter = LineageEmitter(ol_cfg)
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    # Cycle 1: both jobs present
    emitter.emit_batch([event_a, event_b], force=True)

    # Cycle 2: job-b is gone
    emitted, skipped, removed = emitter.emit_batch([event_a])
    assert removed == 1
    assert emitted == 0   # job-a unchanged → skipped
    assert skipped == 1


def test_removal_sends_abort_event(ol_cfg) -> None:
    """The ABORT event must actually be dispatched to the backend."""
    emitter = LineageEmitter(ol_cfg)
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    emitter.emit_batch([event_a, event_b], force=True)

    with patch.object(emitter._client, "emit") as mock_emit:
        emitter.emit_batch([event_a])

    # The mock should have been called once — for the ABORT event on job-b
    assert mock_emit.call_count == 1
    abort_event: RunEvent = mock_emit.call_args[0][0]
    assert abort_event.eventType == RunState.ABORT
    assert abort_event.job.name == "job-b"


def test_removed_job_not_reported_again_next_cycle(ol_cfg) -> None:
    emitter = LineageEmitter(ol_cfg)
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    emitter.emit_batch([event_a, event_b], force=True)
    emitter.emit_batch([event_a])          # job-b removed → ABORT sent

    # Cycle 3: job-b still absent — must NOT report it again
    _, _, removed = emitter.emit_batch([event_a])
    assert removed == 0


def test_removed_job_can_rejoin(ol_cfg) -> None:
    """A job that disappears and then reappears should be re-emitted normally."""
    emitter = LineageEmitter(ol_cfg)
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    emitter.emit_batch([event_a, event_b], force=True)
    emitter.emit_batch([event_a])          # job-b removed

    # job-b comes back (e.g., Flink statement restarted)
    emitted, skipped, removed = emitter.emit_batch([event_a, event_b])
    assert emitted == 1   # job-b emitted as new
    assert removed == 0


def test_removal_preserves_namespace(ol_cfg) -> None:
    """The ABORT event must carry the correct job namespace, not a default."""
    emitter = LineageEmitter(ol_cfg)
    flink_event = _make_event("ol-enrich-orders", ["raw"], ["enriched"],
                              namespace="flink://env-m2qxq")
    connect_event = _make_event("ol-http-sink", ["enriched"], [],
                                namespace="kafka-connect://env-m2qxq")

    emitter.emit_batch([flink_event, connect_event], force=True)

    with patch.object(emitter._client, "emit") as mock_emit:
        emitter.emit_batch([flink_event])  # connect job removed

    abort_event: RunEvent = mock_emit.call_args[0][0]
    assert abort_event.job.namespace == "kafka-connect://env-m2qxq"
    assert abort_event.job.name == "ol-http-sink"


# ── Reset state ────────────────────────────────────────────────────────────────

def test_reset_state_forces_re_emit(ol_cfg) -> None:
    emitter = LineageEmitter(ol_cfg)
    event = _make_event("job-r", ["in"], ["out"])
    emitter.emit(event, force=True)
    emitter.reset_state()
    assert emitter.emit(event) is True


def test_reset_state_clears_known_jobs(ol_cfg) -> None:
    """After reset, previously seen jobs are forgotten — no spurious removals."""
    emitter = LineageEmitter(ol_cfg)
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    emitter.emit_batch([event_a, event_b], force=True)
    emitter.reset_state()

    # After reset, emitting only job-a should not flag job-b as removed
    _, _, removed = emitter.emit_batch([event_a], force=True)
    assert removed == 0

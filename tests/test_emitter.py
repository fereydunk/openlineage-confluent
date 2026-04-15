"""Tests for LineageEmitter diff-tracking, removal-detection, and state persistence."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent, RunState

from openlineage_confluent.emitter.emitter import LineageEmitter, _event_fingerprint


@pytest.fixture()
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "state.json"


def _make_emitter(ol_cfg, state_file: Path) -> LineageEmitter:
    return LineageEmitter(ol_cfg, state_file=state_file)


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

def test_emit_sends_event(ol_cfg, state_file) -> None:
    emitter = _make_emitter(ol_cfg, state_file)
    assert emitter.emit(_make_event("job-a", ["in"], ["out"]), force=True) is True


def test_emit_skips_unchanged_on_second_call(ol_cfg, state_file) -> None:
    emitter = _make_emitter(ol_cfg, state_file)
    event = _make_event("job-b", ["in"], ["out"])
    emitter.emit(event, force=True)
    assert emitter.emit(event) is False


def test_emit_resends_after_lineage_change(ol_cfg, state_file) -> None:
    emitter = _make_emitter(ol_cfg, state_file)
    emitter.emit(_make_event("job-c", ["in"], ["out-old"]), force=True)
    assert emitter.emit(_make_event("job-c", ["in"], ["out-new"])) is True


# ── Batch emit ─────────────────────────────────────────────────────────────────

def test_emit_batch_returns_counts(ol_cfg, state_file) -> None:
    emitter = _make_emitter(ol_cfg, state_file)
    events = [_make_event("job-x", ["in"], ["out"]), _make_event("job-y", ["a"], ["b"])]
    emitted, skipped, removed = emitter.emit_batch(events, force=True)
    assert emitted == 2 and skipped == 0 and removed == 0


def test_emit_batch_skips_unchanged_on_second_batch(ol_cfg, state_file) -> None:
    emitter = _make_emitter(ol_cfg, state_file)
    events = [_make_event("job-z", ["in"], ["out"])]
    emitter.emit_batch(events, force=True)
    emitted, skipped, removed = emitter.emit_batch(events)
    assert emitted == 0 and skipped == 1 and removed == 0


# ── Removal detection ──────────────────────────────────────────────────────────

def test_removal_detected_when_job_disappears(ol_cfg, state_file) -> None:
    emitter = _make_emitter(ol_cfg, state_file)
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    emitter.emit_batch([event_a, event_b], force=True)

    emitted, skipped, removed = emitter.emit_batch([event_a])
    assert removed == 1
    assert emitted == 0 and skipped == 1


def test_removal_sends_abort_event(ol_cfg, state_file) -> None:
    """The ABORT event must actually be dispatched to the backend."""
    emitter = _make_emitter(ol_cfg, state_file)
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    emitter.emit_batch([event_a, event_b], force=True)

    with patch.object(emitter._client, "emit") as mock_emit:
        emitter.emit_batch([event_a])

    assert mock_emit.call_count == 1
    abort_event: RunEvent = mock_emit.call_args[0][0]
    assert abort_event.eventType == RunState.ABORT
    assert abort_event.job.name == "job-b"


def test_removed_job_not_reported_again_next_cycle(ol_cfg, state_file) -> None:
    emitter = _make_emitter(ol_cfg, state_file)
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    emitter.emit_batch([event_a, event_b], force=True)
    emitter.emit_batch([event_a])

    _, _, removed = emitter.emit_batch([event_a])
    assert removed == 0


def test_removed_job_can_rejoin(ol_cfg, state_file) -> None:
    """A job that disappears and reappears should be re-emitted normally."""
    emitter = _make_emitter(ol_cfg, state_file)
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    emitter.emit_batch([event_a, event_b], force=True)
    emitter.emit_batch([event_a])

    emitted, skipped, removed = emitter.emit_batch([event_a, event_b])
    assert emitted == 1 and removed == 0


def test_removal_preserves_namespace(ol_cfg, state_file) -> None:
    """The ABORT event must carry the correct job namespace."""
    emitter = _make_emitter(ol_cfg, state_file)
    flink_event   = _make_event("ol-enrich-orders", ["raw"], ["enriched"],
                                namespace="flink://env-m2qxq")
    connect_event = _make_event("ol-http-sink", ["enriched"], [],
                                namespace="kafka-connect://env-m2qxq")

    emitter.emit_batch([flink_event, connect_event], force=True)

    with patch.object(emitter._client, "emit") as mock_emit:
        emitter.emit_batch([flink_event])

    abort_event: RunEvent = mock_emit.call_args[0][0]
    assert abort_event.job.namespace == "kafka-connect://env-m2qxq"
    assert abort_event.job.name == "ol-http-sink"


# ── State persistence ──────────────────────────────────────────────────────────

def test_state_file_written_after_batch(ol_cfg, state_file) -> None:
    emitter = _make_emitter(ol_cfg, state_file)
    emitter.emit_batch([_make_event("job-a", ["in"], ["out"])], force=True)
    assert state_file.exists()


def test_state_survives_process_restart(ol_cfg, state_file) -> None:
    """A new emitter instance loading saved state detects removals correctly."""
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    # "Process 1" — seeds state and exits
    _make_emitter(ol_cfg, state_file).emit_batch([event_a, event_b], force=True)

    # "Process 2" — fresh instance, loads state, job-b is gone
    emitter2 = _make_emitter(ol_cfg, state_file)
    emitted, skipped, removed = emitter2.emit_batch([event_a])
    assert removed == 1


def test_fingerprints_survive_restart(ol_cfg, state_file) -> None:
    """Unchanged jobs are still skipped after a process restart."""
    event = _make_event("job-a", ["in"], ["out"])

    _make_emitter(ol_cfg, state_file).emit_batch([event], force=True)

    emitter2 = _make_emitter(ol_cfg, state_file)
    emitted, skipped, removed = emitter2.emit_batch([event])
    assert emitted == 0 and skipped == 1


def test_corrupt_state_file_starts_fresh(ol_cfg, state_file) -> None:
    """A corrupt state file should not crash — just start with empty state."""
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text("not valid json {{{")

    emitter = _make_emitter(ol_cfg, state_file)
    assert emitter._known_jobs == {}


# ── Reset state ────────────────────────────────────────────────────────────────

def test_reset_state_forces_re_emit(ol_cfg, state_file) -> None:
    emitter = _make_emitter(ol_cfg, state_file)
    event = _make_event("job-r", ["in"], ["out"])
    emitter.emit(event, force=True)
    emitter.reset_state()
    assert emitter.emit(event) is True


def test_reset_state_clears_known_jobs(ol_cfg, state_file) -> None:
    """After reset, previously seen jobs are forgotten — no spurious removals."""
    emitter = _make_emitter(ol_cfg, state_file)
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    emitter.emit_batch([event_a, event_b], force=True)
    emitter.reset_state()

    _, _, removed = emitter.emit_batch([event_a], force=True)
    assert removed == 0


def test_reset_state_deletes_state_file(ol_cfg, state_file) -> None:
    emitter = _make_emitter(ol_cfg, state_file)
    emitter.emit_batch([_make_event("job-a", ["in"], ["out"])], force=True)
    assert state_file.exists()

    emitter.reset_state()
    assert not state_file.exists()

"""Tests for LineageEmitter — diff-tracking, removal detection, parallel emission."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent, RunState

from openlineage_confluent.emitter.emitter import LineageEmitter, _event_fingerprint
from openlineage_confluent.emitter.state_store import StateStore


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_store(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "state.db")


def _make_emitter(ol_cfg, tmp_path: Path, *, max_workers: int = 1) -> LineageEmitter:
    """Create an emitter backed by a temp SQLite store. max_workers=1 keeps
    tests deterministic; raise it in tests that specifically exercise parallelism."""
    return LineageEmitter(ol_cfg, state_store=_make_store(tmp_path), max_workers=max_workers)


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

def test_emit_sends_event(ol_cfg, tmp_path) -> None:
    emitter = _make_emitter(ol_cfg, tmp_path)
    assert emitter.emit(_make_event("job-a", ["in"], ["out"]), force=True) is True


def test_emit_skips_unchanged_on_second_call(ol_cfg, tmp_path) -> None:
    emitter = _make_emitter(ol_cfg, tmp_path)
    event = _make_event("job-b", ["in"], ["out"])
    emitter.emit(event, force=True)
    assert emitter.emit(event) is False


def test_emit_resends_after_lineage_change(ol_cfg, tmp_path) -> None:
    emitter = _make_emitter(ol_cfg, tmp_path)
    emitter.emit(_make_event("job-c", ["in"], ["out-old"]), force=True)
    assert emitter.emit(_make_event("job-c", ["in"], ["out-new"])) is True


# ── Batch emit ─────────────────────────────────────────────────────────────────

def test_emit_batch_returns_counts(ol_cfg, tmp_path) -> None:
    emitter = _make_emitter(ol_cfg, tmp_path)
    events = [_make_event("job-x", ["in"], ["out"]), _make_event("job-y", ["a"], ["b"])]
    emitted, skipped, removed = emitter.emit_batch(events, force=True)
    assert emitted == 2 and skipped == 0 and removed == 0


def test_emit_batch_skips_unchanged_on_second_batch(ol_cfg, tmp_path) -> None:
    emitter = _make_emitter(ol_cfg, tmp_path)
    events = [_make_event("job-z", ["in"], ["out"])]
    emitter.emit_batch(events, force=True)
    emitted, skipped, removed = emitter.emit_batch(events)
    assert emitted == 0 and skipped == 1 and removed == 0


# ── Removal detection ──────────────────────────────────────────────────────────

def test_removal_detected_when_job_disappears(ol_cfg, tmp_path) -> None:
    emitter = _make_emitter(ol_cfg, tmp_path)
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    emitter.emit_batch([event_a, event_b], force=True)

    emitted, skipped, removed = emitter.emit_batch([event_a])
    assert removed == 1
    assert emitted == 0 and skipped == 1


def test_removal_sends_abort_event(ol_cfg, tmp_path) -> None:
    emitter = _make_emitter(ol_cfg, tmp_path)
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    emitter.emit_batch([event_a, event_b], force=True)

    with patch.object(emitter._client, "emit") as mock_emit:
        emitter.emit_batch([event_a])

    assert mock_emit.call_count == 1
    abort_event: RunEvent = mock_emit.call_args[0][0]
    assert abort_event.eventType == RunState.ABORT
    assert abort_event.job.name == "job-b"


def test_removed_job_not_reported_again_next_cycle(ol_cfg, tmp_path) -> None:
    emitter = _make_emitter(ol_cfg, tmp_path)
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    emitter.emit_batch([event_a, event_b], force=True)
    emitter.emit_batch([event_a])

    _, _, removed = emitter.emit_batch([event_a])
    assert removed == 0


def test_removed_job_can_rejoin(ol_cfg, tmp_path) -> None:
    emitter = _make_emitter(ol_cfg, tmp_path)
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    emitter.emit_batch([event_a, event_b], force=True)
    emitter.emit_batch([event_a])

    emitted, skipped, removed = emitter.emit_batch([event_a, event_b])
    assert emitted == 1 and removed == 0


def test_removal_preserves_namespace(ol_cfg, tmp_path) -> None:
    emitter = _make_emitter(ol_cfg, tmp_path)
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


# ── State persistence (SQLite) ─────────────────────────────────────────────────

def test_state_db_written_after_batch(ol_cfg, tmp_path) -> None:
    emitter = _make_emitter(ol_cfg, tmp_path)
    emitter.emit_batch([_make_event("job-a", ["in"], ["out"])], force=True)
    assert (tmp_path / "state.db").exists()


def test_state_survives_process_restart(ol_cfg, tmp_path) -> None:
    """Removal detected across two separate emitter instances (simulated restart)."""
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    # "Process 1"
    _make_emitter(ol_cfg, tmp_path).emit_batch([event_a, event_b], force=True)

    # "Process 2" — loads state from disk, job-b is gone
    emitter2 = _make_emitter(ol_cfg, tmp_path)
    emitted, skipped, removed = emitter2.emit_batch([event_a])
    assert removed == 1


def test_fingerprints_survive_restart(ol_cfg, tmp_path) -> None:
    """Unchanged jobs are still skipped after a process restart."""
    event = _make_event("job-a", ["in"], ["out"])

    _make_emitter(ol_cfg, tmp_path).emit_batch([event], force=True)

    emitter2 = _make_emitter(ol_cfg, tmp_path)
    emitted, skipped, removed = emitter2.emit_batch([event])
    assert emitted == 0 and skipped == 1


def test_corrupt_state_db_starts_fresh(ol_cfg, tmp_path) -> None:
    db_path = tmp_path / "state.db"
    db_path.write_bytes(b"not a sqlite database !!!!")

    store = StateStore(db_path)
    emitter = LineageEmitter(ol_cfg, state_store=store, max_workers=1)
    assert emitter._known_jobs == {}


# ── Parallel emission ──────────────────────────────────────────────────────────

def test_parallel_emission_all_events_sent(ol_cfg, tmp_path) -> None:
    """With max_workers > 1 all events are still emitted exactly once."""
    emitter = _make_emitter(ol_cfg, tmp_path, max_workers=4)
    events = [_make_event(f"job-{i}", ["in"], ["out"]) for i in range(20)]

    with patch.object(emitter._client, "emit") as mock_emit:
        emitted, skipped, removed = emitter.emit_batch(events, force=True)

    assert emitted == 20
    assert mock_emit.call_count == 20


def test_parallel_emission_skips_unchanged(ol_cfg, tmp_path) -> None:
    """Unchanged events are correctly skipped even under concurrent workers."""
    emitter = _make_emitter(ol_cfg, tmp_path, max_workers=4)
    events = [_make_event(f"job-{i}", ["in"], ["out"]) for i in range(10)]

    emitter.emit_batch(events, force=True)
    emitted, skipped, removed = emitter.emit_batch(events)

    assert emitted == 0 and skipped == 10 and removed == 0


# ── Reset state ────────────────────────────────────────────────────────────────

def test_reset_state_forces_re_emit(ol_cfg, tmp_path) -> None:
    emitter = _make_emitter(ol_cfg, tmp_path)
    event = _make_event("job-r", ["in"], ["out"])
    emitter.emit(event, force=True)
    emitter.reset_state()
    assert emitter.emit(event) is True


def test_reset_state_clears_known_jobs(ol_cfg, tmp_path) -> None:
    emitter = _make_emitter(ol_cfg, tmp_path)
    event_a = _make_event("job-a", ["in"], ["out"])
    event_b = _make_event("job-b", ["in"], ["out"])

    emitter.emit_batch([event_a, event_b], force=True)
    emitter.reset_state()

    _, _, removed = emitter.emit_batch([event_a], force=True)
    assert removed == 0


def test_reset_state_persisted_to_db(ol_cfg, tmp_path) -> None:
    """After reset, a new emitter from the same store has no known jobs."""
    emitter = _make_emitter(ol_cfg, tmp_path)
    emitter.emit_batch([_make_event("job-a", ["in"], ["out"])], force=True)
    emitter.reset_state()

    emitter2 = _make_emitter(ol_cfg, tmp_path)
    _, _, removed = emitter2.emit_batch([])
    assert removed == 0
    assert emitter2._known_jobs == {}

"""Tests for failed_namespaces — preserves prior jobs across transient fetch failures.

The user-visible bug this fixes: a 401 on the Connect / Metrics API returned an
empty list, the diff-tracker saw "no edges from this source" and synthesized
ABORT events for jobs that were really still there. With failed_namespaces, the
fetch failure quarantines those jobs in _known_jobs so they wait for the next
successful poll instead of being falsely removed.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest
from openlineage.client.event_v2 import Dataset, Job, Run, RunEvent, RunState

from openlineage_confluent.config import (
    ConfluentConfig,
    OpenLineageConfig,
)
from openlineage_confluent.confluent.metrics_client import MetricsApiClient
from openlineage_confluent.emitter.emitter import LineageEmitter
from openlineage_confluent.emitter.state_store import StateStore

# ── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture()
def metrics_client() -> MetricsApiClient:
    cfg = ConfluentConfig(
        CONFLUENT_CLOUD_API_KEY="test", CONFLUENT_CLOUD_API_SECRET="test",
    )
    return MetricsApiClient(cfg, cluster_id="lkc-test")


@pytest.fixture()
def emitter(tmp_path) -> LineageEmitter:
    cfg = OpenLineageConfig(OPENLINEAGE_TRANSPORT="console")
    store = StateStore(tmp_path / "state.db")
    em = LineageEmitter(cfg, state_store=store)
    em._client = MagicMock()    # capture .emit() calls without serialising
    return em


def _make_event(namespace: str, name: str, output_topic: str = "t") -> RunEvent:
    return RunEvent(
        eventType=RunState.RUNNING,
        eventTime=datetime.now(UTC).isoformat(),
        job=Job(namespace=namespace, name=name),
        run=Run(runId="00000000-0000-0000-0000-000000000000"),
        producer="test/1.0",
        inputs=[],
        outputs=[Dataset(namespace="kafka://b", name=output_topic)],
    )


# ── MetricsApiClient ok-flag tests ──────────────────────────────────────────

def test_consumer_groups_ok_starts_true(metrics_client) -> None:
    assert metrics_client.consumer_groups_ok is True
    assert metrics_client.producers_ok is True
    assert metrics_client.throughput_ok is True


def test_consumer_groups_ok_false_on_http_error(metrics_client) -> None:
    with patch.object(
        metrics_client._http, "post",
        side_effect=httpx.HTTPError("401 Unauthorized"),
    ):
        result = metrics_client.get_consumer_groups()

    assert result == []
    assert metrics_client.consumer_groups_ok is False


def test_producers_ok_false_on_http_error(metrics_client) -> None:
    with patch.object(
        metrics_client._http, "post",
        side_effect=httpx.HTTPError("401 Unauthorized"),
    ):
        result = metrics_client.get_producers()

    assert result == []
    assert metrics_client.producers_ok is False


def test_throughput_ok_false_when_any_metric_fails(metrics_client) -> None:
    with patch.object(
        metrics_client._http, "post",
        side_effect=httpx.HTTPError("500 Server Error"),
    ):
        result = metrics_client.get_topic_throughput()

    assert result == {}
    assert metrics_client.throughput_ok is False


def test_consumer_groups_ok_true_on_success(metrics_client) -> None:
    page = MagicMock()
    page.json.return_value = {"data": [], "meta": {"pagination": {}}}
    page.raise_for_status.return_value = None
    with patch.object(metrics_client._http, "post", return_value=page):
        result = metrics_client.get_consumer_groups()

    assert result == []                                # zero rows is authoritative
    assert metrics_client.consumer_groups_ok is True   # but ok stays True


# ── Emitter quarantine tests ────────────────────────────────────────────────

def test_emitter_quarantines_jobs_in_failed_namespaces(emitter) -> None:
    """A known job in a failed namespace is NOT ABORT'd; stays in _known_jobs."""
    # Cycle 1: emit a Connect job + a Flink job. Both get tracked.
    cycle1 = [
        _make_event("kafka-connect://env-aaa", "orders-source"),
        _make_event("flink://env-aaa", "enrich-orders"),
    ]
    emitter.emit_batch(cycle1)
    assert "kafka-connect://env-aaa/orders-source" in emitter._known_jobs
    assert "flink://env-aaa/enrich-orders" in emitter._known_jobs

    # Cycle 2: Connect API returns 401 (so no Connect events come through),
    # Flink still fetches successfully. failed_namespaces flags Connect.
    cycle2_events: list[RunEvent] = [
        _make_event("flink://env-aaa", "enrich-orders"),
    ]
    emitted, skipped, removed = emitter.emit_batch(
        cycle2_events,
        failed_namespaces={"kafka-connect://env-aaa"},
    )

    # The Connect job should NOT be removed — it's quarantined.
    assert removed == 0
    assert "kafka-connect://env-aaa/orders-source" in emitter._known_jobs
    assert "flink://env-aaa/enrich-orders" in emitter._known_jobs


def test_emitter_resumes_normal_diff_after_quarantine(emitter) -> None:
    """After the source recovers, removal-detection works normally again."""
    # Cycle 1: track a Connect job
    emitter.emit_batch([_make_event("kafka-connect://env-aaa", "orders-source")])
    assert "kafka-connect://env-aaa/orders-source" in emitter._known_jobs

    # Cycle 2: Connect 401 → quarantined
    emitter.emit_batch([], failed_namespaces={"kafka-connect://env-aaa"})
    assert "kafka-connect://env-aaa/orders-source" in emitter._known_jobs

    # Cycle 3: Connect recovers, returns NO connectors → genuine removal.
    _, _, removed = emitter.emit_batch([], failed_namespaces=set())
    assert removed == 1
    assert "kafka-connect://env-aaa/orders-source" not in emitter._known_jobs


def test_emitter_only_quarantines_matching_namespaces(emitter) -> None:
    """Failed namespace A must not protect jobs in unrelated namespace B."""
    emitter.emit_batch([
        _make_event("kafka-connect://env-aaa", "orders-source"),
        _make_event("flink://env-aaa", "enrich-orders"),
    ])

    # Only Connect failed; Flink succeeded but reports zero statements.
    _, _, removed = emitter.emit_batch(
        [],
        failed_namespaces={"kafka-connect://env-aaa"},
    )
    # Connect job: quarantined. Flink job: removed.
    assert removed == 1
    assert "kafka-connect://env-aaa/orders-source" in emitter._known_jobs
    assert "flink://env-aaa/enrich-orders" not in emitter._known_jobs


def test_emitter_default_failed_namespaces_is_empty(emitter) -> None:
    """Calling emit_batch without failed_namespaces still removes missing jobs."""
    emitter.emit_batch([_make_event("flink://env-aaa", "j")])
    assert "flink://env-aaa/j" in emitter._known_jobs

    _, _, removed = emitter.emit_batch([])    # no failed_namespaces kwarg
    assert removed == 1
    assert "flink://env-aaa/j" not in emitter._known_jobs

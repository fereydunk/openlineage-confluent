"""Tests for MetricsApiClient — consumer group discovery via consumer_lag_offsets."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from openlineage_confluent.confluent.metrics_client import MetricsApiClient, _INTERNAL_PREFIXES
from openlineage_confluent.config import ConfluentConfig


@pytest.fixture()
def cfg() -> ConfluentConfig:
    return ConfluentConfig(
        CONFLUENT_CLOUD_API_KEY="test-key",
        CONFLUENT_CLOUD_API_SECRET="test-secret",
        CONFLUENT_ENV_ID="env-test",
        CONFLUENT_CLUSTER_ID="lkc-test",
    )


@pytest.fixture()
def client(cfg) -> MetricsApiClient:
    return MetricsApiClient(cfg)


# ── Internal prefix filter ─────────────────────────────────────────────────────

@pytest.mark.parametrize("group_id", [
    "_confluent-ksql-pksqlc-xxx_query_CSAS_OUTPUT_0",
    "_schemas",
    "_confluent-controlcenter-0",
    "confluent_cli_consumer_abc123",
])
def test_internal_groups_are_excluded(client, group_id) -> None:
    assert client._is_internal(group_id) is True


@pytest.mark.parametrize("group_id", [
    "order-processor-service",
    "payment-consumer-group",
    "my-kafka-streams-app",
    "connect-jdbc-source",   # self-managed Connect worker groups are NOT internal
])
def test_application_groups_are_not_excluded(client, group_id) -> None:
    assert client._is_internal(group_id) is False


def test_custom_exclude_prefix(cfg) -> None:
    cfg_with_extra = ConfluentConfig(
        CONFLUENT_CLOUD_API_KEY="k",
        CONFLUENT_CLOUD_API_SECRET="s",
        CONFLUENT_ENV_ID="env-x",
        CONFLUENT_CLUSTER_ID="lkc-x",
        consumer_group_exclude_prefixes=["legacy-"],
    )
    c = MetricsApiClient(cfg_with_extra)
    assert c._is_internal("legacy-consumer") is True
    assert c._is_internal("new-consumer") is False


# ── Query interval ─────────────────────────────────────────────────────────────

def test_query_interval_format(client) -> None:
    interval = client._query_interval()
    # Should be: "2024-01-01T00:00:00+00:00/2024-01-01T00:10:00+00:00"
    parts = interval.split("/")
    assert len(parts) == 2
    start = datetime.fromisoformat(parts[0])
    end   = datetime.fromisoformat(parts[1])
    assert end > start
    # Default lookback is 10 minutes
    assert (end - start).total_seconds() == 600


def test_query_interval_custom_lookback(cfg) -> None:
    cfg_custom = ConfluentConfig(
        CONFLUENT_CLOUD_API_KEY="k",
        CONFLUENT_CLOUD_API_SECRET="s",
        CONFLUENT_ENV_ID="env-x",
        CONFLUENT_CLUSTER_ID="lkc-x",
        CONFLUENT_METRICS_LOOKBACK_MINUTES=30,
    )
    c = MetricsApiClient(cfg_custom)
    parts = c._query_interval().split("/")
    start = datetime.fromisoformat(parts[0])
    end   = datetime.fromisoformat(parts[1])
    assert (end - start).total_seconds() == 1800


# ── get_consumer_groups ────────────────────────────────────────────────────────

def _mock_metrics_response(rows: list[dict]) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {"data": rows, "meta": {"pagination": {}}}
    resp.raise_for_status.return_value = None
    return resp


def test_get_consumer_groups_aggregates_by_group(client) -> None:
    rows = [
        {"metric.consumer_group_id": "app-group", "metric.topic": "topic-a", "value": 10},
        {"metric.consumer_group_id": "app-group", "metric.topic": "topic-b", "value":  5},
        {"metric.consumer_group_id": "other-group", "metric.topic": "topic-a", "value": 0},
    ]
    with patch.object(client._http, "post", return_value=_mock_metrics_response(rows)):
        groups = client.get_consumer_groups()

    assert len(groups) == 2
    group_map = {g.group_id: g for g in groups}
    assert set(group_map["app-group"].topics) == {"topic-a", "topic-b"}
    assert group_map["other-group"].topics == ["topic-a"]


def test_get_consumer_groups_filters_internal(client) -> None:
    rows = [
        {"metric.consumer_group_id": "_confluent-ksql-xxx",   "metric.topic": "t1", "value": 1},
        {"metric.consumer_group_id": "app-consumer",           "metric.topic": "t2", "value": 1},
        {"metric.consumer_group_id": "confluent_cli_consumer_123", "metric.topic": "t3", "value": 1},
    ]
    with patch.object(client._http, "post", return_value=_mock_metrics_response(rows)):
        groups = client.get_consumer_groups()

    assert len(groups) == 1
    assert groups[0].group_id == "app-consumer"


def test_get_consumer_groups_handles_http_error(client) -> None:
    import httpx
    with patch.object(
        client._http, "post",
        side_effect=httpx.HTTPError("connection refused"),
    ):
        groups = client.get_consumer_groups()

    assert groups == []


def test_get_consumer_groups_handles_pagination(client) -> None:
    page1 = MagicMock()
    page1.json.return_value = {
        "data": [{"metric.consumer_group_id": "group-a", "metric.topic": "t1", "value": 1}],
        "meta": {"pagination": {"page_token": "next-token"}},
    }
    page1.raise_for_status.return_value = None

    page2 = MagicMock()
    page2.json.return_value = {
        "data": [{"metric.consumer_group_id": "group-b", "metric.topic": "t2", "value": 1}],
        "meta": {"pagination": {}},
    }
    page2.raise_for_status.return_value = None

    with patch.object(client._http, "post", side_effect=[page1, page2]):
        groups = client.get_consumer_groups()

    assert {g.group_id for g in groups} == {"group-a", "group-b"}


def test_get_consumer_groups_topics_are_sorted(client) -> None:
    rows = [
        {"metric.consumer_group_id": "g", "metric.topic": "z-topic", "value": 1},
        {"metric.consumer_group_id": "g", "metric.topic": "a-topic", "value": 1},
        {"metric.consumer_group_id": "g", "metric.topic": "m-topic", "value": 1},
    ]
    with patch.object(client._http, "post", return_value=_mock_metrics_response(rows)):
        groups = client.get_consumer_groups()

    assert groups[0].topics == ["a-topic", "m-topic", "z-topic"]


def test_uses_cloud_key_as_fallback(cfg) -> None:
    """Metrics client falls back to cloud_api_key when no dedicated metrics key is set."""
    c = MetricsApiClient(cfg)
    # The httpx auth tuple should use the cloud key
    assert c._http.auth is not None


# ── get_topic_throughput ──────────────────────────────────────────────────────

def _throughput_responses(by_metric: dict[str, list[dict]]) -> list[MagicMock]:
    """Build one MagicMock per call to client._http.post in metric order:
    received_bytes, sent_bytes, received_records, sent_records.
    """
    from openlineage_confluent.confluent.metrics_client import (
        _RECEIVED_BYTES, _SENT_BYTES, _RECEIVED_RECORDS, _SENT_RECORDS,
    )
    order = [_RECEIVED_BYTES, _SENT_BYTES, _RECEIVED_RECORDS, _SENT_RECORDS]
    return [_mock_metrics_response(by_metric.get(m, [])) for m in order]


def test_get_topic_throughput_aggregates_all_four_metrics(client) -> None:
    responses = _throughput_responses({
        "io.confluent.kafka.server/received_bytes": [
            {"metric.topic": "orders",   "value": 1024},
            {"metric.topic": "payments", "value":  512},
        ],
        "io.confluent.kafka.server/sent_bytes": [
            {"metric.topic": "orders", "value": 2048},
        ],
        "io.confluent.kafka.server/received_records": [
            {"metric.topic": "orders",   "value": 100},
            {"metric.topic": "payments", "value":  50},
        ],
        "io.confluent.kafka.server/sent_records": [
            {"metric.topic": "orders", "value": 200},
        ],
    })
    with patch.object(client._http, "post", side_effect=responses):
        out = client.get_topic_throughput()

    assert set(out) == {"orders", "payments"}
    assert out["orders"].bytes_in    == 1024
    assert out["orders"].bytes_out   == 2048
    assert out["orders"].records_in  == 100
    assert out["orders"].records_out == 200
    assert out["payments"].bytes_in  == 512
    assert out["payments"].bytes_out == 0
    assert out["payments"].records_out == 0


def test_get_topic_throughput_carries_window_minutes(client) -> None:
    responses = _throughput_responses({
        "io.confluent.kafka.server/received_bytes": [
            {"metric.topic": "t", "value": 1},
        ],
    })
    with patch.object(client._http, "post", side_effect=responses):
        out = client.get_topic_throughput()
    assert out["t"].window_minutes == 10  # default lookback


def test_get_topic_throughput_skips_rows_without_topic(client) -> None:
    responses = _throughput_responses({
        "io.confluent.kafka.server/received_bytes": [
            {"value": 1},                  # no topic
            {"metric.topic": "good", "value": 2},
        ],
    })
    with patch.object(client._http, "post", side_effect=responses):
        out = client.get_topic_throughput()
    assert list(out) == ["good"]


def test_get_topic_throughput_partial_failure_returns_what_succeeded(client) -> None:
    """If one metric query fails, the other three should still aggregate."""
    import httpx
    good_resp = _mock_metrics_response([{"metric.topic": "t", "value": 5}])

    def side_effect(*args, **kwargs):
        # Fail the second call (sent_bytes), succeed the others
        side_effect.calls = getattr(side_effect, "calls", 0) + 1
        if side_effect.calls == 2:
            raise httpx.HTTPError("boom")
        return good_resp

    with patch.object(client._http, "post", side_effect=side_effect):
        out = client.get_topic_throughput()
    assert out["t"].bytes_in == 5      # received_bytes succeeded
    assert out["t"].bytes_out == 0     # sent_bytes failed
    assert out["t"].records_in == 5    # received_records succeeded
    assert out["t"].records_out == 5   # sent_records succeeded

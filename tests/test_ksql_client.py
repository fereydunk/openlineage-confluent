"""Tests for KsqlDbClient — persistent query lineage from ksqlDB REST API."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from openlineage_confluent.config import KsqlClusterConfig
from openlineage_confluent.confluent.ksql_client import KsqlDbClient


@pytest.fixture()
def cluster_cfg() -> KsqlClusterConfig:
    return KsqlClusterConfig(
        cluster_id="lksqlc-test123",
        rest_endpoint="https://pksqlc-test.us-east-2.aws.confluent.cloud",
        api_key="ksql-key",
        api_secret="ksql-secret",
    )


@pytest.fixture()
def client(cluster_cfg) -> KsqlDbClient:
    return KsqlDbClient(cluster_cfg)


# ── _run_ksql ──────────────────────────────────────────────────────────────────

def test_run_ksql_returns_parsed_json(client) -> None:
    resp = MagicMock()
    resp.json.return_value = [{"@type": "queries", "queries": []}]
    resp.raise_for_status.return_value = None

    with patch.object(client._http, "post", return_value=resp) as mock_post:
        result = client._run_ksql("SHOW QUERIES EXTENDED;")

    mock_post.assert_called_once_with("/ksql", json={"ksql": "SHOW QUERIES EXTENDED;"})
    assert result == [{"@type": "queries", "queries": []}]


def test_run_ksql_returns_empty_on_error(client) -> None:
    import httpx
    with patch.object(client._http, "post", side_effect=httpx.HTTPError("500")):
        result = client._run_ksql("SHOW QUERIES EXTENDED;")
    assert result == []


# ── _build_topic_map ───────────────────────────────────────────────────────────

def _streams_response(entries: list[dict]) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = [{"sourceDescriptions": entries}]
    resp.raise_for_status.return_value = None
    return resp


def test_build_topic_map_resolves_streams(client) -> None:
    streams_resp = _streams_response([
        {"name": "ORDERS_ENRICHED", "topic": "ol-orders-enriched"},
        {"name": "RAW_ORDERS",      "topic": "ol-raw-orders"},
    ])
    tables_resp = _streams_response([])  # no tables

    with patch.object(client._http, "post", side_effect=[streams_resp, tables_resp]):
        topic_map = client._build_topic_map()

    assert topic_map["ORDERS_ENRICHED"] == "ol-orders-enriched"
    assert topic_map["RAW_ORDERS"] == "ol-raw-orders"


def test_build_topic_map_normalises_to_upper(client) -> None:
    """Stream names in the map are stored in upper-case for case-insensitive lookup."""
    resp = _streams_response([{"name": "my_stream", "topic": "my-topic"}])
    empty = _streams_response([])
    with patch.object(client._http, "post", side_effect=[resp, empty]):
        topic_map = client._build_topic_map()
    assert "MY_STREAM" in topic_map


def test_build_topic_map_handles_error(client) -> None:
    import httpx
    with patch.object(client._http, "post", side_effect=httpx.HTTPError("503")):
        topic_map = client._build_topic_map()
    assert topic_map == {}


# ── get_queries ────────────────────────────────────────────────────────────────

def _queries_response(queries: list[dict]) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = [{"queries": queries}]
    resp.raise_for_status.return_value = None
    return resp


def test_get_queries_resolves_sources_to_topics(client) -> None:
    streams_resp = _streams_response([
        {"name": "ORDERS_ENRICHED", "topic": "ol-orders-enriched"},
    ])
    tables_resp = _streams_response([])
    queries_resp = _queries_response([{
        "id": {"queryId": "CSAS_HIGH_VALUE_0"},
        "state": "RUNNING",
        "queryString": "CREATE STREAM HIGH_VALUE AS SELECT ...",
        "sinkKafkaTopics": ["high-value-stream"],
        "sources": ["ORDERS_ENRICHED"],
    }])

    with patch.object(client._http, "post", side_effect=[streams_resp, tables_resp, queries_resp]):
        queries = client.get_queries()

    assert len(queries) == 1
    q = queries[0]
    assert q.query_id == "CSAS_HIGH_VALUE_0"
    assert q.state == "RUNNING"
    assert q.source_topics == ["ol-orders-enriched"]
    assert q.sink_topics == ["high-value-stream"]
    assert q.ksql_cluster_id == "lksqlc-test123"


def test_get_queries_falls_back_to_stream_name_when_not_in_map(client) -> None:
    """If a source stream isn't in the topic map, use the stream name as-is."""
    streams_resp = _streams_response([])  # empty — nothing resolved
    tables_resp  = _streams_response([])
    queries_resp = _queries_response([{
        "id": {"queryId": "CSAS_X_0"},
        "state": "RUNNING",
        "queryString": "...",
        "sinkKafkaTopics": ["out"],
        "sources": ["UNKNOWN_STREAM"],
    }])

    with patch.object(client._http, "post", side_effect=[streams_resp, tables_resp, queries_resp]):
        queries = client.get_queries()

    assert queries[0].source_topics == ["UNKNOWN_STREAM"]


def test_get_queries_removes_self_references(client) -> None:
    """Output topic should not also appear as a source topic."""
    streams_resp = _streams_response([
        {"name": "OUTPUT", "topic": "output-topic"},
    ])
    tables_resp = _streams_response([])
    queries_resp = _queries_response([{
        "id": {"queryId": "CSAS_SELF_0"},
        "state": "RUNNING",
        "queryString": "...",
        "sinkKafkaTopics": ["output-topic"],
        "sources": ["OUTPUT"],   # resolves to output-topic → should be excluded
    }])

    with patch.object(client._http, "post", side_effect=[streams_resp, tables_resp, queries_resp]):
        queries = client.get_queries()

    assert queries[0].source_topics == []


def test_get_queries_skips_missing_query_id(client) -> None:
    streams_resp = _streams_response([])
    tables_resp  = _streams_response([])
    queries_resp = _queries_response([
        {"id": {}, "state": "RUNNING", "queryString": "...",
         "sinkKafkaTopics": ["out"], "sources": ["src"]},
    ])

    with patch.object(client._http, "post", side_effect=[streams_resp, tables_resp, queries_resp]):
        queries = client.get_queries()

    assert queries == []


def test_get_queries_returns_multiple_queries(client) -> None:
    streams_resp = _streams_response([
        {"name": "SRC_A", "topic": "topic-a"},
        {"name": "SRC_B", "topic": "topic-b"},
    ])
    tables_resp = _streams_response([])
    queries_resp = _queries_response([
        {"id": {"queryId": "Q1"}, "state": "RUNNING", "queryString": "...",
         "sinkKafkaTopics": ["sink-1"], "sources": ["SRC_A"]},
        {"id": {"queryId": "Q2"}, "state": "PAUSED",  "queryString": "...",
         "sinkKafkaTopics": ["sink-2"], "sources": ["SRC_B"]},
    ])

    with patch.object(client._http, "post", side_effect=[streams_resp, tables_resp, queries_resp]):
        queries = client.get_queries()

    assert len(queries) == 2
    q_map = {q.query_id: q for q in queries}
    assert q_map["Q1"].is_running() is True
    assert q_map["Q2"].is_running() is False

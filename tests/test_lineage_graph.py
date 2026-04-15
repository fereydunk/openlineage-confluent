"""Tests for SQL parser, connector topic extraction, and LineageGraph construction."""

from __future__ import annotations

import pytest

from openlineage_confluent.confluent.models import (
    ConnectorInfo,
    ConsumerGroupInfo,
    FlinkStatement,
    KsqlQuery,
    LineageGraph,
)
from openlineage_confluent.confluent.sql_parser import (
    extract_input_tables,
    extract_output_tables,
    parse_statement,
)


# ── SQL parser ─────────────────────────────────────────────────────────────────

def test_simple_insert_select() -> None:
    sql = "INSERT INTO `ol-orders-enriched` SELECT orderid FROM `ol-raw-orders`;"
    inputs, outputs = parse_statement(sql)
    assert inputs == ["ol-raw-orders"]
    assert outputs == ["ol-orders-enriched"]


def test_insert_with_join() -> None:
    sql = """
    INSERT INTO `ol-orders-enriched`
    SELECT o.orderid, c.name
    FROM `ol-raw-orders` o
    JOIN `customers` c ON o.customer_id = c.id;
    """
    inputs, outputs = parse_statement(sql)
    assert "ol-raw-orders" in inputs
    assert "customers" in inputs
    assert "ol-orders-enriched" in outputs


def test_where_filter_statement() -> None:
    sql = "INSERT INTO `ol-high-value-alerts` SELECT * FROM `ol-orders-enriched` WHERE risk_tier = 'HIGH';"
    inputs, outputs = parse_statement(sql)
    assert inputs == ["ol-orders-enriched"]
    assert outputs == ["ol-high-value-alerts"]


def test_comments_stripped() -> None:
    sql = """
    -- This inserts enriched records
    /* Multi-line comment */
    INSERT INTO `sink-topic`
    SELECT id FROM `source-topic`;
    """
    inputs, outputs = parse_statement(sql)
    assert "source-topic" in inputs
    assert "sink-topic" in outputs


def test_output_not_in_inputs() -> None:
    sql = "INSERT INTO `t` SELECT * FROM `t`;"
    inputs, outputs = parse_statement(sql)
    assert "t" in outputs
    assert "t" not in inputs


def test_extract_output_tables_create_table() -> None:
    sql = "CREATE TABLE IF NOT EXISTS `my_table` AS SELECT * FROM `src`;"
    outputs = extract_output_tables(sql)
    assert "my_table" in outputs


# ── Connector topic extraction ─────────────────────────────────────────────────

def test_source_connector_produces_topic(sample_source_connector) -> None:
    assert sample_source_connector.topics_produced() == ["ol-raw-orders"]


def test_source_connector_has_no_consumed_topics(sample_source_connector) -> None:
    assert sample_source_connector.topics_consumed() == []


def test_sink_connector_consumes_topic(sample_sink_connector) -> None:
    assert sample_sink_connector.topics_consumed() == ["ol-orders-enriched"]


def test_sink_connector_has_no_produced_topics(sample_sink_connector) -> None:
    assert sample_sink_connector.topics_produced() == []


def test_multi_topic_sink() -> None:
    conn = ConnectorInfo(
        id="c1",
        name="multi-sink",
        status="RUNNING",
        type="sink",
        config={"topics": "topic-a, topic-b, topic-c"},
    )
    assert conn.topics_consumed() == ["topic-a", "topic-b", "topic-c"]


def test_connector_status_accepts_non_standard_values() -> None:
    """Self-managed clusters may return statuses not in the original StrEnum."""
    conn = ConnectorInfo(
        id="sm:jdbc-source",
        name="jdbc-source",
        status="UNASSIGNED",   # not a standard Confluent Cloud status
        type="source",
        config={"kafka.topic": "t"},
        connect_cluster="on-prem",
    )
    assert conn.status == "UNASSIGNED"
    assert conn.connect_cluster == "on-prem"


def test_self_managed_connector_identifies_cluster(sample_self_managed_connector) -> None:
    assert sample_self_managed_connector.connect_cluster == "on-prem-connect"
    assert sample_self_managed_connector.topics_produced() == ["jdbc-orders"]


# ── ConsumerGroupInfo ──────────────────────────────────────────────────────────

def test_consumer_group_stores_topics(sample_consumer_group) -> None:
    assert sample_consumer_group.group_id == "order-processor-service"
    assert "ol-orders-enriched" in sample_consumer_group.topics
    assert "ol-raw-orders" in sample_consumer_group.topics


def test_consumer_group_empty_topics() -> None:
    group = ConsumerGroupInfo(group_id="idle-group")
    assert group.topics == []


# ── KsqlQuery ─────────────────────────────────────────────────────────────────

def test_ksql_query_is_running(sample_ksql_query) -> None:
    assert sample_ksql_query.is_running() is True


def test_ksql_query_not_running() -> None:
    q = KsqlQuery(
        query_id="CSAS_X",
        sql="...",
        state="PAUSED",
        sink_topics=["out"],
        source_topics=["in"],
        ksql_cluster_id="lksqlc-test",
    )
    assert q.is_running() is False


def test_ksql_query_topic_bindings(sample_ksql_query) -> None:
    assert sample_ksql_query.source_topics == ["ol-orders-enriched"]
    assert sample_ksql_query.sink_topics == ["HIGH_VALUE_STREAM"]


# ── LineageGraph ───────────────────────────────────────────────────────────────

def test_lineage_graph_summary(sample_lineage_graph: LineageGraph) -> None:
    s = sample_lineage_graph.summary()
    assert s["managed_connectors"] == 2        # postgres-orders-source + http-sink
    assert s["self_managed_connectors"] == 1   # jdbc-source
    assert s["flink_statements"] == 1
    assert s["consumer_groups"] == 1
    assert s["ksql_queries"] == 1
    assert s["edges"] == 7


def test_lineage_graph_all_job_types_present(sample_lineage_graph: LineageGraph) -> None:
    types = {e.job_type for e in sample_lineage_graph.edges}
    assert types == {
        "kafka_connect_source",
        "kafka_connect_sink",
        "flink_statement",
        "kafka_consumer_group",
        "ksqldb_query",
    }


def test_consumer_group_edge_target_type(sample_lineage_graph: LineageGraph) -> None:
    cg_edges = [e for e in sample_lineage_graph.edges if e.job_type == "kafka_consumer_group"]
    assert len(cg_edges) == 2
    for edge in cg_edges:
        assert edge.source_type == "kafka_topic"
        assert edge.target_type == "consumer_group"


def test_ksqldb_edge_both_kafka_topics(sample_lineage_graph: LineageGraph) -> None:
    ksql_edges = [e for e in sample_lineage_graph.edges if e.job_type == "ksqldb_query"]
    assert len(ksql_edges) == 1
    edge = ksql_edges[0]
    assert edge.source_type == "kafka_topic"
    assert edge.target_type == "kafka_topic"
    assert edge.source_name == "ol-orders-enriched"
    assert edge.target_name == "HIGH_VALUE_STREAM"


def test_self_managed_connector_edge_namespace(sample_lineage_graph: LineageGraph) -> None:
    sm_edges = [
        e for e in sample_lineage_graph.edges
        if e.job_namespace_hint.startswith("kafka-connect://on-prem")
    ]
    assert len(sm_edges) == 1
    assert sm_edges[0].job_name == "jdbc-source"

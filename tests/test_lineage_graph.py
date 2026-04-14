"""Tests for SQL parser, connector topic extraction, and LineageGraph construction."""

from __future__ import annotations

import pytest

from openlineage_confluent.confluent.models import ConnectorInfo, FlinkStatement, LineageGraph
from openlineage_confluent.confluent.sql_parser import parse_statement, extract_input_tables, extract_output_tables


# ---------------------------------------------------------------------------
# SQL parser
# ---------------------------------------------------------------------------

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
    # The target of INSERT INTO must not appear as an input
    sql = "INSERT INTO `t` SELECT * FROM `t`;"
    inputs, outputs = parse_statement(sql)
    assert "t" in outputs
    assert "t" not in inputs


def test_extract_output_tables_create_table() -> None:
    sql = "CREATE TABLE IF NOT EXISTS `my_table` AS SELECT * FROM `src`;"
    outputs = extract_output_tables(sql)
    assert "my_table" in outputs


# ---------------------------------------------------------------------------
# Connector topic extraction
# ---------------------------------------------------------------------------

def test_source_connector_produces_topic(sample_source_connector) -> None:
    produced = sample_source_connector.topics_produced()
    assert produced == ["ol-raw-orders"]


def test_source_connector_has_no_consumed_topics(sample_source_connector) -> None:
    assert sample_source_connector.topics_consumed() == []


def test_sink_connector_consumes_topic(sample_sink_connector) -> None:
    consumed = sample_sink_connector.topics_consumed()
    assert consumed == ["ol-orders-enriched"]


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


# ---------------------------------------------------------------------------
# LineageGraph
# ---------------------------------------------------------------------------

def test_lineage_graph_summary(sample_lineage_graph: LineageGraph) -> None:
    s = sample_lineage_graph.summary()
    assert s["connectors"] == 2
    assert s["flink_statements"] == 1
    assert s["edges"] == 3


def test_lineage_graph_edge_types(sample_lineage_graph: LineageGraph) -> None:
    types = {e.job_type for e in sample_lineage_graph.edges}
    assert "kafka_connect_source" in types
    assert "kafka_connect_sink" in types
    assert "flink_statement" in types


def test_flink_edge_connects_topics(sample_lineage_graph: LineageGraph) -> None:
    flink_edges = [e for e in sample_lineage_graph.edges if e.job_type == "flink_statement"]
    assert len(flink_edges) == 1
    edge = flink_edges[0]
    assert edge.source_name == "ol-raw-orders"
    assert edge.target_name == "ol-orders-enriched"

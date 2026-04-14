"""Shared test fixtures."""

from __future__ import annotations

import pytest

from openlineage_confluent.confluent.models import (
    ConnectorInfo,
    ConnectorType,
    FlinkStatement,
    LineageEdge,
    LineageGraph,
)
from openlineage_confluent.config import ConfluentConfig, OpenLineageConfig


@pytest.fixture()
def confluent_cfg() -> ConfluentConfig:
    return ConfluentConfig(
        CONFLUENT_CLOUD_API_KEY="test-cloud-key",
        CONFLUENT_CLOUD_API_SECRET="test-cloud-secret",
        CONFLUENT_ENV_ID="env-abc123",
        CONFLUENT_CLUSTER_ID="lkc-abc123",
    )


@pytest.fixture()
def ol_cfg() -> OpenLineageConfig:
    return OpenLineageConfig(
        OPENLINEAGE_TRANSPORT="console",
        OPENLINEAGE_URL="http://localhost:5000",
        OPENLINEAGE_KAFKA_BOOTSTRAP="pkc-abc123.us-east-2.aws.confluent.cloud:9092",
    )


@pytest.fixture()
def sample_source_connector() -> ConnectorInfo:
    return ConnectorInfo(
        id="lkc-abc123:postgres-orders-source",
        name="postgres-orders-source",
        status="RUNNING",
        type="source",
        config={
            "connector.class": "DatagenSource",
            "kafka.topic": "ol-raw-orders",
        },
    )


@pytest.fixture()
def sample_sink_connector() -> ConnectorInfo:
    return ConnectorInfo(
        id="lkc-abc123:http-sink",
        name="http-sink",
        status="RUNNING",
        type="sink",
        config={
            "connector.class": "HttpSink",
            "topics": "ol-orders-enriched",
        },
    )


@pytest.fixture()
def sample_flink_statement() -> FlinkStatement:
    return FlinkStatement(
        name="ol-enrich-orders",
        statement="INSERT INTO `ol-orders-enriched` SELECT orderid, itemid FROM `ol-raw-orders`;",
        status="RUNNING",
        compute_pool="lfcp-test",
    )


@pytest.fixture()
def sample_lineage_graph(
    sample_source_connector,
    sample_sink_connector,
    sample_flink_statement,
) -> LineageGraph:
    edges = [
        # Source connector: external → ol-raw-orders
        LineageEdge(
            source_name="postgres-orders-source",
            source_type="external",
            target_name="ol-raw-orders",
            target_type="kafka_topic",
            job_name="postgres-orders-source",
            job_type="kafka_connect_source",
            job_namespace_hint="kafka-connect://env-abc123",
        ),
        # Flink: ol-raw-orders → ol-orders-enriched
        LineageEdge(
            source_name="ol-raw-orders",
            source_type="kafka_topic",
            target_name="ol-orders-enriched",
            target_type="kafka_topic",
            job_name="ol-enrich-orders",
            job_type="flink_statement",
            job_namespace_hint="flink://env-abc123",
        ),
        # Sink connector: ol-orders-enriched → external
        LineageEdge(
            source_name="ol-orders-enriched",
            source_type="kafka_topic",
            target_name="http-sink",
            target_type="external",
            job_name="http-sink",
            job_type="kafka_connect_sink",
            job_namespace_hint="kafka-connect://env-abc123",
        ),
    ]
    return LineageGraph(
        edges=edges,
        connectors=[sample_source_connector, sample_sink_connector],
        statements=[sample_flink_statement],
    )

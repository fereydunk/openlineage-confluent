"""Shared test fixtures."""

from __future__ import annotations

import pytest

from openlineage_confluent.config import ConfluentConfig, EnvDeployment, OpenLineageConfig
from openlineage_confluent.confluent.models import (
    ConnectorInfo,
    ConsumerGroupInfo,
    FlinkStatement,
    KafkaProducerInfo,
    KsqlQuery,
    LineageEdge,
    LineageGraph,
)

# ── Config fixtures ────────────────────────────────────────────────────────────

def _make_env(env_id: str = "env-abc123", cluster_id: str = "lkc-abc123",
              bootstrap: str = "pkc-abc123.us-east-2.aws.confluent.cloud:9092",
              env_name: str = "test-env", cluster_name: str = "cluster_0") -> EnvDeployment:
    return EnvDeployment(
        env_id=env_id, env_name=env_name,
        cluster_id=cluster_id, cluster_name=cluster_name,
        kafka_bootstrap=bootstrap,
    )


@pytest.fixture()
def confluent_cfg() -> ConfluentConfig:
    return ConfluentConfig(
        CONFLUENT_CLOUD_API_KEY="test-cloud-key",
        CONFLUENT_CLOUD_API_SECRET="test-cloud-secret",
        environments=[_make_env()],
    )


@pytest.fixture()
def multi_env_cfg() -> ConfluentConfig:
    """Config with two environments, used by multi-env tests."""
    return ConfluentConfig(
        CONFLUENT_CLOUD_API_KEY="test-cloud-key",
        CONFLUENT_CLOUD_API_SECRET="test-cloud-secret",
        environments=[
            _make_env("env-aaa", "lkc-aaa", "pkc-aaa.us-west-2.aws.confluent.cloud:9092"),
            _make_env("env-bbb", "lkc-bbb", "pkc-bbb.eu-central-1.aws.confluent.cloud:9092"),
        ],
    )


@pytest.fixture()
def ol_cfg() -> OpenLineageConfig:
    return OpenLineageConfig(
        OPENLINEAGE_TRANSPORT="console",
        OPENLINEAGE_URL="http://localhost:5000",
        OPENLINEAGE_KAFKA_BOOTSTRAP="pkc-abc123.us-east-2.aws.confluent.cloud:9092",
    )


# ── Connector fixtures ─────────────────────────────────────────────────────────

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
def sample_self_managed_connector() -> ConnectorInfo:
    """A connector from a self-managed Connect cluster."""
    return ConnectorInfo(
        id="on-prem-connect:jdbc-source",
        name="jdbc-source",
        status="RUNNING",
        type="source",
        config={
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "kafka.topic": "jdbc-orders",
        },
        connect_cluster="on-prem-connect",
    )


# ── Flink fixture ──────────────────────────────────────────────────────────────

@pytest.fixture()
def sample_flink_statement() -> FlinkStatement:
    return FlinkStatement(
        name="ol-enrich-orders",
        statement="INSERT INTO `ol-orders-enriched` SELECT orderid, itemid FROM `ol-raw-orders`;",
        status="RUNNING",
        compute_pool="lfcp-test",
    )


# ── Kafka producer fixture ─────────────────────────────────────────────────────

@pytest.fixture()
def sample_producer() -> KafkaProducerInfo:
    return KafkaProducerInfo(
        client_id="order-service-producer",
        topics=["ol-raw-orders"],
    )


# ── Consumer group fixture ─────────────────────────────────────────────────────

@pytest.fixture()
def sample_consumer_group() -> ConsumerGroupInfo:
    return ConsumerGroupInfo(
        group_id="order-processor-service",
        topics=["ol-orders-enriched", "ol-raw-orders"],
    )


# ── ksqlDB fixture ─────────────────────────────────────────────────────────────

@pytest.fixture()
def sample_ksql_query() -> KsqlQuery:
    return KsqlQuery(
        query_id="CSAS_HIGH_VALUE_STREAM_0",
        sql="CREATE STREAM HIGH_VALUE_STREAM AS SELECT * FROM ORDERS_ENRICHED WHERE amount > 1000;",
        state="RUNNING",
        sink_topics=["HIGH_VALUE_STREAM"],
        source_topics=["ol-orders-enriched"],
        ksql_cluster_id="lksqlc-test123",
    )


# ── Composite LineageGraph fixture ─────────────────────────────────────────────

@pytest.fixture()
def sample_lineage_graph(
    sample_source_connector,
    sample_sink_connector,
    sample_flink_statement,
    sample_producer,
    sample_consumer_group,
    sample_ksql_query,
    sample_self_managed_connector,
) -> LineageGraph:
    edges = [
        # Managed source: external → ol-raw-orders
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
        # Managed sink: ol-orders-enriched → external
        LineageEdge(
            source_name="ol-orders-enriched",
            source_type="kafka_topic",
            target_name="http-sink",
            target_type="external",
            job_name="http-sink",
            job_type="kafka_connect_sink",
            job_namespace_hint="kafka-connect://env-abc123",
        ),
        # Consumer group: ol-orders-enriched → group
        LineageEdge(
            source_name="ol-orders-enriched",
            source_type="kafka_topic",
            target_name="order-processor-service",
            target_type="consumer_group",
            job_name="order-processor-service",
            job_type="kafka_consumer_group",
            job_namespace_hint="kafka-consumer-group://lkc-abc123",
        ),
        # Consumer group: ol-raw-orders → same group
        LineageEdge(
            source_name="ol-raw-orders",
            source_type="kafka_topic",
            target_name="order-processor-service",
            target_type="consumer_group",
            job_name="order-processor-service",
            job_type="kafka_consumer_group",
            job_namespace_hint="kafka-consumer-group://lkc-abc123",
        ),
        # ksqlDB query: ol-orders-enriched → HIGH_VALUE_STREAM
        LineageEdge(
            source_name="ol-orders-enriched",
            source_type="kafka_topic",
            target_name="HIGH_VALUE_STREAM",
            target_type="kafka_topic",
            job_name="CSAS_HIGH_VALUE_STREAM_0",
            job_type="ksqldb_query",
            job_namespace_hint="ksqldb://lksqlc-test123",
        ),
        # Self-managed Connect source: external → jdbc-orders
        LineageEdge(
            source_name="jdbc-source",
            source_type="external",
            target_name="jdbc-orders",
            target_type="kafka_topic",
            job_name="jdbc-source",
            job_type="kafka_connect_source",
            job_namespace_hint="kafka-connect://on-prem-connect",
        ),
        # Kafka producer: external → ol-raw-orders
        LineageEdge(
            source_name="order-service-producer",
            source_type="external",
            target_name="ol-raw-orders",
            target_type="kafka_topic",
            job_name="order-service-producer",
            job_type="kafka_producer",
            job_namespace_hint="kafka-producer://lkc-abc123",
        ),
    ]
    return LineageGraph(
        edges=edges,
        connectors=[
            sample_source_connector,
            sample_sink_connector,
            sample_self_managed_connector,
        ],
        statements=[sample_flink_statement],
        producers=[sample_producer],
        consumer_groups=[sample_consumer_group],
        ksql_queries=[sample_ksql_query],
    )

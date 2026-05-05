"""Data models for all Confluent lineage sources.

Lineage is assembled from six sources:
  1. Connect API (managed)       — connector configs, topic mappings
  2. Flink SQL (CLI)             — statement SQL, parsed for tables
  3. Confluent Metrics API       — consumer_lag_offsets → consumer group → topic
  4. ksqlDB REST API             — persistent queries, stream/table → topic map
  5. Self-managed Connect REST   — connector configs from customer-hosted clusters
  6. Tableflow CLI               — active topic → Iceberg table mappings
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from openlineage_confluent.confluent.kafka_rest_client import TopicMetadata
from openlineage_confluent.confluent.schema_registry_client import TopicSchema


# ──────────────────────────────────────────────────────────────────────────────
# Per-topic throughput  (defined here to avoid a circular import between
# metrics_client.py and models.py)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class TopicThroughput:
    """Per-topic throughput totals over the metrics lookback window."""

    topic: str
    bytes_in: int = 0       # received_bytes
    bytes_out: int = 0      # sent_bytes
    records_in: int = 0     # received_records
    records_out: int = 0    # sent_records
    window_minutes: int = 0


# ──────────────────────────────────────────────────────────────────────────────
# Connector models  (managed + self-managed Connect)
# ──────────────────────────────────────────────────────────────────────────────

class ConnectorType(StrEnum):
    SOURCE = "source"
    SINK   = "sink"


class ConnectorInfo(BaseModel):
    """Metadata for a managed or self-managed Kafka Connect connector."""

    id: str
    name: str
    # str, not a StrEnum — self-managed clusters can return non-standard states
    # (e.g. "UNASSIGNED", "RESTARTING") that aren't in the Confluent managed set.
    status: str
    connector_type: ConnectorType = Field(alias="type")
    config: dict[str, Any] = Field(default_factory=dict)

    # None  → Confluent Cloud managed connector (namespace keyed on env_id)
    # "str" → self-managed cluster label (namespace keyed on this value)
    connect_cluster: str | None = None

    model_config = {"populate_by_name": True}

    def topics_produced(self) -> list[str]:
        """Topics this source connector writes to."""
        if self.connector_type != ConnectorType.SOURCE:
            return []
        raw = self.config.get("kafka.topic") or self.config.get("topic") or ""
        return [t.strip() for t in raw.split(",") if t.strip()]

    def topics_consumed(self) -> list[str]:
        """Topics this sink connector reads from."""
        if self.connector_type != ConnectorType.SINK:
            return []
        raw = (
            self.config.get("topics")
            or self.config.get("kafka.topics")
            or self.config.get("topic")
            or ""
        )
        return [t.strip() for t in raw.split(",") if t.strip()]


# ──────────────────────────────────────────────────────────────────────────────
# Flink Statement models  (confluent flink statement list -o json)
# ──────────────────────────────────────────────────────────────────────────────

class FlinkStatement(BaseModel):
    """A Confluent Cloud Flink SQL statement."""

    name: str
    sql: str = Field(alias="statement")
    status: str = "UNKNOWN"
    compute_pool: str = Field(alias="compute_pool", default="")

    model_config = {"populate_by_name": True}

    def is_running(self) -> bool:
        # Include STOPPED (deployed but paused) so lineage reflects the full
        # deployed topology, not just what's actively streaming right now.
        return self.status.upper() in ("RUNNING", "STOPPED")


# ──────────────────────────────────────────────────────────────────────────────
# Kafka Producer models  (Confluent Metrics API — received_bytes / client_id)
# ──────────────────────────────────────────────────────────────────────────────

class KafkaProducerInfo(BaseModel):
    """A Kafka producer identified by its client_id and the topics it writes to.

    Sourced from the Confluent Metrics API received_bytes metric, grouped by
    metric.client_id + metric.topic. Covers any Kafka producer that wrote to a
    topic within the configured lookback window.

    Internal Confluent producer client_ids (connector-producer-*, etc.) are
    filtered out — those are already represented via the Connect API source.
    """

    client_id: str
    topics: list[str] = Field(default_factory=list)


# ──────────────────────────────────────────────────────────────────────────────
# Consumer Group models  (Confluent Metrics API — consumer_lag_offsets)
# ──────────────────────────────────────────────────────────────────────────────

class ConsumerGroupInfo(BaseModel):
    """A Kafka consumer group and the topics it actively reads.

    Sourced from the Confluent Metrics API consumer_lag_offsets metric,
    grouped by consumer_group_id + topic. Covers application consumers,
    Kafka Streams apps, and self-managed Connect workers.
    """

    group_id: str
    topics: list[str] = Field(default_factory=list)


# ──────────────────────────────────────────────────────────────────────────────
# ksqlDB models  (ksqlDB REST API — SHOW QUERIES EXTENDED)
# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# Tableflow models  (confluent tableflow topic list)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class TableflowTopic:
    """A Confluent Tableflow-enabled topic and its Iceberg table mapping."""

    topic_name: str
    status: str = "UNKNOWN"

    @property
    def iceberg_table_name(self) -> str:
        """Glue table name: hyphens replaced with underscores."""
        return self.topic_name.replace("-", "_")


class KsqlQuery(BaseModel):
    """A ksqlDB persistent query with resolved Kafka topic bindings.

    Source streams/tables are resolved to their underlying Kafka topics
    via SHOW STREAMS/TABLES EXTENDED before storing in source_topics.
    """

    query_id: str
    sql: str
    state: str = "UNKNOWN"
    sink_topics: list[str] = Field(default_factory=list)    # actual Kafka topics written
    source_topics: list[str] = Field(default_factory=list)  # resolved Kafka topic names
    ksql_cluster_id: str

    def is_running(self) -> bool:
        return self.state.upper() == "RUNNING"


# ──────────────────────────────────────────────────────────────────────────────
# Internal lineage graph edge (derived, not from any single API)
# ──────────────────────────────────────────────────────────────────────────────

class LineageEdge(BaseModel):
    """One directed data-flow edge in the lineage graph.

    Edge semantics by job_type:
      kafka_connect_source      — external → kafka_topic
      kafka_connect_sink        — kafka_topic → external
      flink_statement           — kafka_topic → kafka_topic
      kafka_consumer_group      — kafka_topic → consumer_group
      ksqldb_query              — kafka_topic → kafka_topic
      tableflow                 — kafka_topic → iceberg_table

    The mapper converts edges to RunEvents. Only kafka_topic and iceberg_table
    typed endpoints become OpenLineage Dataset nodes; external and consumer_group
    endpoints are silently dropped on the OL side (resulting in inputs-only or
    outputs-only events, which is correct and intentional).
    """

    source_name: str          # topic, external system, or consumer group name
    source_type: str          # "kafka_topic" | "external" | "iceberg_table"
    target_name: str
    target_type: str          # "kafka_topic" | "external" | "consumer_group" | "iceberg_table"
    job_name: str             # connector name, statement name, group ID, query ID
    job_type: str             # see docstring above
    job_namespace_hint: str   # used directly as the OL Job namespace
    target_namespace: str | None = None  # explicit OL namespace for non-Kafka targets
    # Per-env Kafka bootstrap, used to namespace topic Datasets. None falls
    # back to OpenLineageConfig.kafka_bootstrap.
    kafka_bootstrap: str | None = None
    # Confluent topology context — emitted as ConfluentJobFacet on jobs and
    # ConfluentDatasetFacet on Kafka topic datasets. Lets Marquez consumers
    # filter / group by env / cluster / region without having to parse the
    # job namespace URI. None on globally-scoped sources (ksqlDB, self-managed
    # Connect) where the env is irrelevant.
    env_id:     str | None = None
    cluster_id: str | None = None
    cloud:      str | None = None    # "aws" | "gcp" | "azure"
    region:     str | None = None    # e.g. "us-east-2"


class LineageGraph(BaseModel):
    """Complete lineage graph built from all sources in one poll cycle."""

    # TopicSchema is a non-pydantic dataclass; allow it as a field value.
    model_config = ConfigDict(arbitrary_types_allowed=True)

    edges: list[LineageEdge] = Field(default_factory=list)

    # Constituent source objects (kept for summary and debugging)
    connectors: list[ConnectorInfo] = Field(default_factory=list)      # managed + self-managed
    statements: list[FlinkStatement] = Field(default_factory=list)
    producers: list[KafkaProducerInfo] = Field(default_factory=list)
    consumer_groups: list[ConsumerGroupInfo] = Field(default_factory=list)
    ksql_queries: list[KsqlQuery] = Field(default_factory=list)
    tableflow_topics: list[TableflowTopic] = Field(default_factory=list)

    # topic name → resolved key/value schemas (empty if SR not configured)
    topic_schemas: dict[str, TopicSchema] = Field(default_factory=dict)

    # topic name → partition count + replication factor (empty if Kafka REST not configured)
    topic_metadata: dict[str, TopicMetadata] = Field(default_factory=dict)

    # topic name → throughput over the metrics lookback window
    topic_throughput: dict[str, TopicThroughput] = Field(default_factory=dict)

    # OL job-namespace prefixes (e.g. "kafka-connect://env-aaa") whose source
    # fetch failed this cycle (401, 5xx, network error, CLI subprocess crash).
    # The emitter consults this set to suppress removal-detection for jobs in
    # these namespaces — a transient auth/network blip must not synthesize a
    # fake ABORT that wipes lineage in Marquez. Successful next poll picks
    # them back up; persistent failures keep them quarantined indefinitely.
    failed_namespaces: set[str] = Field(default_factory=set)

    def summary(self) -> dict[str, int]:
        managed  = sum(1 for c in self.connectors if c.connect_cluster is None)
        self_mgd = sum(1 for c in self.connectors if c.connect_cluster is not None)
        return {
            "managed_connectors":      managed,
            "self_managed_connectors": self_mgd,
            "flink_statements":        len(self.statements),
            "kafka_producers":         len(self.producers),
            "consumer_groups":         len(self.consumer_groups),
            "ksql_queries":            len(self.ksql_queries),
            "tableflow_topics":        len(self.tableflow_topics),
            "edges":                   len(self.edges),
            "topics_with_schema":      len(self.topic_schemas),
            "topics_with_metadata":    len(self.topic_metadata),
            "topics_with_throughput":  len(self.topic_throughput),
        }

"""Configuration — loaded from environment variables or a YAML file."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

_DEFAULT_STATE_DB = Path.home() / ".openlineage-confluent" / "state.db"


# ──────────────────────────────────────────────────────────────────────────────
# Nested config models (YAML-only, not env-var mapped)
# ──────────────────────────────────────────────────────────────────────────────

class KsqlClusterConfig(BaseModel):
    """Configuration for one Confluent Cloud ksqlDB cluster."""

    cluster_id: str
    rest_endpoint: str   # e.g. https://pksqlc-xxxxx.us-east-2.aws.confluent.cloud
    api_key: str
    api_secret: SecretStr


class SchemaRegistryConfig(BaseModel):
    """Configuration for the Confluent Schema Registry cluster of one environment.

    SR uses its own API key scope — Cloud-level API keys do NOT work here.
    Generate the key from Confluent Cloud → Environment → Schema Registry.
    """

    endpoint: str               # e.g. https://psrc-xxxxx.us-east-2.aws.confluent.cloud
    api_key: str
    api_secret: SecretStr


class SelfManagedConnectClusterConfig(BaseModel):
    """Configuration for one self-managed (on-prem / customer-hosted) Connect cluster."""

    name: str            # unique label — used in namespace: kafka-connect://<name>
    rest_endpoint: str   # e.g. http://connect-host:8083
    username: str | None = None
    password: SecretStr | None = None


class EnvDeployment(BaseModel):
    """One Confluent Cloud environment + its primary Kafka cluster.

    The bridge polls every entry in ConfluentConfig.environments and merges the
    resulting LineageGraphs. Per-env cluster + SR + Kafka REST credentials let
    multi-region setups produce correctly-namespaced topic Datasets.
    """

    env_id: str                           # env-xxxxxx
    cluster_id: str                       # lkc-xxxxxx — primary Kafka cluster
    kafka_bootstrap: str                  # pkc-xxx.<region>.aws.confluent.cloud:9092

    # Human-readable names. Optional; populated by the wizard during
    # _provision_env. The bridge surfaces both ID and name on the Confluent
    # topology facet so Marquez consumers can show "test-lineage" instead of
    # the opaque "env-dpog0y" in the UI.
    env_name:     str | None = None       # e.g. "test-lineage"
    cluster_name: str | None = None       # e.g. "cluster_0"

    # Optional per-env operational endpoints
    flink_compute_pool: str | None = None  # lfcp-xxxxxx (used by provisioning scripts)

    # Kafka REST (cluster-scoped key, NOT cloud-level). When unset, KafkaTopicDatasetFacet is omitted.
    kafka_rest_endpoint: str | None = None
    kafka_api_key: str | None = None
    kafka_api_secret: SecretStr | None = None

    # Schema Registry (env-scoped). When unset, SchemaDatasetFacet is omitted for this env.
    schema_registry: SchemaRegistryConfig | None = None


# ──────────────────────────────────────────────────────────────────────────────
# Core settings classes
# ──────────────────────────────────────────────────────────────────────────────

class ConfluentConfig(BaseSettings):
    """Confluent Cloud credentials and topology identifiers."""

    model_config = SettingsConfigDict(env_prefix="CONFLUENT_", populate_by_name=True)

    # ── Required ─────────────────────────────────────────────────────────────

    # Cloud-level API key (api.confluent.cloud — NOT Schema Registry or Kafka scoped)
    cloud_api_key: str = Field(..., alias="CONFLUENT_CLOUD_API_KEY")
    cloud_api_secret: SecretStr = Field(..., alias="CONFLUENT_CLOUD_API_SECRET")

    # One entry per Confluent Cloud environment to bridge.
    # Populated by the wizard (web/server.py) or hand-edited.
    environments: list[EnvDeployment] = Field(default_factory=list)

    # ── Flink (CLI-based, REST URL stored for future direct access) ───────────

    flink_rest_url: str = Field(
        default="https://flink.us-west-2.aws.confluent.cloud",
        alias="CONFLUENT_FLINK_REST_URL",
    )

    # ── Metrics API ───────────────────────────────────────────────────────────
    # Used to discover consumer group → topic mappings.
    # If not set, falls back to cloud_api_key / cloud_api_secret.
    # The key must have the MetricsViewer role at Environment or Org level.

    metrics_api_key: str | None = Field(default=None, alias="CONFLUENT_METRICS_API_KEY")
    metrics_api_secret: SecretStr | None = Field(
        default=None, alias="CONFLUENT_METRICS_API_SECRET"
    )

    # How far back to look (minutes). Consumer groups active within this window
    # appear in the lineage graph. Default 10 min covers a single poll cycle
    # with headroom for Metrics API ingestion lag (~2-3 min).
    metrics_lookback_minutes: int = Field(
        default=10, alias="CONFLUENT_METRICS_LOOKBACK_MINUTES"
    )

    # Additional consumer group ID prefixes to exclude from lineage.
    # Built-in exclusions: "_" prefix (Confluent-internal) and
    # "confluent_cli_consumer_" prefix (CLI ad-hoc consumers).
    consumer_group_exclude_prefixes: list[str] = Field(default_factory=list)

    # Additional producer client_id prefixes to exclude from lineage.
    # Built-in exclusions: "connector-producer-" and "connector-" (Connect internals).
    producer_client_id_exclude_prefixes: list[str] = Field(default_factory=list)

    # ── ksqlDB clusters (list, YAML-only) ────────────────────────────────────

    ksql_clusters: list[KsqlClusterConfig] = Field(default_factory=list)

    # ── Self-managed Connect clusters (list, YAML-only) ──────────────────────

    self_managed_connect_clusters: list[SelfManagedConnectClusterConfig] = Field(
        default_factory=list
    )

    # ── Tableflow → Iceberg (optional) ───────────────────────────────────────
    # When set, Tableflow topics are included in the lineage graph with their
    # output Iceberg datasets identified by the Glue catalog namespace.
    # Must match what the OpenLineage Spark integration emits for the same tables.

    tableflow_glue_region: str | None = Field(default=None, alias="CONFLUENT_TABLEFLOW_GLUE_REGION")
    tableflow_glue_database: str | None = Field(default=None, alias="CONFLUENT_TABLEFLOW_GLUE_DB")


class OpenLineageConfig(BaseSettings):
    """OpenLineage transport settings."""

    model_config = SettingsConfigDict(env_prefix="OPENLINEAGE_", populate_by_name=True)

    transport: Literal["http", "console"] = Field(default="http", alias="OPENLINEAGE_TRANSPORT")
    url: str = Field(default="http://localhost:5000", alias="OPENLINEAGE_URL")
    api_key: str | None = Field(default=None, alias="OPENLINEAGE_API_KEY")

    # Fallback Kafka bootstrap — used as namespace for topic Datasets when an
    # edge has no per-env bootstrap set. Per-env values from
    # EnvDeployment.kafka_bootstrap take precedence.
    kafka_bootstrap: str = Field(default="", alias="OPENLINEAGE_KAFKA_BOOTSTRAP")

    producer: str = Field(
        default="openlineage-confluent/0.1.0",
        alias="OPENLINEAGE_PRODUCER",
    )


class PipelineConfig(BaseSettings):
    """Polling / scheduling settings."""

    model_config = SettingsConfigDict(env_prefix="PIPELINE_", populate_by_name=True)

    poll_interval_seconds: int = Field(default=60, alias="PIPELINE_POLL_INTERVAL")
    emit_full_refresh: bool = Field(default=False, alias="PIPELINE_FULL_REFRESH")

    # Number of parallel threads for event emission.
    max_workers: int = Field(default=8, alias="PIPELINE_MAX_WORKERS")

    # SQLite state database path.
    state_db: Path = Field(default=_DEFAULT_STATE_DB, alias="PIPELINE_STATE_DB")


# ──────────────────────────────────────────────────────────────────────────────
# Backward-compatibility shim
# ──────────────────────────────────────────────────────────────────────────────

# Old config.yaml put environment_id, cluster_id, kafka_rest_*, and
# schema_registry at the top of the confluent: section. New schema collects
# all of these into the environments: list. This shim folds legacy fields into
# a single-element environments entry at load time so old configs keep working.

_LEGACY_FIELDS = (
    "CONFLUENT_ENV_ID", "environment_id",
    "CONFLUENT_CLUSTER_ID", "cluster_id",
    "CONFLUENT_KAFKA_REST_ENDPOINT", "kafka_rest_endpoint",
    "CONFLUENT_KAFKA_API_KEY", "kafka_api_key",
    "CONFLUENT_KAFKA_API_SECRET", "kafka_api_secret",
    "schema_registry",
)


def _apply_legacy_shim(raw: dict[str, Any]) -> dict[str, Any]:
    """Fold legacy top-level env fields into a single-element environments list.

    Mutates and returns `raw` (the parsed YAML mapping). No-op if the new
    `environments:` list is already populated, or if no legacy fields are set.
    """
    confluent = raw.get("confluent") or {}
    if confluent.get("environments"):
        return raw
    if not any(confluent.get(k) for k in _LEGACY_FIELDS):
        return raw

    env_id     = confluent.get("CONFLUENT_ENV_ID")     or confluent.get("environment_id")
    cluster_id = confluent.get("CONFLUENT_CLUSTER_ID") or confluent.get("cluster_id")
    if not env_id or not cluster_id:
        # Not enough to synthesize — let pydantic raise a clear validation error
        return raw

    bootstrap = (raw.get("openlineage") or {}).get("OPENLINEAGE_KAFKA_BOOTSTRAP", "")

    entry: dict[str, Any] = {
        "env_id":          env_id,
        "cluster_id":      cluster_id,
        "kafka_bootstrap": bootstrap,
    }
    for src_key, dst_key in (
        ("CONFLUENT_KAFKA_REST_ENDPOINT", "kafka_rest_endpoint"),
        ("kafka_rest_endpoint",           "kafka_rest_endpoint"),
        ("CONFLUENT_KAFKA_API_KEY",       "kafka_api_key"),
        ("kafka_api_key",                 "kafka_api_key"),
        ("CONFLUENT_KAFKA_API_SECRET",    "kafka_api_secret"),
        ("kafka_api_secret",              "kafka_api_secret"),
    ):
        if src_key in confluent and confluent[src_key] is not None:
            entry[dst_key] = confluent[src_key]
    if "schema_registry" in confluent and confluent["schema_registry"] is not None:
        entry["schema_registry"] = confluent["schema_registry"]

    confluent["environments"] = [entry]

    # Strip legacy fields so pydantic does not warn about extras
    for k in _LEGACY_FIELDS:
        confluent.pop(k, None)
    raw["confluent"] = confluent
    return raw


class AppConfig:
    """Aggregate config object. Load from env or YAML."""

    def __init__(
        self,
        confluent: ConfluentConfig,
        openlineage: OpenLineageConfig,
        pipeline: PipelineConfig,
    ) -> None:
        self.confluent = confluent
        self.openlineage = openlineage
        self.pipeline = pipeline

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            confluent=ConfluentConfig(),
            openlineage=OpenLineageConfig(),
            pipeline=PipelineConfig(),
        )

    @classmethod
    def from_yaml(cls, path: Path) -> "AppConfig":
        raw = yaml.safe_load(path.read_text()) or {}
        raw = _apply_legacy_shim(raw)
        return cls(
            confluent=ConfluentConfig.model_validate(raw.get("confluent", {})),
            openlineage=OpenLineageConfig.model_validate(raw.get("openlineage", {})),
            pipeline=PipelineConfig.model_validate(raw.get("pipeline", {})),
        )

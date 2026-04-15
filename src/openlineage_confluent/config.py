"""Configuration — loaded from environment variables or a YAML file."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

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


class SelfManagedConnectClusterConfig(BaseModel):
    """Configuration for one self-managed (on-prem / customer-hosted) Connect cluster."""

    name: str            # unique label — used in namespace: kafka-connect://<name>
    rest_endpoint: str   # e.g. http://connect-host:8083
    username: str | None = None
    password: SecretStr | None = None


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

    environment_id: str = Field(..., alias="CONFLUENT_ENV_ID")
    cluster_id: str     = Field(..., alias="CONFLUENT_CLUSTER_ID")

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

    # ── ksqlDB clusters (list, YAML-only) ────────────────────────────────────

    ksql_clusters: list[KsqlClusterConfig] = Field(default_factory=list)

    # ── Self-managed Connect clusters (list, YAML-only) ──────────────────────

    self_managed_connect_clusters: list[SelfManagedConnectClusterConfig] = Field(
        default_factory=list
    )


class OpenLineageConfig(BaseSettings):
    """OpenLineage transport settings."""

    model_config = SettingsConfigDict(env_prefix="OPENLINEAGE_", populate_by_name=True)

    transport: Literal["http", "console"] = Field(default="http", alias="OPENLINEAGE_TRANSPORT")
    url: str = Field(default="http://localhost:5000", alias="OPENLINEAGE_URL")
    api_key: str | None = Field(default=None, alias="OPENLINEAGE_API_KEY")

    # Kafka bootstrap — used as namespace for all topic Datasets
    kafka_bootstrap: str = Field(..., alias="OPENLINEAGE_KAFKA_BOOTSTRAP")

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
        raw = yaml.safe_load(path.read_text())
        return cls(
            confluent=ConfluentConfig.model_validate(raw.get("confluent", {})),
            openlineage=OpenLineageConfig.model_validate(raw.get("openlineage", {})),
            pipeline=PipelineConfig.model_validate(raw.get("pipeline", {})),
        )

"""Configuration — loaded from environment variables or a YAML file."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConfluentConfig(BaseSettings):
    """Confluent Cloud credentials and target identifiers."""

    model_config = SettingsConfigDict(env_prefix="CONFLUENT_", populate_by_name=True)

    # Cloud-level API key (for api.confluent.cloud — not resource-specific)
    cloud_api_key: str = Field(..., alias="CONFLUENT_CLOUD_API_KEY")
    cloud_api_secret: SecretStr = Field(..., alias="CONFLUENT_CLOUD_API_SECRET")

    # Confluent Cloud topology IDs
    environment_id: str = Field(..., alias="CONFLUENT_ENV_ID")
    cluster_id: str = Field(..., alias="CONFLUENT_CLUSTER_ID")

    # Regional Flink REST endpoint  (e.g. https://flink.us-west-2.aws.confluent.cloud)
    # Find under: Confluent Cloud → Flink → Compute pool → REST endpoint
    flink_rest_url: str = Field(
        default="https://flink.us-west-2.aws.confluent.cloud",
        alias="CONFLUENT_FLINK_REST_URL",
    )


class OpenLineageConfig(BaseSettings):
    """OpenLineage transport settings."""

    model_config = SettingsConfigDict(env_prefix="OPENLINEAGE_", populate_by_name=True)

    # Transport: "http" or "console"
    transport: Literal["http", "console"] = Field(default="http", alias="OPENLINEAGE_TRANSPORT")

    # HTTP transport
    url: str = Field(default="http://localhost:5000", alias="OPENLINEAGE_URL")
    api_key: str | None = Field(default=None, alias="OPENLINEAGE_API_KEY")

    # Kafka bootstrap — used as namespace for topic Datasets
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

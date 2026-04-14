"""Data models for Confluent Cloud REST API responses.

Confluent lineage is NOT in the Atlas catalog (/catalog/v1/lineage/).
Instead we build it from two sources:
  1. Connect API  — connector configs (source/sink topic mapping)
  2. Flink SQL API — statement SQL (parsed for input/output tables)
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Connector models  (api.confluent.cloud/connect/v1/...)
# ---------------------------------------------------------------------------

class ConnectorType(StrEnum):
    SOURCE = "source"
    SINK   = "sink"


class ConnectorStatus(StrEnum):
    RUNNING  = "RUNNING"
    PAUSED   = "PAUSED"
    FAILED   = "FAILED"
    DEGRADED = "DEGRADED"


class ConnectorInfo(BaseModel):
    """Metadata about a single Confluent Cloud Managed Connector."""
    id: str
    name: str
    status: ConnectorStatus
    connector_type: ConnectorType = Field(alias="type")
    config: dict[str, Any] = Field(default_factory=dict)

    model_config = {"populate_by_name": True}

    # ---- convenience -------------------------------------------------------

    def topics_produced(self) -> list[str]:
        """Topics this connector writes to (source connectors)."""
        if self.connector_type != ConnectorType.SOURCE:
            return []
        raw = self.config.get("kafka.topic") or self.config.get("topic") or ""
        return [t.strip() for t in raw.split(",") if t.strip()]

    def topics_consumed(self) -> list[str]:
        """Topics this connector reads from (sink connectors)."""
        if self.connector_type != ConnectorType.SINK:
            return []
        raw = (
            self.config.get("topics")
            or self.config.get("kafka.topics")
            or self.config.get("topic")
            or ""
        )
        return [t.strip() for t in raw.split(",") if t.strip()]


# ---------------------------------------------------------------------------
# Flink Statement models  (api.confluent.cloud/sql/v1/...)
# ---------------------------------------------------------------------------

class FlinkStatementStatus(StrEnum):
    RUNNING   = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED    = "FAILED"
    STOPPED   = "STOPPED"
    PENDING   = "PENDING"
    DELETING  = "DELETING"


class FlinkStatement(BaseModel):
    """A Confluent Cloud Flink SQL statement."""
    name: str
    sql: str = Field(alias="statement")
    status: str = "UNKNOWN"
    compute_pool: str = Field(alias="compute_pool", default="")

    model_config = {"populate_by_name": True}

    def is_running(self) -> bool:
        return self.status.upper() == "RUNNING"


# ---------------------------------------------------------------------------
# Internal lineage graph edge (derived, not from API)
# ---------------------------------------------------------------------------

class LineageEdge(BaseModel):
    """One data-flow edge in our constructed lineage graph."""

    source_name: str          # topic / external system name
    source_type: str          # "kafka_topic" | "external"
    target_name: str
    target_type: str          # "kafka_topic" | "external"
    job_name: str             # connector or Flink statement name
    job_type: str             # "kafka_connect_source" | "kafka_connect_sink" | "flink_statement"
    job_namespace_hint: str   # for building OL namespace


class LineageGraph(BaseModel):
    """Derived lineage graph for one cluster poll cycle."""
    edges: list[LineageEdge] = Field(default_factory=list)
    connectors: list[ConnectorInfo] = Field(default_factory=list)
    statements: list[FlinkStatement] = Field(default_factory=list)

    def summary(self) -> dict[str, int]:
        return {
            "connectors": len(self.connectors),
            "flink_statements": len(self.statements),
            "edges": len(self.edges),
        }

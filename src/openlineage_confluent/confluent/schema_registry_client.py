"""Confluent Schema Registry client.

Fetches the latest schema for each topic's key + value subjects and converts
them to OpenLineage SchemaDatasetFacet field lists.

Subject naming convention (TopicNameStrategy — Confluent default):
  - {topic}-key
  - {topic}-value

Other strategies (RecordNameStrategy, TopicRecordNameStrategy) are not yet
resolved here; only TopicNameStrategy subjects are matched.

Supported schema types:
  - AVRO          — parsed into nested fields
  - JSON          — JSON Schema parsed into nested fields
  - PROTOBUF      — stored as a single opaque field (proto2/proto3 parsing
                    requires protoc; left as future work)

Auth: HTTP Basic with a Schema Registry API key/secret pair scoped to the SR
cluster. Cloud-level API keys do NOT work — Schema Registry uses its own
key scope.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

import httpx

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Result types
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class SchemaField:
    """One field in a SchemaDatasetFacet — mirrors SchemaDatasetFacetFields."""

    name: str
    type: str
    description: str | None = None
    fields: list[SchemaField] = field(default_factory=list)


@dataclass
class TopicSchema:
    """Resolved key + value schema for a single Kafka topic."""

    topic: str
    key_fields: list[SchemaField] = field(default_factory=list)
    value_fields: list[SchemaField] = field(default_factory=list)
    key_schema_type: str | None = None      # AVRO | JSON | PROTOBUF
    value_schema_type: str | None = None
    key_subject: str | None = None
    value_subject: str | None = None


# ──────────────────────────────────────────────────────────────────────────────
# Client
# ──────────────────────────────────────────────────────────────────────────────

class SchemaRegistryClient:
    """Fetch latest schemas from Confluent Schema Registry.

    Uses bulk subject listing + individual latest-version fetches. For
    deployments with a few hundred subjects this is one-API-call-per-subject;
    that is fine at our poll cadence (≥1 minute).
    """

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        api_secret: str,
        *,
        timeout: float = 30.0,
    ) -> None:
        self._http = httpx.Client(
            base_url=endpoint.rstrip("/"),
            auth=(api_key, api_secret),
            timeout=timeout,
            headers={"Accept": "application/vnd.schemaregistry.v1+json"},
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    def get_topic_schemas(self, topics: list[str]) -> dict[str, TopicSchema]:
        """Return {topic: TopicSchema} for all requested topics.

        Topics with no key or value subject get an empty TopicSchema; topics
        that fail to fetch are silently skipped (logged at WARN).
        """
        if not topics:
            return {}

        subjects = self._list_subjects()
        log.debug("Schema Registry: %d subjects available", len(subjects))

        result: dict[str, TopicSchema] = {}
        for topic in topics:
            ts = TopicSchema(topic=topic)

            key_sub = f"{topic}-key"
            val_sub = f"{topic}-value"

            if key_sub in subjects:
                schema_type, fields = self._fetch_latest_fields(key_sub)
                ts.key_subject = key_sub
                ts.key_schema_type = schema_type
                ts.key_fields = fields
            if val_sub in subjects:
                schema_type, fields = self._fetch_latest_fields(val_sub)
                ts.value_subject = val_sub
                ts.value_schema_type = schema_type
                ts.value_fields = fields

            if ts.key_subject or ts.value_subject:
                result[topic] = ts

        log.info(
            "Schema Registry: resolved schemas for %d/%d topics",
            len(result), len(topics),
        )
        return result

    # ──────────────────────────────────────────────────────────────────────────
    # Subject + schema fetch
    # ──────────────────────────────────────────────────────────────────────────

    def _list_subjects(self) -> set[str]:
        try:
            resp = self._http.get("/subjects")
            resp.raise_for_status()
            return set(resp.json())
        except httpx.HTTPError as exc:
            log.warning("Schema Registry: failed to list subjects: %s", exc)
            return set()

    def _fetch_latest_fields(self, subject: str) -> tuple[str | None, list[SchemaField]]:
        """Return (schema_type, fields) for the latest version of a subject."""
        try:
            resp = self._http.get(f"/subjects/{subject}/versions/latest")
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            log.warning("Schema Registry: fetch failed for %s: %s", subject, exc)
            return None, []

        body = resp.json()
        # schemaType absent in response means AVRO (Confluent default)
        schema_type = body.get("schemaType", "AVRO").upper()
        schema_str  = body.get("schema", "")

        if schema_type == "AVRO":
            return schema_type, _parse_avro(schema_str)
        if schema_type == "JSON":
            return schema_type, _parse_json_schema(schema_str)
        if schema_type == "PROTOBUF":
            # Proto parsing requires protoc; surface as one opaque field.
            return schema_type, [SchemaField(name="<protobuf>", type="protobuf",
                                             description=schema_str[:500])]
        return schema_type, []

    # ──────────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────────────────────────────────

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> SchemaRegistryClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


# ──────────────────────────────────────────────────────────────────────────────
# Schema parsers
# ──────────────────────────────────────────────────────────────────────────────

def _parse_avro(schema_str: str) -> list[SchemaField]:
    """Parse an Avro schema JSON string into SchemaField list.

    Handles record / array / map / union / enum / fixed / primitives. Nested
    records produce nested SchemaField.fields.
    """
    try:
        node = json.loads(schema_str)
    except json.JSONDecodeError as exc:
        log.warning("Schema Registry: invalid Avro schema JSON: %s", exc)
        return []

    fields = _avro_record_fields(node)
    return fields


def _avro_record_fields(node: Any) -> list[SchemaField]:
    """Return field list for an Avro record, or [] if node isn't a record."""
    if not isinstance(node, dict):
        return []
    if node.get("type") != "record":
        return []

    out: list[SchemaField] = []
    for f in node.get("fields", []):
        if not isinstance(f, dict):
            continue
        name = f.get("name", "")
        if not name:
            continue
        ftype = f.get("type")
        type_str, sub_fields = _avro_type(ftype)
        out.append(SchemaField(
            name=name,
            type=type_str,
            description=f.get("doc"),
            fields=sub_fields,
        ))
    return out


def _avro_type(node: Any) -> tuple[str, list[SchemaField]]:
    """Render an Avro type node as (type_string, nested_fields)."""
    # Primitive
    if isinstance(node, str):
        return node, []

    # Union (array of types)
    if isinstance(node, list):
        # Drop "null" from union for the rendered type — keep the non-null branch.
        non_null = [t for t in node if t != "null"]
        if len(non_null) == 1:
            return _avro_type(non_null[0])
        rendered = " | ".join(_avro_type(t)[0] for t in node)
        return f"union<{rendered}>", []

    if isinstance(node, dict):
        ntype = node.get("type")
        if ntype == "record":
            return f"record<{node.get('name', '')}>", _avro_record_fields(node)
        if ntype == "array":
            inner_type, inner_fields = _avro_type(node.get("items"))
            return f"array<{inner_type}>", inner_fields
        if ntype == "map":
            inner_type, inner_fields = _avro_type(node.get("values"))
            return f"map<string,{inner_type}>", inner_fields
        if ntype == "enum":
            symbols = ",".join(node.get("symbols", []))
            return f"enum<{symbols}>", []
        if ntype == "fixed":
            return f"fixed<{node.get('size', '?')}>", []
        # Logical type or primitive wrapped in an object
        if isinstance(ntype, str):
            logical = node.get("logicalType")
            return f"{ntype}({logical})" if logical else ntype, []

    return "unknown", []


def _parse_json_schema(schema_str: str) -> list[SchemaField]:
    """Parse a JSON Schema string into a flat field list (top-level properties)."""
    try:
        node = json.loads(schema_str)
    except json.JSONDecodeError as exc:
        log.warning("Schema Registry: invalid JSON Schema: %s", exc)
        return []

    return _json_schema_object_fields(node)


def _json_schema_object_fields(node: Any) -> list[SchemaField]:
    if not isinstance(node, dict):
        return []
    props = node.get("properties")
    if not isinstance(props, dict):
        return []

    out: list[SchemaField] = []
    for name, prop in props.items():
        if not isinstance(prop, dict):
            continue
        ptype = prop.get("type", "unknown")
        if isinstance(ptype, list):
            ptype = " | ".join(str(t) for t in ptype)
        sub: list[SchemaField] = []
        if ptype == "object":
            sub = _json_schema_object_fields(prop)
        elif ptype == "array":
            items = prop.get("items", {})
            item_type = items.get("type") if isinstance(items, dict) else "unknown"
            ptype = f"array<{item_type}>"
            if isinstance(items, dict) and items.get("type") == "object":
                sub = _json_schema_object_fields(items)
        out.append(SchemaField(
            name=name,
            type=str(ptype),
            description=prop.get("description"),
            fields=sub,
        ))
    return out

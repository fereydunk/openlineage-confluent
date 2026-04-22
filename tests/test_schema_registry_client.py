"""Tests for SchemaRegistryClient and Avro/JSON Schema parsers."""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from openlineage_confluent.confluent.schema_registry_client import (
    SchemaField,
    SchemaRegistryClient,
    _parse_avro,
    _parse_json_schema,
)


# ── Avro parser ────────────────────────────────────────────────────────────────

def test_avro_parser_primitive_fields() -> None:
    schema = json.dumps({
        "type": "record", "name": "Order",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "amount", "type": "double"},
            {"name": "count", "type": "int"},
        ],
    })
    out = _parse_avro(schema)
    assert [f.name for f in out] == ["id", "amount", "count"]
    assert [f.type for f in out] == ["string", "double", "int"]


def test_avro_parser_unwraps_nullable_union() -> None:
    schema = json.dumps({
        "type": "record", "name": "X",
        "fields": [{"name": "name", "type": ["null", "string"]}],
    })
    out = _parse_avro(schema)
    assert out[0].type == "string"


def test_avro_parser_array_and_map() -> None:
    schema = json.dumps({
        "type": "record", "name": "X",
        "fields": [
            {"name": "items", "type": {"type": "array", "items": "string"}},
            {"name": "meta", "type": {"type": "map", "values": "long"}},
        ],
    })
    out = _parse_avro(schema)
    assert out[0].type == "array<string>"
    assert out[1].type == "map<string,long>"


def test_avro_parser_nested_record() -> None:
    schema = json.dumps({
        "type": "record", "name": "Order",
        "fields": [{
            "name": "customer",
            "type": {
                "type": "record", "name": "Customer",
                "fields": [{"name": "email", "type": "string"}],
            },
        }],
    })
    out = _parse_avro(schema)
    assert out[0].name == "customer"
    assert out[0].type == "record<Customer>"
    assert len(out[0].fields) == 1
    assert out[0].fields[0].name == "email"


def test_avro_parser_doc_propagated_to_description() -> None:
    schema = json.dumps({
        "type": "record", "name": "X",
        "fields": [{"name": "id", "type": "string", "doc": "primary key"}],
    })
    out = _parse_avro(schema)
    assert out[0].description == "primary key"


def test_avro_parser_invalid_json_returns_empty() -> None:
    assert _parse_avro("not json {{") == []


# ── JSON Schema parser ────────────────────────────────────────────────────────

def test_json_schema_parser_simple_properties() -> None:
    schema = json.dumps({
        "type": "object",
        "properties": {
            "id":     {"type": "string"},
            "amount": {"type": "number", "description": "USD"},
        },
    })
    out = _parse_json_schema(schema)
    assert {f.name: f.type for f in out} == {"id": "string", "amount": "number"}
    by_name = {f.name: f for f in out}
    assert by_name["amount"].description == "USD"


def test_json_schema_parser_nested_object() -> None:
    schema = json.dumps({
        "type": "object",
        "properties": {
            "customer": {
                "type": "object",
                "properties": {"email": {"type": "string"}},
            },
        },
    })
    out = _parse_json_schema(schema)
    assert out[0].name == "customer"
    assert out[0].type == "object"
    assert out[0].fields[0].name == "email"


def test_json_schema_parser_array_of_objects() -> None:
    schema = json.dumps({
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"sku": {"type": "string"}},
                },
            },
        },
    })
    out = _parse_json_schema(schema)
    assert out[0].type == "array<object>"
    assert out[0].fields[0].name == "sku"


# ── HTTP client integration ───────────────────────────────────────────────────

@pytest.fixture()
def sr_client() -> SchemaRegistryClient:
    return SchemaRegistryClient(
        endpoint="https://psrc-test.confluent.cloud",
        api_key="key",
        api_secret="secret",
    )


@respx.mock
def test_get_topic_schemas_resolves_key_and_value(sr_client: SchemaRegistryClient) -> None:
    avro_value = json.dumps({
        "type": "record", "name": "Order",
        "fields": [{"name": "id", "type": "string"}],
    })
    avro_key = json.dumps({
        "type": "record", "name": "OrderKey",
        "fields": [{"name": "order_id", "type": "string"}],
    })

    respx.get("https://psrc-test.confluent.cloud/subjects").mock(
        return_value=httpx.Response(200, json=["orders-key", "orders-value", "other-value"])
    )
    respx.get("https://psrc-test.confluent.cloud/subjects/orders-key/versions/latest").mock(
        return_value=httpx.Response(200, json={"schemaType": "AVRO", "schema": avro_key})
    )
    respx.get("https://psrc-test.confluent.cloud/subjects/orders-value/versions/latest").mock(
        return_value=httpx.Response(200, json={"schemaType": "AVRO", "schema": avro_value})
    )

    result = sr_client.get_topic_schemas(["orders"])
    assert "orders" in result
    ts = result["orders"]
    assert ts.key_subject == "orders-key"
    assert ts.value_subject == "orders-value"
    assert ts.key_schema_type == "AVRO"
    assert [f.name for f in ts.value_fields] == ["id"]
    assert [f.name for f in ts.key_fields] == ["order_id"]


@respx.mock
def test_get_topic_schemas_skips_topic_without_subjects(
    sr_client: SchemaRegistryClient,
) -> None:
    respx.get("https://psrc-test.confluent.cloud/subjects").mock(
        return_value=httpx.Response(200, json=["other-value"])
    )

    result = sr_client.get_topic_schemas(["orders"])
    assert result == {}


@respx.mock
def test_get_topic_schemas_handles_missing_schema_type_as_avro(
    sr_client: SchemaRegistryClient,
) -> None:
    """SR omits schemaType when it's the default AVRO."""
    avro = json.dumps({
        "type": "record", "name": "X", "fields": [{"name": "n", "type": "int"}],
    })
    respx.get("https://psrc-test.confluent.cloud/subjects").mock(
        return_value=httpx.Response(200, json=["t-value"])
    )
    respx.get("https://psrc-test.confluent.cloud/subjects/t-value/versions/latest").mock(
        return_value=httpx.Response(200, json={"schema": avro})  # no schemaType
    )

    result = sr_client.get_topic_schemas(["t"])
    assert result["t"].value_schema_type == "AVRO"
    assert result["t"].value_fields[0].name == "n"


@respx.mock
def test_get_topic_schemas_subject_list_failure_returns_empty(
    sr_client: SchemaRegistryClient,
) -> None:
    respx.get("https://psrc-test.confluent.cloud/subjects").mock(
        return_value=httpx.Response(500)
    )
    assert sr_client.get_topic_schemas(["orders"]) == {}


@respx.mock
def test_get_topic_schemas_protobuf_returns_opaque_field(
    sr_client: SchemaRegistryClient,
) -> None:
    proto_def = 'syntax = "proto3"; message Order { string id = 1; }'
    respx.get("https://psrc-test.confluent.cloud/subjects").mock(
        return_value=httpx.Response(200, json=["t-value"])
    )
    respx.get("https://psrc-test.confluent.cloud/subjects/t-value/versions/latest").mock(
        return_value=httpx.Response(200, json={"schemaType": "PROTOBUF", "schema": proto_def})
    )
    result = sr_client.get_topic_schemas(["t"])
    assert result["t"].value_schema_type == "PROTOBUF"
    assert len(result["t"].value_fields) == 1
    assert result["t"].value_fields[0].type == "protobuf"


def test_get_topic_schemas_empty_input() -> None:
    client = SchemaRegistryClient("https://x", "k", "s")
    assert client.get_topic_schemas([]) == {}


def test_schema_field_dataclass_is_constructible() -> None:
    sf = SchemaField(name="x", type="string")
    assert sf.fields == []

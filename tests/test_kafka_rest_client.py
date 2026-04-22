"""Tests for KafkaRestClient — Confluent Cloud Kafka REST topic listing."""

from __future__ import annotations

import httpx
import pytest
import respx

from openlineage_confluent.confluent.kafka_rest_client import (
    KafkaRestClient,
    TopicMetadata,
)

REST_URL = "https://lkc-test.us-west-2.aws.confluent.cloud:443"
CLUSTER  = "lkc-test"


@pytest.fixture()
def client() -> KafkaRestClient:
    return KafkaRestClient(
        endpoint=REST_URL, cluster_id=CLUSTER, api_key="K", api_secret="S",
    )


@respx.mock
def test_get_topic_metadata_returns_partitions_and_replication(
    client: KafkaRestClient,
) -> None:
    respx.get(f"{REST_URL}/kafka/v3/clusters/{CLUSTER}/topics").mock(
        return_value=httpx.Response(200, json={"data": [
            {
                "topic_name": "ol-raw-orders",
                "partitions_count": 6,
                "replication_factor": 3,
                "is_internal": False,
            },
            {
                "topic_name": "_confluent-metrics",
                "partitions_count": 12,
                "replication_factor": 3,
                "is_internal": True,
            },
        ]})
    )

    out = client.get_topic_metadata()
    assert out["ol-raw-orders"] == TopicMetadata(
        topic="ol-raw-orders", partitions=6, replication_factor=3, is_internal=False,
    )
    assert out["_confluent-metrics"].is_internal is True


@respx.mock
def test_get_topic_metadata_skips_entries_without_topic_name(
    client: KafkaRestClient,
) -> None:
    respx.get(f"{REST_URL}/kafka/v3/clusters/{CLUSTER}/topics").mock(
        return_value=httpx.Response(200, json={"data": [
            {"partitions_count": 3, "replication_factor": 3},  # no topic_name
            {"topic_name": "good", "partitions_count": 1, "replication_factor": 1},
        ]})
    )

    out = client.get_topic_metadata()
    assert list(out) == ["good"]


@respx.mock
def test_get_topic_metadata_returns_empty_dict_on_http_error(
    client: KafkaRestClient,
) -> None:
    respx.get(f"{REST_URL}/kafka/v3/clusters/{CLUSTER}/topics").mock(
        return_value=httpx.Response(401)
    )
    assert client.get_topic_metadata() == {}


@respx.mock
def test_get_topic_metadata_handles_missing_data_field(
    client: KafkaRestClient,
) -> None:
    respx.get(f"{REST_URL}/kafka/v3/clusters/{CLUSTER}/topics").mock(
        return_value=httpx.Response(200, json={})
    )
    assert client.get_topic_metadata() == {}


@respx.mock
def test_get_topic_metadata_default_zero_when_fields_missing(
    client: KafkaRestClient,
) -> None:
    respx.get(f"{REST_URL}/kafka/v3/clusters/{CLUSTER}/topics").mock(
        return_value=httpx.Response(200, json={"data": [
            {"topic_name": "t"},  # no partitions / replication
        ]})
    )
    out = client.get_topic_metadata()
    assert out["t"].partitions == 0
    assert out["t"].replication_factor == 0

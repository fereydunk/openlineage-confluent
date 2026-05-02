"""Tests for ConfluentOpenLineageMapper."""

from __future__ import annotations

import pytest

from openlineage.client.event_v2 import RunState

from openlineage_confluent.confluent.kafka_rest_client import TopicMetadata
from openlineage_confluent.confluent.models import LineageGraph
from openlineage_confluent.confluent.schema_registry_client import SchemaField, TopicSchema
from openlineage_confluent.mapping.facets import KafkaTopicDatasetFacet
from openlineage_confluent.mapping.mapper import ConfluentOpenLineageMapper


@pytest.fixture()
def mapper(confluent_cfg, ol_cfg) -> ConfluentOpenLineageMapper:
    return ConfluentOpenLineageMapper(confluent_cfg, ol_cfg)


# ── Basic counts ───────────────────────────────────────────────────────────────

def test_mapper_produces_one_event_per_job(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = mapper.map_all(sample_lineage_graph)
    # 8 distinct jobs: managed source, flink, managed sink, kafka producer,
    # consumer group, ksqlDB query, self-managed source
    # Consumer group has 2 edges (same job key) → unique jobs = 7
    assert len(events) == 7


def test_all_events_are_complete(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = mapper.map_all(sample_lineage_graph)
    assert all(e.eventType == RunState.COMPLETE for e in events)


# ── Managed Connect ────────────────────────────────────────────────────────────

def test_source_connector_has_no_inputs(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    event = events["postgres-orders-source"]
    assert not event.inputs


def test_source_connector_has_output_topic(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    event = events["postgres-orders-source"]
    assert event.outputs
    assert event.outputs[0].name == "ol-raw-orders"


def test_sink_connector_has_no_outputs(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    event = events["http-sink"]
    assert not event.outputs


def test_sink_connector_has_input_topic(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    event = events["http-sink"]
    assert event.inputs
    assert event.inputs[0].name == "ol-orders-enriched"


def test_managed_connect_namespace(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    assert events["http-sink"].job.namespace == "kafka-connect://env-abc123"


# ── Flink ──────────────────────────────────────────────────────────────────────

def test_flink_job_namespace(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    event = events["ol-enrich-orders"]
    assert event.job.namespace == "flink://env-abc123"


def test_flink_job_inputs_and_outputs(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    event = events["ol-enrich-orders"]
    assert [d.name for d in (event.inputs or [])] == ["ol-raw-orders"]
    assert [d.name for d in (event.outputs or [])] == ["ol-orders-enriched"]


# ── Kafka producers ───────────────────────────────────────────────────────────

def test_producer_namespace(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    event = events["order-service-producer"]
    assert event.job.namespace == "kafka-producer://lkc-abc123"


def test_producer_job_type_facet(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    event = events["order-service-producer"]
    jt = event.job.facets["jobType"]
    assert jt.processingType == "STREAMING"
    assert jt.integration == "KAFKA"
    assert jt.jobType == "PRODUCER"


def test_producer_has_output_topic_and_no_inputs(
    mapper, sample_lineage_graph: LineageGraph
) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    event = events["order-service-producer"]
    assert not event.inputs
    assert event.outputs
    assert event.outputs[0].name == "ol-raw-orders"


# ── Consumer groups ────────────────────────────────────────────────────────────

def test_consumer_group_namespace(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    event = events["order-processor-service"]
    assert event.job.namespace == "kafka-consumer-group://lkc-abc123"


def test_consumer_group_has_inputs_not_outputs(mapper, sample_lineage_graph: LineageGraph) -> None:
    """Consumer groups read topics but produce nothing — inputs only."""
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    event = events["order-processor-service"]
    assert event.inputs
    assert not event.outputs


def test_consumer_group_inputs_are_all_subscribed_topics(
    mapper, sample_lineage_graph: LineageGraph
) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    event = events["order-processor-service"]
    input_names = {d.name for d in (event.inputs or [])}
    assert input_names == {"ol-orders-enriched", "ol-raw-orders"}


# ── ksqlDB ─────────────────────────────────────────────────────────────────────

def test_ksqldb_query_namespace(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    event = events["CSAS_HIGH_VALUE_STREAM_0"]
    assert event.job.namespace == "ksqldb://lksqlc-test123"


def test_ksqldb_query_has_inputs_and_outputs(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    event = events["CSAS_HIGH_VALUE_STREAM_0"]
    input_names  = [d.name for d in (event.inputs  or [])]
    output_names = [d.name for d in (event.outputs or [])]
    assert "ol-orders-enriched" in input_names
    assert "HIGH_VALUE_STREAM"  in output_names


# ── Self-managed Connect ───────────────────────────────────────────────────────

def test_self_managed_connect_namespace(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    event = events["jdbc-source"]
    assert event.job.namespace == "kafka-connect://on-prem-connect"


def test_self_managed_connector_has_output_topic(
    mapper, sample_lineage_graph: LineageGraph
) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    event = events["jdbc-source"]
    assert event.outputs
    assert event.outputs[0].name == "jdbc-orders"


# ── Dataset namespace ──────────────────────────────────────────────────────────

def test_dataset_namespace_is_kafka(mapper, sample_lineage_graph: LineageGraph) -> None:
    bootstrap = "pkc-abc123.us-east-2.aws.confluent.cloud:9092"
    events = mapper.map_all(sample_lineage_graph)
    for e in events:
        for ds in list(e.inputs or []) + list(e.outputs or []):
            assert ds.namespace == f"kafka://{bootstrap}"


# ── Determinism ────────────────────────────────────────────────────────────────

def test_stable_run_id_is_deterministic(mapper, sample_lineage_graph: LineageGraph) -> None:
    ids_a = {e.job.name: e.run.runId for e in mapper.map_all(sample_lineage_graph, cycle_key="c1")}
    ids_b = {e.job.name: e.run.runId for e in mapper.map_all(sample_lineage_graph, cycle_key="c1")}
    assert ids_a == ids_b


def test_different_cycles_produce_different_run_ids(
    mapper, sample_lineage_graph: LineageGraph
) -> None:
    events_a = mapper.map_all(sample_lineage_graph, cycle_key="cycle-1")
    events_b = mapper.map_all(sample_lineage_graph, cycle_key="cycle-2")
    for ea, eb in zip(events_a, events_b):
        assert ea.run.runId != eb.run.runId


# ── SchemaDatasetFacet ─────────────────────────────────────────────────────────

def test_topic_without_schema_has_no_facet(
    mapper, sample_lineage_graph: LineageGraph
) -> None:
    """Topics not present in topic_schemas get no facets attached."""
    events = mapper.map_all(sample_lineage_graph)
    for e in events:
        for ds in list(e.inputs or []) + list(e.outputs or []):
            assert ds.facets is None or "schema" not in (ds.facets or {})


def test_schema_facet_attached_to_input_and_output_topics(
    mapper, sample_lineage_graph: LineageGraph
) -> None:
    sample_lineage_graph.topic_schemas = {
        "ol-raw-orders": TopicSchema(
            topic="ol-raw-orders",
            value_subject="ol-raw-orders-value",
            value_schema_type="AVRO",
            value_fields=[SchemaField(name="id", type="string")],
        ),
        "ol-orders-enriched": TopicSchema(
            topic="ol-orders-enriched",
            value_subject="ol-orders-enriched-value",
            value_schema_type="AVRO",
            value_fields=[SchemaField(name="enriched", type="boolean")],
        ),
    }
    events = mapper.map_all(sample_lineage_graph)
    flink_event = next(e for e in events if e.job.name == "ol-enrich-orders")

    in_facets  = (flink_event.inputs[0].facets  or {})
    out_facets = (flink_event.outputs[0].facets or {})
    assert "schema" in in_facets
    assert "schema" in out_facets
    assert in_facets["schema"].fields[0].name == "value.id"
    assert out_facets["schema"].fields[0].name == "value.enriched"


def test_schema_facet_includes_key_and_value_with_prefixes(
    mapper, sample_lineage_graph: LineageGraph
) -> None:
    sample_lineage_graph.topic_schemas = {
        "ol-raw-orders": TopicSchema(
            topic="ol-raw-orders",
            key_subject="ol-raw-orders-key",
            value_subject="ol-raw-orders-value",
            key_schema_type="AVRO",
            value_schema_type="AVRO",
            key_fields=[SchemaField(name="order_id", type="string")],
            value_fields=[SchemaField(name="amount", type="double")],
        ),
    }
    events = mapper.map_all(sample_lineage_graph)
    src_event = next(e for e in events if e.job.name == "postgres-orders-source")
    facet = src_event.outputs[0].facets["schema"]

    names = [f.name for f in facet.fields]
    assert "key.order_id" in names
    assert "value.amount" in names


def test_schema_facet_carries_nested_record_fields(
    mapper, sample_lineage_graph: LineageGraph
) -> None:
    sample_lineage_graph.topic_schemas = {
        "ol-raw-orders": TopicSchema(
            topic="ol-raw-orders",
            value_subject="ol-raw-orders-value",
            value_schema_type="AVRO",
            value_fields=[SchemaField(
                name="customer", type="record<Customer>",
                fields=[SchemaField(name="email", type="string")],
            )],
        ),
    }
    events = mapper.map_all(sample_lineage_graph)
    src_event = next(e for e in events if e.job.name == "postgres-orders-source")
    facet = src_event.outputs[0].facets["schema"]

    customer = next(f for f in facet.fields if f.name == "value.customer")
    assert customer.fields is not None
    assert customer.fields[0].name == "email"


# ── KafkaTopicDatasetFacet ────────────────────────────────────────────────────

def test_kafka_topic_facet_attached_when_metadata_present(
    mapper, sample_lineage_graph: LineageGraph,
) -> None:
    sample_lineage_graph.topic_metadata = {
        "ol-raw-orders": TopicMetadata(
            topic="ol-raw-orders", partitions=6, replication_factor=3,
        ),
        "ol-orders-enriched": TopicMetadata(
            topic="ol-orders-enriched", partitions=12, replication_factor=3,
        ),
    }
    events = mapper.map_all(sample_lineage_graph)
    flink_event = next(e for e in events if e.job.name == "ol-enrich-orders")

    in_facets  = flink_event.inputs[0].facets  or {}
    out_facets = flink_event.outputs[0].facets or {}
    assert "kafkaTopic" in in_facets
    assert "kafkaTopic" in out_facets
    assert in_facets["kafkaTopic"].partitions == 6
    assert out_facets["kafkaTopic"].partitions == 12
    assert out_facets["kafkaTopic"].replicationFactor == 3


def test_kafka_topic_facet_omitted_when_no_metadata(
    mapper, sample_lineage_graph: LineageGraph,
) -> None:
    events = mapper.map_all(sample_lineage_graph)
    for e in events:
        for ds in list(e.inputs or []) + list(e.outputs or []):
            assert ds.facets is None or "kafkaTopic" not in (ds.facets or {})


def test_kafka_topic_facet_marks_internal_topics(
    mapper, sample_lineage_graph: LineageGraph,
) -> None:
    sample_lineage_graph.topic_metadata = {
        "ol-raw-orders": TopicMetadata(
            topic="ol-raw-orders", partitions=6, replication_factor=3,
            is_internal=True,
        ),
    }
    events = mapper.map_all(sample_lineage_graph)
    src_event = next(e for e in events if e.job.name == "postgres-orders-source")
    assert src_event.outputs[0].facets["kafkaTopic"].isInternal is True


def test_kafka_topic_and_schema_facets_coexist(
    mapper, sample_lineage_graph: LineageGraph,
) -> None:
    """Both facets should be attached when both data sources are populated."""
    sample_lineage_graph.topic_schemas = {
        "ol-raw-orders": TopicSchema(
            topic="ol-raw-orders",
            value_subject="ol-raw-orders-value", value_schema_type="AVRO",
            value_fields=[SchemaField(name="id", type="string")],
        ),
    }
    sample_lineage_graph.topic_metadata = {
        "ol-raw-orders": TopicMetadata(
            topic="ol-raw-orders", partitions=6, replication_factor=3,
        ),
    }
    events = mapper.map_all(sample_lineage_graph)
    src_event = next(e for e in events if e.job.name == "postgres-orders-source")
    facets = src_event.outputs[0].facets
    assert set(facets) >= {"schema", "kafkaTopic"}


def test_custom_kafka_topic_facet_directly_constructible() -> None:
    facet = KafkaTopicDatasetFacet(partitions=3, replicationFactor=3, isInternal=False)
    assert facet.partitions == 3
    assert facet.replicationFactor == 3
    assert facet.isInternal is False

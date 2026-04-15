"""Tests for ConfluentOpenLineageMapper."""

from __future__ import annotations

import pytest

from openlineage.client.event_v2 import RunState

from openlineage_confluent.confluent.models import LineageGraph
from openlineage_confluent.mapping.mapper import ConfluentOpenLineageMapper


@pytest.fixture()
def mapper(confluent_cfg, ol_cfg) -> ConfluentOpenLineageMapper:
    return ConfluentOpenLineageMapper(confluent_cfg, ol_cfg)


# ── Basic counts ───────────────────────────────────────────────────────────────

def test_mapper_produces_one_event_per_job(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = mapper.map_all(sample_lineage_graph)
    # 7 distinct jobs: managed source, flink, managed sink, consumer group,
    # ksqlDB query, self-managed source  → but consumer group has 2 edges (same job key)
    # so unique jobs = 6
    assert len(events) == 6


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

"""Tests for ConfluentOpenLineageMapper."""

from __future__ import annotations

import pytest

from openlineage.client.event_v2 import RunState

from openlineage_confluent.confluent.models import LineageGraph
from openlineage_confluent.mapping.mapper import ConfluentOpenLineageMapper


@pytest.fixture()
def mapper(confluent_cfg, ol_cfg) -> ConfluentOpenLineageMapper:
    return ConfluentOpenLineageMapper(confluent_cfg, ol_cfg)


def test_mapper_produces_one_event_per_job(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = mapper.map_all(sample_lineage_graph)
    # 3 distinct jobs: source connector, flink stmt, sink connector
    assert len(events) == 3


def test_all_events_are_complete(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = mapper.map_all(sample_lineage_graph)
    assert all(e.eventType == RunState.COMPLETE for e in events)


def test_source_connector_has_no_inputs(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    source_event = events["postgres-orders-source"]
    # Source connector: external → topic (no kafka input datasets)
    assert not source_event.inputs


def test_source_connector_has_output_topic(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    source_event = events["postgres-orders-source"]
    assert source_event.outputs
    assert source_event.outputs[0].name == "ol-raw-orders"


def test_flink_job_namespace(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    flink_event = events["ol-enrich-orders"]
    assert flink_event.job.namespace == "flink://env-abc123"


def test_flink_job_inputs_and_outputs(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    flink_event = events["ol-enrich-orders"]
    input_names  = [d.name for d in (flink_event.inputs  or [])]
    output_names = [d.name for d in (flink_event.outputs or [])]
    assert "ol-raw-orders" in input_names
    assert "ol-orders-enriched" in output_names


def test_sink_connector_namespace(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    sink_event = events["http-sink"]
    assert sink_event.job.namespace == "kafka-connect://env-abc123"


def test_sink_connector_has_no_outputs(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = {e.job.name: e for e in mapper.map_all(sample_lineage_graph)}
    sink_event = events["http-sink"]
    # Sink connector: topic → external (no kafka output datasets)
    assert not sink_event.outputs


def test_dataset_namespace_is_kafka(mapper, sample_lineage_graph: LineageGraph) -> None:
    events = mapper.map_all(sample_lineage_graph)
    bootstrap = "pkc-abc123.us-east-2.aws.confluent.cloud:9092"
    for e in events:
        for ds in list(e.inputs or []) + list(e.outputs or []):
            assert ds.namespace == f"kafka://{bootstrap}"


def test_stable_run_id_is_deterministic(mapper, sample_lineage_graph: LineageGraph) -> None:
    events_a = mapper.map_all(sample_lineage_graph, cycle_key="cycle-1")
    events_b = mapper.map_all(sample_lineage_graph, cycle_key="cycle-1")
    ids_a = {e.job.name: e.run.runId for e in events_a}
    ids_b = {e.job.name: e.run.runId for e in events_b}
    assert ids_a == ids_b


def test_different_cycles_produce_different_run_ids(mapper, sample_lineage_graph: LineageGraph) -> None:
    events_a = mapper.map_all(sample_lineage_graph, cycle_key="cycle-1")
    events_b = mapper.map_all(sample_lineage_graph, cycle_key="cycle-2")
    for ea, eb in zip(events_a, events_b):
        assert ea.run.runId != eb.run.runId

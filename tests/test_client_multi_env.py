"""End-to-end test: ConfluentLineageClient fans out across multiple envs."""

from __future__ import annotations

from unittest.mock import patch

from openlineage_confluent.confluent.client import ConfluentLineageClient
from openlineage_confluent.confluent.models import (
    ConsumerGroupInfo,
    FlinkStatement,
    KafkaProducerInfo,
    LineageGraph,
    TableflowTopic,
)


def test_get_lineage_graph_merges_per_env_results(multi_env_cfg) -> None:
    """Two envs each contribute one Flink statement; merged graph carries both,
    each with its own kafka_bootstrap stamped on the edges."""
    client = ConfluentLineageClient(multi_env_cfg)
    try:
        # Stub each per-env sub-client to return a tiny canned graph.
        env_a, env_b = client._envs

        def _stub_env_a():
            return LineageGraph(edges=[], statements=[
                FlinkStatement(name="stmt-a", statement="x", status="RUNNING")
            ])

        def _stub_env_b():
            return LineageGraph(edges=[], statements=[
                FlinkStatement(name="stmt-b", statement="x", status="RUNNING")
            ])

        with patch.object(env_a, "build_graph", side_effect=_stub_env_a), \
             patch.object(env_b, "build_graph", side_effect=_stub_env_b):
            graph = client.get_lineage_graph()
    finally:
        client.close()

    names = sorted(s.name for s in graph.statements)
    assert names == ["stmt-a", "stmt-b"]


def test_per_env_edges_carry_their_own_bootstrap(multi_env_cfg) -> None:
    """Edges produced by env-a should carry env-a's bootstrap, env-b similarly."""
    client = ConfluentLineageClient(multi_env_cfg)
    try:
        env_a, env_b = client._envs

        # Real build_graph for both envs, but stub their inputs so we know what to expect.
        def _empty(*_args, **_kwargs):
            return []

        def _empty_dict(*_args, **_kwargs):
            return {}

        # env-a: one consumer group on cg-a
        with patch.object(env_a, "list_connectors", side_effect=_empty), \
             patch.object(env_a, "list_flink_statements", side_effect=_empty), \
             patch.object(env_a._metrics, "get_producers", side_effect=_empty), \
             patch.object(env_a._metrics, "get_consumer_groups",
                          side_effect=lambda: [ConsumerGroupInfo(group_id="cg-a", topics=["t-a"])]), \
             patch.object(env_a._metrics, "get_topic_throughput", side_effect=_empty_dict), \
             patch.object(env_a._tableflow, "list_topics", side_effect=_empty), \
             patch.object(env_b, "list_connectors", side_effect=_empty), \
             patch.object(env_b, "list_flink_statements", side_effect=_empty), \
             patch.object(env_b._metrics, "get_producers",
                          side_effect=lambda: [KafkaProducerInfo(client_id="prod-b", topics=["t-b"])]), \
             patch.object(env_b._metrics, "get_consumer_groups", side_effect=_empty), \
             patch.object(env_b._metrics, "get_topic_throughput", side_effect=_empty_dict), \
             patch.object(env_b._tableflow, "list_topics", side_effect=_empty):
            graph = client.get_lineage_graph()
    finally:
        client.close()

    by_job = {e.job_name: e for e in graph.edges}
    assert "cg-a" in by_job
    assert "prod-b" in by_job
    assert by_job["cg-a"].kafka_bootstrap == "pkc-aaa.us-west-2.aws.confluent.cloud:9092"
    assert by_job["prod-b"].kafka_bootstrap == "pkc-bbb.eu-central-1.aws.confluent.cloud:9092"
    # Namespaces use cluster ids (per-env)
    assert by_job["cg-a"].job_namespace_hint == "kafka-consumer-group://lkc-aaa"
    assert by_job["prod-b"].job_namespace_hint == "kafka-producer://lkc-bbb"


def test_tableflow_lineage_uses_env_bootstrap(multi_env_cfg) -> None:
    client = ConfluentLineageClient(multi_env_cfg)
    try:
        env_a, env_b = client._envs

        def _empty(*_args, **_kwargs):
            return []

        def _empty_dict(*_args, **_kwargs):
            return {}

        with patch.object(env_a, "list_connectors", side_effect=_empty), \
             patch.object(env_a, "list_flink_statements", side_effect=_empty), \
             patch.object(env_a._metrics, "get_producers", side_effect=_empty), \
             patch.object(env_a._metrics, "get_consumer_groups", side_effect=_empty), \
             patch.object(env_a._metrics, "get_topic_throughput", side_effect=_empty_dict), \
             patch.object(env_a._tableflow, "list_topics",
                          side_effect=lambda: [TableflowTopic(topic_name="orders", status="RUNNING")]), \
             patch.object(env_b, "list_connectors", side_effect=_empty), \
             patch.object(env_b, "list_flink_statements", side_effect=_empty), \
             patch.object(env_b._metrics, "get_producers", side_effect=_empty), \
             patch.object(env_b._metrics, "get_consumer_groups", side_effect=_empty), \
             patch.object(env_b._metrics, "get_topic_throughput", side_effect=_empty_dict), \
             patch.object(env_b._tableflow, "list_topics", side_effect=_empty):
            graph = client.get_lineage_graph()
    finally:
        client.close()

    tf_edges = [e for e in graph.edges if e.job_type == "tableflow"]
    assert len(tf_edges) == 1
    assert tf_edges[0].job_namespace_hint == "tableflow://env-aaa"
    assert tf_edges[0].kafka_bootstrap == "pkc-aaa.us-west-2.aws.confluent.cloud:9092"

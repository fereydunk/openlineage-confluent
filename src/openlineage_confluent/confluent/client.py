"""Confluent Cloud lineage client — assembles a LineageGraph from all sources.

Multi-environment topology
──────────────────────────
The bridge is configured with a list of EnvDeployment entries (cfg.environments).
For every env, a per-env sub-client (`_EnvLineageClient`) runs the env-scoped
fan-out: managed Connect, Flink, Metrics API (producers + consumer groups +
topic throughput), Tableflow, optional Schema Registry, optional Kafka REST.
The top-level ConfluentLineageClient runs every env's fan-out concurrently
and merges the resulting LineageGraphs.

Globally-configured (NOT env-scoped) sources also folded into the merged graph:
  - ksqlDB persistent queries     (cfg.ksql_clusters — each carries its own creds)
  - Self-managed Connect clusters (cfg.self_managed_connect_clusters)

Auth
────
Cloud-level API key (api.confluent.cloud) is shared across every per-env
sub-client via a single httpx.Client. Per-env sub-clients carry their own
SR/Kafka REST keys when configured.
"""

from __future__ import annotations

import json
import logging
import subprocess
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx

from openlineage_confluent.config import ConfluentConfig, EnvDeployment
from openlineage_confluent.confluent.ksql_client import KsqlDbClient
from openlineage_confluent.confluent.metrics_client import MetricsApiClient
from openlineage_confluent.confluent.models import (
    ConnectorInfo,
    ConnectorType,
    ConsumerGroupInfo,
    FlinkStatement,
    KafkaProducerInfo,
    KsqlQuery,
    LineageEdge,
    LineageGraph,
    TableflowTopic,
    TopicThroughput,
)
from openlineage_confluent.confluent.kafka_rest_client import (
    KafkaRestClient,
    TopicMetadata,
)
from openlineage_confluent.confluent.schema_registry_client import (
    SchemaRegistryClient,
    TopicSchema,
)
from openlineage_confluent.confluent.self_managed_connect_client import SelfManagedConnectClient
from openlineage_confluent.confluent.tableflow_client import TableflowClient
from openlineage_confluent.confluent.sql_parser import parse_statement

log = logging.getLogger(__name__)

_CLOUD_API = "https://api.confluent.cloud"


def _flink_region_args(kafka_bootstrap: str) -> list[str]:
    """Cloud/region flags required by `confluent flink statement` commands.

    The CLI errors with "no cloud provider and region selected" unless these
    flags are passed (or the user has set a context with `confluent flink
    region use`). Parses both from the Kafka bootstrap host:
      pkc-XXXXXX.<region>.<cloud>.confluent.cloud:<port>
      e.g. pkc-921jm.us-east-2.aws.confluent.cloud:9092 → aws / us-east-2

    Returns [] when the bootstrap doesn't match the expected shape, in which
    case the CLI falls back to whatever region the user's context has set.
    """
    host = (kafka_bootstrap or "").split(":", 1)[0]
    parts = host.split(".")
    if len(parts) >= 5 and parts[-2] == "confluent" and parts[-1] == "cloud":
        return ["--cloud", parts[-3], "--region", parts[-4]]
    return []


class _EnvLineageClient:
    """Per-environment lineage fan-out: Connect/Flink/Metrics/Tableflow/SR/Kafka REST."""

    def __init__(
        self,
        cfg: ConfluentConfig,
        env: EnvDeployment,
        http: httpx.Client,
        *,
        timeout: float = 30.0,
    ) -> None:
        self._cfg = cfg
        self._env = env
        self._http = http       # shared cloud-API httpx client
        self._metrics = MetricsApiClient(cfg, env.cluster_id, timeout=timeout)
        self._tableflow = TableflowClient(env.env_id, env.cluster_id)

        self._sr_client: SchemaRegistryClient | None = None
        if env.schema_registry is not None:
            self._sr_client = SchemaRegistryClient(
                endpoint=env.schema_registry.endpoint,
                api_key=env.schema_registry.api_key,
                api_secret=env.schema_registry.api_secret.get_secret_value(),
                timeout=timeout,
            )

        self._kafka_rest: KafkaRestClient | None = None
        if env.kafka_rest_endpoint and env.kafka_api_key and env.kafka_api_secret:
            self._kafka_rest = KafkaRestClient(
                endpoint=env.kafka_rest_endpoint,
                cluster_id=env.cluster_id,
                api_key=env.kafka_api_key,
                api_secret=env.kafka_api_secret.get_secret_value(),
                timeout=timeout,
            )

        # Per-source health flags. Set True after a successful fetch (incl.
        # zero rows), False on HTTP/CLI error. Read by build_graph() to
        # populate LineageGraph.failed_namespaces — empty results from a
        # failed fetch must not be interpreted as "the resources were removed."
        self._connect_ok: bool = True
        self._flink_ok:   bool = True

    # ──────────────────────────────────────────────────────────────────────────
    # Managed Connect
    # ──────────────────────────────────────────────────────────────────────────

    def list_connectors(self) -> list[ConnectorInfo]:
        env     = self._env.env_id
        cluster = self._env.cluster_id
        self._connect_ok = True
        try:
            resp = self._http.get(
                f"/connect/v1/environments/{env}/clusters/{cluster}/connectors",
                params={"expand": "info,status"},
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            self._connect_ok = False
            log.warning("Connect API fetch failed for env=%s: %s", env, exc)
            return []
        raw: dict[str, Any] = resp.json()

        connectors: list[ConnectorInfo] = []
        for name, data in raw.items():
            info        = data.get("info", {})
            status_data = data.get("status", {})
            state       = status_data.get("connector", {}).get("state", "UNKNOWN")
            config      = info.get("config", {})
            c_type      = info.get("type", "source").lower()

            connectors.append(ConnectorInfo(
                id=f"{cluster}:{name}",
                name=name,
                status=state,
                type=c_type,
                config=config,
                # connect_cluster=None → managed; namespace keyed on env_id
            ))
        log.debug("env=%s: %d managed connectors", env, len(connectors))
        return connectors

    # ──────────────────────────────────────────────────────────────────────────
    # Flink statements (via Confluent CLI)
    # ──────────────────────────────────────────────────────────────────────────

    def list_flink_statements(self) -> list[FlinkStatement]:
        env = self._env.env_id
        self._flink_ok = True
        try:
            result = subprocess.run(
                ["confluent", "flink", "statement", "list",
                 "--environment", env, *_flink_region_args(self._env.kafka_bootstrap),
                 "-o", "json"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode != 0:
                self._flink_ok = False
                log.warning("confluent flink statement list (env=%s) failed: %s",
                            env, result.stderr[:300])
                return []
            raw: list[dict[str, Any]] = json.loads(result.stdout)
        except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as exc:
            self._flink_ok = False
            log.warning("Failed to list Flink statements for env=%s: %s", env, exc)
            return []

        statements: list[FlinkStatement] = []
        for item in raw:
            name   = item.get("name", "")
            sql    = item.get("statement", "")
            status = item.get("status", "UNKNOWN")
            pool   = item.get("compute_pool", "")
            if name and sql:
                statements.append(FlinkStatement(
                    name=name,
                    statement=sql,
                    status=status,
                    compute_pool=pool,
                ))
        log.debug("env=%s: %d Flink statements", env, len(statements))
        return statements

    # ──────────────────────────────────────────────────────────────────────────
    # Build per-env lineage graph
    # ──────────────────────────────────────────────────────────────────────────

    def build_graph(self) -> LineageGraph:
        env       = self._env.env_id
        cluster   = self._env.cluster_id
        bootstrap = self._env.kafka_bootstrap

        n_workers = 6
        futures: dict[str, Any] = {}

        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures["connectors"]       = pool.submit(self.list_connectors)
            futures["statements"]       = pool.submit(self.list_flink_statements)
            futures["producers"]        = pool.submit(self._metrics.get_producers)
            futures["consumer_groups"]  = pool.submit(self._metrics.get_consumer_groups)
            futures["topic_throughput"] = pool.submit(self._metrics.get_topic_throughput)
            futures["tableflow"]        = pool.submit(self._tableflow.list_topics)

            if self._kafka_rest is not None:
                futures["topic_metadata"] = pool.submit(
                    self._kafka_rest.get_topic_metadata
                )

        managed_connectors: list[ConnectorInfo] = futures["connectors"].result()
        statements: list[FlinkStatement]         = futures["statements"].result()
        producers: list[KafkaProducerInfo]       = futures["producers"].result()
        consumer_groups: list[ConsumerGroupInfo] = futures["consumer_groups"].result()
        tableflow_topics: list[TableflowTopic]   = futures["tableflow"].result()

        edges: list[LineageEdge] = []

        # ── 1. Managed connectors ────────────────────────────────────────────
        for conn in managed_connectors:
            ns = f"kafka-connect://{env}"
            if conn.connector_type == ConnectorType.SOURCE:
                for topic in conn.topics_produced():
                    edges.append(LineageEdge(
                        source_name=conn.name,
                        source_type="external",
                        target_name=topic,
                        target_type="kafka_topic",
                        job_name=conn.name,
                        job_type="kafka_connect_source",
                        job_namespace_hint=ns,
                        kafka_bootstrap=bootstrap,
                    ))
            else:  # SINK
                for topic in conn.topics_consumed():
                    edges.append(LineageEdge(
                        source_name=topic,
                        source_type="kafka_topic",
                        target_name=conn.name,
                        target_type="external",
                        job_name=conn.name,
                        job_type="kafka_connect_sink",
                        job_namespace_hint=ns,
                        kafka_bootstrap=bootstrap,
                    ))

        # ── 2. Flink statements ──────────────────────────────────────────────
        for stmt in statements:
            if not stmt.is_running():
                continue
            inputs, outputs = parse_statement(stmt.sql)
            if not inputs and not outputs:
                continue
            for inp in inputs:
                for out in outputs:
                    edges.append(LineageEdge(
                        source_name=inp,
                        source_type="kafka_topic",
                        target_name=out,
                        target_type="kafka_topic",
                        job_name=stmt.name,
                        job_type="flink_statement",
                        job_namespace_hint=f"flink://{env}",
                        kafka_bootstrap=bootstrap,
                    ))

        # ── 3. Kafka producers (Metrics API received_bytes + client_id) ──────
        for producer in producers:
            for topic in producer.topics:
                edges.append(LineageEdge(
                    source_name=producer.client_id,
                    source_type="external",
                    target_name=topic,
                    target_type="kafka_topic",
                    job_name=producer.client_id,
                    job_type="kafka_producer",
                    job_namespace_hint=f"kafka-producer://{cluster}",
                    kafka_bootstrap=bootstrap,
                ))

        # ── 4. Consumer groups (Metrics API) ─────────────────────────────────
        for group in consumer_groups:
            for topic in group.topics:
                edges.append(LineageEdge(
                    source_name=topic,
                    source_type="kafka_topic",
                    target_name=group.group_id,
                    target_type="consumer_group",
                    job_name=group.group_id,
                    job_type="kafka_consumer_group",
                    job_namespace_hint=f"kafka-consumer-group://{cluster}",
                    kafka_bootstrap=bootstrap,
                ))

        # ── 5. Tableflow: Kafka topic → Iceberg dataset ──────────────────────
        glue_region = self._cfg.tableflow_glue_region
        glue_db     = self._cfg.tableflow_glue_database
        for tf_topic in tableflow_topics:
            if glue_region and glue_db:
                iceberg_ns   = f"glue://{glue_region}"
                iceberg_name = f"{glue_db}.{tf_topic.iceberg_table_name}"
            else:
                iceberg_ns   = "iceberg://tableflow"
                iceberg_name = tf_topic.iceberg_table_name
            edges.append(LineageEdge(
                source_name=tf_topic.topic_name,
                source_type="kafka_topic",
                target_name=iceberg_name,
                target_type="iceberg_table",
                target_namespace=iceberg_ns,
                job_name=tf_topic.topic_name,
                job_type="tableflow",
                job_namespace_hint=f"tableflow://{env}",
                kafka_bootstrap=bootstrap,
            ))

        # ── 6. Schema Registry: schemas for every topic in graph ─────────────
        topic_schemas: dict[str, TopicSchema] = {}
        if self._sr_client is not None:
            topics = sorted({
                ep for e in edges
                for ep in (
                    (e.source_name,) if e.source_type == "kafka_topic" else ()
                ) + (
                    (e.target_name,) if e.target_type == "kafka_topic" else ()
                )
            })
            try:
                topic_schemas = self._sr_client.get_topic_schemas(topics)
            except Exception as exc:    # noqa: BLE001
                log.warning("Schema Registry fetch failed for env=%s: %s", env, exc)

        # ── 7. Kafka REST: per-topic partition / replication metadata ────────
        topic_metadata: dict[str, TopicMetadata] = {}
        if self._kafka_rest is not None:
            try:
                topic_metadata = futures["topic_metadata"].result()
            except Exception as exc:    # noqa: BLE001
                log.warning("Kafka REST fetch failed for env=%s: %s", env, exc)

        # ── 8. Per-topic throughput ──────────────────────────────────────────
        topic_throughput: dict[str, TopicThroughput] = {}
        try:
            topic_throughput = futures["topic_throughput"].result()
        except Exception as exc:    # noqa: BLE001
            log.warning("Topic throughput fetch failed for env=%s: %s", env, exc)

        # Build the failed-namespace set: a fetch failure means we couldn't
        # observe that source's resources this cycle, so the emitter must NOT
        # interpret their absence from `edges` as a removal. The namespaces
        # below are the OL job-namespace prefixes the mapper would assign.
        failed_namespaces: set[str] = set()
        if not self._connect_ok:
            failed_namespaces.add(f"kafka-connect://{env}")
        if not self._flink_ok:
            failed_namespaces.add(f"flink://{env}")
        if not self._metrics.producers_ok:
            failed_namespaces.add(f"kafka-producer://{cluster}")
        if not self._metrics.consumer_groups_ok:
            failed_namespaces.add(f"kafka-consumer-group://{cluster}")
        if not self._tableflow.last_ok:
            failed_namespaces.add(f"tableflow://{env}")
        # Throughput failures only affect dataset-level facets, not jobs —
        # not added to failed_namespaces (no jobs to suppress removal for).

        log.info(
            "env=%s: %d edges (managed=%d flink=%d producers=%d cg=%d tf=%d) failed_sources=%s",
            env, len(edges),
            len(managed_connectors), len(statements),
            len(producers), len(consumer_groups), len(tableflow_topics),
            sorted(failed_namespaces) or "[]",
        )

        return LineageGraph(
            edges=edges,
            connectors=managed_connectors,
            statements=statements,
            producers=producers,
            consumer_groups=consumer_groups,
            tableflow_topics=tableflow_topics,
            topic_schemas=topic_schemas,
            topic_metadata=topic_metadata,
            topic_throughput=topic_throughput,
            failed_namespaces=failed_namespaces,
        )

    def close(self) -> None:
        self._metrics.close()
        if self._sr_client is not None:
            self._sr_client.close()
        if self._kafka_rest is not None:
            self._kafka_rest.close()


class ConfluentLineageClient:
    """Top-level lineage client. Fans out across all envs + global ksqlDB / self-managed Connect."""

    def __init__(self, cfg: ConfluentConfig, *, timeout: float = 30.0) -> None:
        self._cfg = cfg
        auth = (cfg.cloud_api_key, cfg.cloud_api_secret.get_secret_value())
        self._http = httpx.Client(
            base_url=_CLOUD_API,
            auth=auth,
            timeout=timeout,
            headers={"Accept": "application/json"},
        )

        self._envs: list[_EnvLineageClient] = [
            _EnvLineageClient(cfg, env, self._http, timeout=timeout)
            for env in cfg.environments
        ]

        # ksqlDB clusters and self-managed Connect clusters are configured at
        # the top level (each entry carries its own creds), not per-env.
        self._ksql_clients = [
            KsqlDbClient(cluster, timeout=timeout)
            for cluster in cfg.ksql_clusters
        ]
        self._sm_connect_clients = [
            SelfManagedConnectClient(cluster, timeout=timeout)
            for cluster in cfg.self_managed_connect_clusters
        ]

    # ──────────────────────────────────────────────────────────────────────────
    # Build merged lineage graph across all envs + global sources
    # ──────────────────────────────────────────────────────────────────────────

    def get_lineage_graph(self) -> LineageGraph:
        per_env_graphs: list[LineageGraph] = []
        if self._envs:
            with ThreadPoolExecutor(max_workers=max(len(self._envs), 1)) as pool:
                futures = [pool.submit(env.build_graph) for env in self._envs]
                for fut in futures:
                    try:
                        per_env_graphs.append(fut.result())
                    except Exception as exc:    # noqa: BLE001
                        log.exception("Per-env graph build failed: %s", exc)

        # ── Global: ksqlDB + self-managed Connect (queried in parallel) ──────
        ksql_queries: list[KsqlQuery]      = []
        sm_connectors: list[ConnectorInfo] = []
        n_global = len(self._ksql_clients) + len(self._sm_connect_clients)
        if n_global > 0:
            with ThreadPoolExecutor(max_workers=n_global) as pool:
                ksql_futs = [pool.submit(c.get_queries)    for c in self._ksql_clients]
                sm_futs   = [pool.submit(c.get_connectors) for c in self._sm_connect_clients]
                for fut in ksql_futs:
                    try:
                        ksql_queries.extend(fut.result())
                    except Exception as exc:    # noqa: BLE001
                        log.warning("ksqlDB fetch failed: %s", exc)
                for fut in sm_futs:
                    try:
                        sm_connectors.extend(fut.result())
                    except Exception as exc:    # noqa: BLE001
                        log.warning("Self-managed Connect fetch failed: %s", exc)

        # ── Build edges for global sources ───────────────────────────────────
        # Self-managed connectors keyed by their cluster label (NOT an env_id)
        global_edges: list[LineageEdge] = []
        # Self-managed connectors don't have a per-env Kafka bootstrap (they
        # may target self-hosted brokers). Leave kafka_bootstrap=None so the
        # mapper falls back to OpenLineageConfig.kafka_bootstrap.
        for conn in sm_connectors:
            ns = f"kafka-connect://{conn.connect_cluster}"
            if conn.connector_type == ConnectorType.SOURCE:
                for topic in conn.topics_produced():
                    global_edges.append(LineageEdge(
                        source_name=conn.name,
                        source_type="external",
                        target_name=topic,
                        target_type="kafka_topic",
                        job_name=conn.name,
                        job_type="kafka_connect_source",
                        job_namespace_hint=ns,
                    ))
            else:
                for topic in conn.topics_consumed():
                    global_edges.append(LineageEdge(
                        source_name=topic,
                        source_type="kafka_topic",
                        target_name=conn.name,
                        target_type="external",
                        job_name=conn.name,
                        job_type="kafka_connect_sink",
                        job_namespace_hint=ns,
                    ))

        # ksqlDB persistent queries
        for query in ksql_queries:
            if not query.is_running():
                continue
            if not query.source_topics or not query.sink_topics:
                continue
            for src in query.source_topics:
                for sink in query.sink_topics:
                    global_edges.append(LineageEdge(
                        source_name=src,
                        source_type="kafka_topic",
                        target_name=sink,
                        target_type="kafka_topic",
                        job_name=query.query_id,
                        job_type="ksqldb_query",
                        job_namespace_hint=f"ksqldb://{query.ksql_cluster_id}",
                    ))

        # ── Merge ────────────────────────────────────────────────────────────
        all_edges:           list[LineageEdge]           = list(global_edges)
        all_managed_conns:   list[ConnectorInfo]         = []
        all_statements:      list[FlinkStatement]        = []
        all_producers:       list[KafkaProducerInfo]     = []
        all_consumer_groups: list[ConsumerGroupInfo]     = []
        all_tableflow:       list[TableflowTopic]        = []
        topic_schemas:       dict[str, TopicSchema]      = {}
        topic_metadata:      dict[str, TopicMetadata]    = {}
        topic_throughput:    dict[str, TopicThroughput]  = {}

        failed_namespaces: set[str] = set()
        for g in per_env_graphs:
            all_edges.extend(g.edges)
            all_managed_conns.extend(g.connectors)
            all_statements.extend(g.statements)
            all_producers.extend(g.producers)
            all_consumer_groups.extend(g.consumer_groups)
            all_tableflow.extend(g.tableflow_topics)
            topic_schemas.update(g.topic_schemas)
            topic_metadata.update(g.topic_metadata)
            topic_throughput.update(g.topic_throughput)
            failed_namespaces.update(g.failed_namespaces)

        # Global sources: ksqlDB queries and self-managed Connect clusters.
        # Failures here also need to suppress removal-detection for jobs in
        # the corresponding namespaces.
        for c in self._ksql_clients:
            if not c.last_ok:
                failed_namespaces.add(f"ksqldb://{c.cluster_id}")
        for c in self._sm_connect_clients:
            if not c.last_ok:
                failed_namespaces.add(f"kafka-connect://{c.cluster_name}")

        all_connectors = all_managed_conns + sm_connectors

        log.info(
            "Merged graph across %d envs: %d edges (managed=%d self-managed=%d "
            "flink=%d producers=%d cg=%d ksqlDB=%d tableflow=%d) failed_sources=%s",
            len(self._envs), len(all_edges),
            len(all_managed_conns), len(sm_connectors),
            len(all_statements), len(all_producers),
            len(all_consumer_groups), len(ksql_queries), len(all_tableflow),
            sorted(failed_namespaces) or "[]",
        )
        if failed_namespaces:
            log.warning(
                "%d source(s) failed to fetch this cycle — removal-detection "
                "suppressed for: %s. Fix auth/network and the next cycle will "
                "resume normal diff-tracking.",
                len(failed_namespaces), sorted(failed_namespaces),
            )

        return LineageGraph(
            edges=all_edges,
            connectors=all_connectors,
            statements=all_statements,
            producers=all_producers,
            consumer_groups=all_consumer_groups,
            ksql_queries=ksql_queries,
            tableflow_topics=all_tableflow,
            topic_schemas=topic_schemas,
            topic_metadata=topic_metadata,
            topic_throughput=topic_throughput,
            failed_namespaces=failed_namespaces,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────────────────────────────────

    def close(self) -> None:
        self._http.close()
        for env in self._envs:
            env.close()
        for c in self._ksql_clients:
            c.close()
        for c in self._sm_connect_clients:
            c.close()

    def __enter__(self) -> "ConfluentLineageClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

"""Confluent Cloud lineage client — assembles a LineageGraph from all sources.

Sources fetched concurrently via ThreadPoolExecutor:
  1. Managed Connect API      → connector topic mappings
  2. Flink SQL (CLI)          → statement SQL parsed for tables
  3. Confluent Metrics API    → consumer_lag_offsets → consumer group topics
  4. ksqlDB REST API          → persistent query source/sink topic bindings
  5. Self-managed Connect     → connector configs from customer-hosted clusters

Auth for managed APIs: HTTP Basic with a Cloud-level API key.
Auth for ksqlDB / self-managed Connect: see KsqlClusterConfig / SelfManagedConnectClusterConfig.
"""

from __future__ import annotations

import json
import logging
import subprocess
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx

from openlineage_confluent.config import ConfluentConfig
from openlineage_confluent.confluent.ksql_client import KsqlDbClient
from openlineage_confluent.confluent.metrics_client import MetricsApiClient
from openlineage_confluent.confluent.models import (
    ConnectorInfo,
    ConnectorType,
    ConsumerGroupInfo,
    FlinkStatement,
    KsqlQuery,
    LineageEdge,
    LineageGraph,
)
from openlineage_confluent.confluent.self_managed_connect_client import SelfManagedConnectClient
from openlineage_confluent.confluent.sql_parser import parse_statement

log = logging.getLogger(__name__)

_CLOUD_API = "https://api.confluent.cloud"


class ConfluentLineageClient:
    """Builds a LineageGraph from all Confluent data-plane and control-plane sources."""

    def __init__(self, cfg: ConfluentConfig, *, timeout: float = 30.0) -> None:
        self._cfg = cfg
        auth = (cfg.cloud_api_key, cfg.cloud_api_secret.get_secret_value())
        self._http = httpx.Client(
            base_url=_CLOUD_API,
            auth=auth,
            timeout=timeout,
            headers={"Accept": "application/json"},
        )

        # Metrics API client — always created (falls back to cloud key)
        self._metrics = MetricsApiClient(cfg, timeout=timeout)

        # Per-cluster ksqlDB clients (empty if no clusters configured)
        self._ksql_clients = [
            KsqlDbClient(cluster, timeout=timeout)
            for cluster in cfg.ksql_clusters
        ]

        # Per-cluster self-managed Connect clients (empty if none configured)
        self._sm_connect_clients = [
            SelfManagedConnectClient(cluster, timeout=timeout)
            for cluster in cfg.self_managed_connect_clusters
        ]

    # ──────────────────────────────────────────────────────────────────────────
    # Managed Connect
    # ──────────────────────────────────────────────────────────────────────────

    def list_connectors(self) -> list[ConnectorInfo]:
        """Return all managed connectors for the configured cluster."""
        env     = self._cfg.environment_id
        cluster = self._cfg.cluster_id

        resp = self._http.get(
            f"/connect/v1/environments/{env}/clusters/{cluster}/connectors",
            params={"expand": "info,status"},
        )
        resp.raise_for_status()
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
            log.debug("Managed connector: %s  type=%s  state=%s", name, c_type, state)

        return connectors

    # ──────────────────────────────────────────────────────────────────────────
    # Flink statements (via Confluent CLI)
    # ──────────────────────────────────────────────────────────────────────────

    def list_flink_statements(self) -> list[FlinkStatement]:
        """Return all Flink SQL statements via the Confluent CLI.

        The Flink REST API requires user-level OAuth tokens unavailable via
        Cloud API keys. The CLI already holds valid credentials and emits JSON.
        """
        env = self._cfg.environment_id
        try:
            result = subprocess.run(
                ["confluent", "flink", "statement", "list",
                 "--environment", env, "-o", "json"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode != 0:
                log.warning("confluent flink statement list failed: %s", result.stderr[:300])
                return []
            raw: list[dict[str, Any]] = json.loads(result.stdout)
        except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as exc:
            log.warning("Failed to list Flink statements: %s", exc)
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
                log.debug("Flink statement: %s  status=%s", name, status)

        return statements

    # ──────────────────────────────────────────────────────────────────────────
    # Build lineage graph (all sources, concurrent)
    # ──────────────────────────────────────────────────────────────────────────

    def get_lineage_graph(self) -> LineageGraph:
        """Fetch all lineage sources concurrently and build a LineageGraph.

        All I/O-bound fetches run in parallel:
          - Managed Connect REST  (HTTP)
          - Flink CLI             (subprocess)
          - Metrics API           (HTTP)
          - ksqlDB REST           (HTTP, one per configured cluster)
          - Self-managed Connect  (HTTP, one per configured cluster)
        """
        env     = self._cfg.environment_id
        cluster = self._cfg.cluster_id

        # Submit all concurrent fetches
        n_workers = 3 + len(self._ksql_clients) + len(self._sm_connect_clients)
        futures: dict[str, Any] = {}

        with ThreadPoolExecutor(max_workers=max(n_workers, 4)) as pool:
            futures["connectors"]      = pool.submit(self.list_connectors)
            futures["statements"]      = pool.submit(self.list_flink_statements)
            futures["consumer_groups"] = pool.submit(self._metrics.get_consumer_groups)

            for kc in self._ksql_clients:
                futures[f"ksql:{kc._cluster.cluster_id}"] = pool.submit(kc.get_queries)

            for sc in self._sm_connect_clients:
                futures[f"sm:{sc._name}"] = pool.submit(sc.get_connectors)

        # Collect results (each underlying method already catches exceptions
        # and returns an empty list on failure, so .result() is safe here)
        managed_connectors: list[ConnectorInfo]   = futures["connectors"].result()
        statements: list[FlinkStatement]           = futures["statements"].result()
        consumer_groups: list[ConsumerGroupInfo]   = futures["consumer_groups"].result()

        ksql_queries: list[KsqlQuery] = []
        for kc in self._ksql_clients:
            ksql_queries.extend(futures[f"ksql:{kc._cluster.cluster_id}"].result())

        sm_connectors: list[ConnectorInfo] = []
        for sc in self._sm_connect_clients:
            sm_connectors.extend(futures[f"sm:{sc._name}"].result())

        all_connectors = managed_connectors + sm_connectors

        log.info(
            "Fetched: %d managed connectors, %d self-managed connectors, "
            "%d Flink statements, %d consumer groups, %d ksqlDB queries",
            len(managed_connectors), len(sm_connectors),
            len(statements), len(consumer_groups), len(ksql_queries),
        )

        # ── Build edges ───────────────────────────────────────────────────────

        edges: list[LineageEdge] = []

        # ── 1. Connectors (managed + self-managed) ────────────────────────────
        for conn in all_connectors:
            ns = (
                f"kafka-connect://{conn.connect_cluster}"
                if conn.connect_cluster
                else f"kafka-connect://{env}"
            )
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
                    ))

        # ── 2. Flink statements ───────────────────────────────────────────────
        for stmt in statements:
            if not stmt.is_running():
                log.debug("Skipping non-running Flink statement: %s (%s)", stmt.name, stmt.status)
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
                    ))

        # ── 3. Consumer groups (from Metrics API) ─────────────────────────────
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
                ))

        # ── 4. ksqlDB persistent queries ──────────────────────────────────────
        for query in ksql_queries:
            if not query.is_running():
                log.debug(
                    "Skipping non-running ksqlDB query: %s (%s)", query.query_id, query.state
                )
                continue
            if not query.source_topics or not query.sink_topics:
                log.debug(
                    "Skipping ksqlDB query with incomplete topic bindings: %s", query.query_id
                )
                continue
            for src in query.source_topics:
                for sink in query.sink_topics:
                    edges.append(LineageEdge(
                        source_name=src,
                        source_type="kafka_topic",
                        target_name=sink,
                        target_type="kafka_topic",
                        job_name=query.query_id,
                        job_type="ksqldb_query",
                        job_namespace_hint=f"ksqldb://{query.ksql_cluster_id}",
                    ))

        log.info(
            "Built lineage graph: %d edges from %d connectors + %d statements + "
            "%d consumer groups + %d ksqlDB queries",
            len(edges), len(all_connectors), len(statements),
            len(consumer_groups), len(ksql_queries),
        )

        return LineageGraph(
            edges=edges,
            connectors=all_connectors,
            statements=statements,
            consumer_groups=consumer_groups,
            ksql_queries=ksql_queries,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────────────────────────────────

    def close(self) -> None:
        self._http.close()
        self._metrics.close()
        for c in self._ksql_clients:
            c.close()
        for c in self._sm_connect_clients:
            c.close()

    def __enter__(self) -> "ConfluentLineageClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

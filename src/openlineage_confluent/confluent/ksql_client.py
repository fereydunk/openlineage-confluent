"""Confluent Cloud ksqlDB REST API client.

Discovers persistent query lineage via three ksqlDB REST statements:

  SHOW STREAMS EXTENDED  → stream name → underlying Kafka topic
  SHOW TABLES  EXTENDED  → table  name → underlying Kafka topic
  SHOW QUERIES EXTENDED  → query ID, state, source streams, sink topics

The source streams returned by SHOW QUERIES are ksqlDB stream/table names,
not Kafka topic names. We resolve them through the stream/table → topic map
built from the first two statements.

Auth: HTTP Basic with a ksqlDB-scoped API key.
Create under: Confluent Cloud → ksqlDB cluster → API Keys → Add key.

Content-Type for the ksqlDB REST API must be the vendor media type:
  application/vnd.ksql.v1+json
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from openlineage_confluent.config import KsqlClusterConfig
from openlineage_confluent.confluent.models import KsqlQuery

log = logging.getLogger(__name__)

_KSQL_HEADERS = {
    "Content-Type": "application/vnd.ksql.v1+json; charset=utf-8",
    "Accept":       "application/vnd.ksql.v1+json",
}


class KsqlDbClient:
    """Fetches ksqlDB persistent query lineage for one ksqlDB cluster."""

    def __init__(self, cluster: KsqlClusterConfig, *, timeout: float = 30.0) -> None:
        self._cluster = cluster
        self._http = httpx.Client(
            base_url=cluster.rest_endpoint.rstrip("/"),
            auth=(cluster.api_key, cluster.api_secret.get_secret_value()),
            timeout=timeout,
            headers=_KSQL_HEADERS,
        )
        # True after the most recent get_queries() call returned authoritatively.
        # False on any underlying ksqlDB REST error — empty result must then
        # be treated as "unknown," not "no queries."
        self.last_ok: bool = True

    @property
    def cluster_id(self) -> str:
        """Public accessor used by ConfluentLineageClient when building the
        merged graph's failed_namespaces set. Exposing this avoids reaching
        into the private _cluster attribute from another module."""
        return self._cluster.cluster_id

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    def get_queries(self) -> list[KsqlQuery]:
        """Return all persistent queries with resolved Kafka topic bindings."""
        self.last_ok = True
        topic_map = self._build_topic_map()
        return self._fetch_queries(topic_map)

    # ──────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _run_ksql(self, statement: str) -> list[dict[str, Any]]:
        """Execute a ksqlDB statement and return the parsed JSON response array."""
        try:
            resp = self._http.post("/ksql", json={"ksql": statement})
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            self.last_ok = False
            log.warning(
                "[ksqlDB %s] statement failed (%r): %s",
                self._cluster.cluster_id, statement.strip(), exc,
            )
            return []

    def _build_topic_map(self) -> dict[str, str]:
        """Build uppercase(stream/table name) → Kafka topic name mapping.

        ksqlDB stream/table names are case-insensitive; we normalise to
        upper-case to match the format returned in query source lists.
        """
        topic_map: dict[str, str] = {}

        for stmt in ("SHOW STREAMS EXTENDED;", "SHOW TABLES EXTENDED;"):
            for item in self._run_ksql(stmt):
                for desc in item.get("sourceDescriptions", []):
                    name  = desc.get("name", "").upper()
                    topic = desc.get("topic", "")
                    if name and topic:
                        topic_map[name] = topic

        log.debug(
            "[ksqlDB %s] resolved %d stream/table → topic mappings",
            self._cluster.cluster_id, len(topic_map),
        )
        return topic_map

    def _fetch_queries(self, topic_map: dict[str, str]) -> list[KsqlQuery]:
        """Fetch all persistent queries and resolve source names to topics."""
        queries: list[KsqlQuery] = []

        for item in self._run_ksql("SHOW QUERIES EXTENDED;"):
            for q in item.get("queries", []):
                query_id = (q.get("id") or {}).get("queryId", "")
                if not query_id:
                    continue

                state = q.get("state", "UNKNOWN")
                sql   = q.get("queryString", "")

                # sinkKafkaTopics: actual Kafka topic names written by this query.
                # Falls back to "sinks" (stream names) if not present — older servers.
                sink_topics: list[str] = q.get("sinkKafkaTopics") or q.get("sinks") or []

                # sources: ksqlDB stream/table names — resolve to Kafka topic names.
                source_streams: list[str] = q.get("sources") or []
                source_topics = [
                    topic_map.get(s.upper(), s)   # resolved name, or stream name as fallback
                    for s in source_streams
                ]

                # Remove self-references (output topic also listed as a source)
                sink_set = set(sink_topics)
                source_topics = [t for t in source_topics if t not in sink_set]

                queries.append(KsqlQuery(
                    query_id=query_id,
                    sql=sql,
                    state=state,
                    sink_topics=sink_topics,
                    source_topics=source_topics,
                    ksql_cluster_id=self._cluster.cluster_id,
                ))
                log.debug(
                    "[ksqlDB %s] query=%s state=%s sources=%s sinks=%s",
                    self._cluster.cluster_id, query_id, state,
                    source_topics, sink_topics,
                )

        log.info(
            "[ksqlDB %s] found %d persistent queries",
            self._cluster.cluster_id, len(queries),
        )
        return queries

    # ──────────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────────────────────────────────

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> KsqlDbClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

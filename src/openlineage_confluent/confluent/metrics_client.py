"""Confluent Telemetry (Metrics) API client.

Two responsibilities:

1) Discover consumer group → topic mappings via the consumer_lag_offsets
   metric. This single metric covers a broad set of components:
     - Application Kafka consumers (any language/framework)
     - Kafka Streams applications
     - Self-managed Connect workers  (connect-<name> groups)
     - Any other component that commits Kafka consumer group offsets

2) Fetch per-topic throughput (bytes + records, in + out) over the lookback
   window. These are topic-level aggregates — the data is shared across all
   producers/consumers — and surface as a custom KafkaTopicThroughputDatasetFacet
   on every dataset in the lineage graph.

Internal Confluent system groups are filtered out — they are already
represented in the lineage graph by their dedicated sources:
  - ksqlDB queries  → "_confluent-ksql-*" and "_ksql-*" groups  (already via ksqlDB REST)
  - Managed Connect → internal worker groups  (already via Connect API)
  - Flink           → Flink internal groups   (already via Flink CLI)

Auth: HTTP Basic with a Cloud-level API key that has the MetricsViewer
role. This can be granted at the Environment or Organisation level in
Confluent Cloud → Access Control. If a dedicated metrics key is not
configured, the existing cloud_api_key is used as a fallback.

API reference:
  https://api.telemetry.confluent.cloud/docs
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import httpx

from openlineage_confluent.config import ConfluentConfig
from openlineage_confluent.confluent.models import ConsumerGroupInfo, TopicThroughput

log = logging.getLogger(__name__)

_METRICS_BASE = "https://api.telemetry.confluent.cloud"
_LAG_METRIC   = "io.confluent.kafka.server/consumer_lag_offsets"

# Topic-level throughput metrics. All four are SUM-aggregable cumulative
# counters when grouped by topic over an interval.
_RECEIVED_BYTES   = "io.confluent.kafka.server/received_bytes"
_SENT_BYTES       = "io.confluent.kafka.server/sent_bytes"
_RECEIVED_RECORDS = "io.confluent.kafka.server/received_records"
_SENT_RECORDS     = "io.confluent.kafka.server/sent_records"

_THROUGHPUT_METRICS: tuple[str, ...] = (
    _RECEIVED_BYTES, _SENT_BYTES, _RECEIVED_RECORDS, _SENT_RECORDS,
)

_PAGE_SIZE    = 1_000   # max rows per Metrics API page

# Consumer group ID prefixes that identify internal Confluent infrastructure.
# These groups are covered by dedicated lineage sources (Flink CLI, Connect API,
# ksqlDB REST) and must not appear as plain "consumer group" jobs.
_INTERNAL_PREFIXES: tuple[str, ...] = (
    "_",                        # _confluent-*, _ksql-*, _schemas, Flink internals
    "confluent_cli_consumer_",  # ad-hoc Confluent CLI consumers
)


class MetricsApiClient:
    """Queries consumer_lag_offsets to discover consumer group → topic bindings."""

    def __init__(self, cfg: ConfluentConfig, *, timeout: float = 30.0) -> None:
        # Fall back to the cloud API key if a dedicated metrics key is not set.
        key = cfg.metrics_api_key or cfg.cloud_api_key
        secret = (cfg.metrics_api_secret or cfg.cloud_api_secret).get_secret_value()

        self._http = httpx.Client(
            base_url=_METRICS_BASE,
            auth=(key, secret),
            timeout=timeout,
            headers={
                "Content-Type": "application/json",
                "Accept":       "application/json",
            },
        )
        self._cluster_id = cfg.cluster_id
        self._lookback   = cfg.metrics_lookback_minutes
        self._extra      = tuple(cfg.consumer_group_exclude_prefixes)

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    def get_consumer_groups(self) -> list[ConsumerGroupInfo]:
        """Return all non-internal consumer groups active in the lookback window.

        Any consumer group that committed offsets to any topic within the
        configured lookback window (default: last 10 minutes) is included.
        Groups that are caught up (lag = 0) still appear — lag=0 means the
        group is active and up to date, not inactive.
        """
        rows = self._query_lag_metric()

        # Aggregate: group_id → set[topic]
        groups: dict[str, set[str]] = defaultdict(set)
        for row in rows:
            group_id = row.get("metric.consumer_group_id", "")
            topic    = row.get("metric.topic", "")
            if group_id and topic and not self._is_internal(group_id):
                groups[group_id].add(topic)

        result = [
            ConsumerGroupInfo(group_id=gid, topics=sorted(topics))
            for gid, topics in sorted(groups.items())
        ]
        log.info(
            "Metrics API: %d consumer groups found (lookback=%d min)",
            len(result), self._lookback,
        )
        return result

    # ──────────────────────────────────────────────────────────────────────────
    # Topic throughput
    # ──────────────────────────────────────────────────────────────────────────

    def get_topic_throughput(self) -> dict[str, TopicThroughput]:
        """Return {topic: TopicThroughput} for all topics with traffic in the window.

        Issues four separate Metrics API queries (one per metric), summing per
        topic across the interval. Topics with zero traffic for all four metrics
        are omitted.
        """
        agg: dict[str, TopicThroughput] = defaultdict(
            lambda: TopicThroughput(topic="", window_minutes=self._lookback)
        )
        for metric in _THROUGHPUT_METRICS:
            for row in self._query_topic_metric(metric):
                topic = row.get("metric.topic")
                value = row.get("value")
                if not topic or value is None:
                    continue
                bucket = agg[topic]
                if not bucket.topic:
                    bucket.topic = topic
                # The four metric ids are non-overlapping; assign by which one we asked for.
                if metric == _RECEIVED_BYTES:
                    bucket.bytes_in    += int(value)
                elif metric == _SENT_BYTES:
                    bucket.bytes_out   += int(value)
                elif metric == _RECEIVED_RECORDS:
                    bucket.records_in  += int(value)
                elif metric == _SENT_RECORDS:
                    bucket.records_out += int(value)

        result = dict(sorted(agg.items()))
        log.info(
            "Metrics API: throughput for %d topics (window=%d min)",
            len(result), self._lookback,
        )
        return result

    # ──────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _is_internal(self, group_id: str) -> bool:
        """Return True if this group ID should be excluded from lineage."""
        all_prefixes = _INTERNAL_PREFIXES + self._extra
        return any(group_id.startswith(p) for p in all_prefixes)

    def _query_interval(self) -> str:
        """ISO 8601 interval covering the last N minutes, aligned to minutes."""
        now   = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        start = now - timedelta(minutes=self._lookback)
        return f"{start.isoformat()}/{now.isoformat()}"

    def _query_lag_metric(self) -> list[dict]:
        """Fetch all rows for consumer_lag_offsets, handling pagination."""
        return self._query_paginated(
            metric=_LAG_METRIC,
            group_by=["metric.consumer_group_id", "metric.topic"],
        )

    def _query_topic_metric(self, metric: str) -> list[dict]:
        """Fetch all rows for a single topic-grouped metric."""
        return self._query_paginated(metric=metric, group_by=["metric.topic"])

    def _query_paginated(self, *, metric: str, group_by: list[str]) -> list[dict]:
        """Generic paginated POST against /v2/metrics/cloud/query."""
        payload: dict = {
            "aggregations": [{"metric": metric, "agg": "SUM"}],
            "filter": {
                "field": "resource.kafka.id",
                "op":    "EQ",
                "value": self._cluster_id,
            },
            "granularity": "PT1M",
            "intervals":   [self._query_interval()],
            "group_by":    group_by,
            "limit":       _PAGE_SIZE,
        }

        rows: list[dict] = []
        page_token: str | None = None

        while True:
            if page_token:
                payload["page_token"] = page_token

            try:
                resp = self._http.post("/v2/metrics/cloud/query", json=payload)
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                log.warning("Metrics API query failed (%s): %s", metric, exc)
                break

            body = resp.json()
            rows.extend(body.get("data", []))

            page_token = (
                body.get("meta", {})
                    .get("pagination", {})
                    .get("page_token")
            )
            if not page_token:
                break

        log.debug("Metrics API returned %d rows for %s", len(rows), metric)
        return rows

    # ──────────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────────────────────────────────

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "MetricsApiClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

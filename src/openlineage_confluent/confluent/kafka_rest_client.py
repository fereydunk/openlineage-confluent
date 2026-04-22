"""Confluent Cloud Kafka REST API client.

Fetches per-topic metadata (partition count, replication factor, internal flag)
via the cluster-scoped Kafka REST API:

    GET <cluster-rest-endpoint>/kafka/v3/clusters/<cluster_id>/topics

Auth: HTTP Basic with a Kafka API key/secret (cluster-scoped). Cloud-level API
keys do NOT work against this endpoint — Kafka API keys are scoped to a single
Kafka cluster and must be created under the cluster's "API keys" tab.

The cluster REST endpoint is shown in Confluent Cloud → Cluster → Cluster
settings → "REST endpoint" (looks like
`https://lkc-xxxxx.<region>.<cloud>.confluent.cloud:443`).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

log = logging.getLogger(__name__)


@dataclass
class TopicMetadata:
    """Per-topic metadata returned by Kafka REST `/kafka/v3/.../topics`."""

    topic: str
    partitions: int
    replication_factor: int
    is_internal: bool = False


class KafkaRestClient:
    """Lists topics and their partition counts via the Kafka REST API."""

    def __init__(
        self,
        endpoint: str,
        cluster_id: str,
        api_key: str,
        api_secret: str,
        *,
        timeout: float = 30.0,
    ) -> None:
        self._http = httpx.Client(
            base_url=endpoint.rstrip("/"),
            auth=(api_key, api_secret),
            timeout=timeout,
            headers={"Accept": "application/json"},
        )
        self._cluster_id = cluster_id

    def get_topic_metadata(self) -> dict[str, TopicMetadata]:
        """Return {topic: TopicMetadata} for every topic in the cluster.

        Returns an empty dict on any HTTP failure — partition metadata is a
        nice-to-have facet and must never break the lineage graph build.
        """
        try:
            resp = self._http.get(
                f"/kafka/v3/clusters/{self._cluster_id}/topics"
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            log.warning("Kafka REST: list topics failed: %s", exc)
            return {}

        body = resp.json()
        out: dict[str, TopicMetadata] = {}
        for item in body.get("data", []):
            name = item.get("topic_name")
            if not name:
                continue
            out[name] = TopicMetadata(
                topic=name,
                partitions=int(item.get("partitions_count", 0)),
                replication_factor=int(item.get("replication_factor", 0)),
                is_internal=bool(item.get("is_internal", False)),
            )

        log.info("Kafka REST: fetched metadata for %d topics", len(out))
        return out

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "KafkaRestClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

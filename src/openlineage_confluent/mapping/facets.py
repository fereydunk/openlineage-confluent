"""Custom OpenLineage facets for Confluent / Kafka topology metadata.

Custom facets must subclass `DatasetFacet` (or `JobFacet`/`InputDatasetFacet`/
`OutputDatasetFacet`) from `openlineage.client.facet_v2` and use `@attrs.define`
so the OpenLineage SDK can serialize them.

These are emitted under stable keys in the dataset/job `facets` dict; consumers
that don't recognize them will simply ignore them per the OpenLineage spec.
"""

from __future__ import annotations

from attrs import define
from openlineage.client.facet_v2 import DatasetFacet


@define
class KafkaTopicDatasetFacet(DatasetFacet):
    """Per-topic Kafka physical metadata.

    Mirrors what Confluent Stream Lineage shows on a topic node:
      - partition count
      - replication factor
      - whether it is a Kafka-internal topic
    """

    partitions: int = 0
    replicationFactor: int = 0
    isInternal: bool = False


@define
class KafkaTopicThroughputDatasetFacet(DatasetFacet):
    """Topic-level throughput totals over a fixed lookback window.

    These are aggregated across ALL producers and consumers of the topic — they
    are NOT specific to the job carrying this RunEvent. Mirrors what Confluent
    Stream Lineage shows on a topic node ("bytes/messages received in last 10
    minutes").

    OpenLineage's standard Output/InputStatistics facets are run-scoped, so
    they do not fit topic-level aggregates. Hence a custom facet.
    """

    bytesIn: int = 0          # received_bytes
    bytesOut: int = 0         # sent_bytes
    recordsIn: int = 0        # received_records
    recordsOut: int = 0       # sent_records
    windowMinutes: int = 0    # length of the aggregation window

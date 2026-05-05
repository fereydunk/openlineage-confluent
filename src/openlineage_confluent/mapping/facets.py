"""Custom OpenLineage facets for Confluent / Kafka topology metadata.

Custom facets must subclass `DatasetFacet` (or `JobFacet`/`InputDatasetFacet`/
`OutputDatasetFacet`) from `openlineage.client.facet_v2` and use `@attrs.define`
so the OpenLineage SDK can serialize them.

These are emitted under stable keys in the dataset/job `facets` dict; consumers
that don't recognize them will simply ignore them per the OpenLineage spec.
"""

from __future__ import annotations

from attrs import define
from openlineage.client.facet_v2 import DatasetFacet, JobFacet


@define
class ConfluentJobFacet(JobFacet):
    """Confluent topology context attached to every job (connector, Flink
    statement, ksqlDB query, Tableflow sync, Kafka producer, consumer group).

    Mirrors what Confluent Cloud's UI shows in the breadcrumbs: "Org →
    Environment → Cluster → Resource". Lets downstream Marquez consumers
    filter / group lineage by Confluent topology without parsing the job
    namespace URI. All fields are optional — globally-scoped sources
    (ksqlDB clusters, self-managed Connect) leave env/cluster blank.
    """

    envId:     str = ""
    clusterId: str = ""
    cloud:     str = ""    # "aws" | "gcp" | "azure"
    region:    str = ""    # e.g. "us-east-2"


@define
class ConfluentDatasetFacet(DatasetFacet):
    """Confluent topology context attached to Kafka topic + Iceberg datasets.

    Same fields as ConfluentJobFacet — env/cluster/region — so a topic node
    in Marquez carries the same Confluent provenance as the jobs that read
    or write it. Useful for "show me everything in env-XXX" or "show me
    everything in us-east-2" queries against the lineage warehouse.
    """

    envId:     str = ""
    clusterId: str = ""
    cloud:     str = ""
    region:    str = ""


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

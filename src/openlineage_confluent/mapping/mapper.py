"""Map Confluent LineageGraph edges to OpenLineage RunEvents.

One RunEvent per unique job (connector or Flink statement).
Each event carries all input datasets and all output datasets for that job.

Namespace conventions
─────────────────────
kafka_topic    → Dataset   namespace=kafka://<bootstrap>      name=<topic>
iceberg_table  → Dataset   namespace=glue://<region>          name=<db>.<table>
source conn.   → Job       namespace=kafka-connect://<env>    name=<connector>
sink conn.     → Job       namespace=kafka-connect://<env>    name=<connector>
flink stmt.    → Job       namespace=flink://<env>            name=<statement>
tableflow      → Job       namespace=tableflow://<env>        name=<topic>
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from datetime import datetime, timezone
from uuid import UUID

from openlineage.client.event_v2 import (
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.facet_v2 import documentation_job, job_type_job, ownership_job, schema_dataset

from openlineage_confluent.confluent.kafka_rest_client import TopicMetadata
from openlineage_confluent.confluent.models import LineageEdge, LineageGraph, TopicThroughput
from openlineage_confluent.confluent.schema_registry_client import SchemaField, TopicSchema
from openlineage_confluent.config import ConfluentConfig, OpenLineageConfig
from openlineage_confluent.mapping.facets import (
    KafkaTopicDatasetFacet,
    KafkaTopicThroughputDatasetFacet,
)

log = logging.getLogger(__name__)


_JOB_TYPE_MAP: dict[str, tuple[str, str, str]] = {
    # job_type string → (processingType, integration, jobType)
    "kafka_connect_source":  ("STREAMING", "KAFKA_CONNECT", "SOURCE_CONNECTOR"),
    "kafka_connect_sink":    ("STREAMING", "KAFKA_CONNECT", "SINK_CONNECTOR"),
    "flink_statement":       ("STREAMING", "FLINK",         "QUERY"),
    "kafka_producer":        ("STREAMING", "KAFKA",         "PRODUCER"),
    "kafka_consumer_group":  ("STREAMING", "KAFKA",         "CONSUMER_GROUP"),
    "ksqldb_query":          ("STREAMING", "KSQLDB",        "QUERY"),
    "tableflow":             ("BATCH",     "TABLEFLOW",     "TABLE_SYNC"),
}


def _job_type_facet(job_type: str) -> job_type_job.JobTypeJobFacet:
    processing, integration, jt = _JOB_TYPE_MAP.get(
        job_type, ("STREAMING", "KAFKA", job_type.upper())
    )
    return job_type_job.JobTypeJobFacet(
        processingType=processing,
        integration=integration,
        jobType=jt,
    )


def _stable_run_id(namespace: str, name: str, cycle_key: str) -> str:
    raw = f"{namespace}:{name}:{cycle_key}"
    digest = hashlib.sha256(raw.encode()).digest()[:16]
    return str(UUID(bytes=digest, version=4))


class ConfluentOpenLineageMapper:
    """Converts a Confluent LineageGraph into OpenLineage RunEvents."""

    def __init__(self, confluent_cfg: ConfluentConfig, ol_cfg: OpenLineageConfig) -> None:
        self._confluent = confluent_cfg
        self._ol = ol_cfg

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def map_all(
        self,
        graph: LineageGraph,
        *,
        cycle_key: str = "default",
    ) -> list[RunEvent]:
        """Convert all lineage edges into RunEvents, one per job."""
        now = datetime.now(timezone.utc)

        # Group edges by (job_name, job_type, job_namespace_hint)
        job_edges: dict[tuple[str, str, str], list[LineageEdge]] = defaultdict(list)
        for edge in graph.edges:
            key = (edge.job_name, edge.job_type, edge.job_namespace_hint)
            job_edges[key].append(edge)

        events: list[RunEvent] = []
        for (job_name, job_type, ns_hint), edges in job_edges.items():
            event = self._build_event(
                job_name=job_name,
                job_type=job_type,
                ns_hint=ns_hint,
                edges=edges,
                event_time=now,
                cycle_key=cycle_key,
                topic_schemas=graph.topic_schemas,
                topic_metadata=graph.topic_metadata,
                topic_throughput=graph.topic_throughput,
            )
            events.append(event)

        log.info("Mapped %d RunEvents from %d edges", len(events), len(graph.edges))
        return events

    # ------------------------------------------------------------------
    # Event builder
    # ------------------------------------------------------------------

    def _build_event(
        self,
        job_name: str,
        job_type: str,
        ns_hint: str,
        edges: list[LineageEdge],
        event_time: datetime,
        cycle_key: str,
        topic_schemas: dict[str, TopicSchema],
        topic_metadata: dict[str, TopicMetadata],
        topic_throughput: dict[str, TopicThroughput],
    ) -> RunEvent:
        job = Job(
            namespace=ns_hint,
            name=job_name,
            facets={"jobType": _job_type_facet(job_type)},
        )
        run = Run(runId=_stable_run_id(ns_hint, job_name, cycle_key))

        inputs:  list[InputDataset]  = []
        outputs: list[OutputDataset] = []

        seen_inputs:  set[str] = set()
        seen_outputs: set[str] = set()

        # All edges in this group share the same job, so they share the same
        # per-env Kafka bootstrap. Pick the first non-empty value, fall back
        # to the OL config default.
        bootstrap = next(
            (e.kafka_bootstrap for e in edges if e.kafka_bootstrap),
            self._ol.kafka_bootstrap,
        )

        for edge in edges:
            # Source side
            src_key = f"{edge.source_type}:{edge.source_name}"
            if src_key not in seen_inputs:
                if edge.source_type == "kafka_topic":
                    inputs.append(self._make_input(
                        edge.source_name, bootstrap,
                        topic_schemas, topic_metadata, topic_throughput,
                    ))
                    seen_inputs.add(src_key)
                elif edge.source_type == "iceberg_table":
                    inputs.append(InputDataset(
                        namespace=edge.job_namespace_hint,
                        name=edge.source_name,
                    ))
                    seen_inputs.add(src_key)

            # Target side
            tgt_key = f"{edge.target_type}:{edge.target_name}"
            if tgt_key not in seen_outputs:
                if edge.target_type == "kafka_topic":
                    outputs.append(self._make_output(
                        edge.target_name, bootstrap,
                        topic_schemas, topic_metadata, topic_throughput,
                    ))
                    seen_outputs.add(tgt_key)
                elif edge.target_type == "iceberg_table":
                    iceberg_ns = edge.target_namespace or "iceberg://tableflow"
                    outputs.append(OutputDataset(
                        namespace=iceberg_ns,
                        name=edge.target_name,
                    ))
                    seen_outputs.add(tgt_key)

        return RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=event_time.isoformat(),
            job=job,
            run=run,
            inputs=inputs or None,
            outputs=outputs or None,
            producer=self._ol.producer,
        )

    # ------------------------------------------------------------------
    # Dataset helpers
    # ------------------------------------------------------------------

    def _make_input(
        self,
        topic: str,
        bootstrap: str,
        topic_schemas: dict[str, TopicSchema],
        topic_metadata: dict[str, TopicMetadata],
        topic_throughput: dict[str, TopicThroughput],
    ) -> InputDataset:
        return InputDataset(
            namespace=f"kafka://{bootstrap}",
            name=topic,
            facets=self._dataset_facets(topic, topic_schemas, topic_metadata, topic_throughput),
        )

    def _make_output(
        self,
        topic: str,
        bootstrap: str,
        topic_schemas: dict[str, TopicSchema],
        topic_metadata: dict[str, TopicMetadata],
        topic_throughput: dict[str, TopicThroughput],
    ) -> OutputDataset:
        return OutputDataset(
            namespace=f"kafka://{bootstrap}",
            name=topic,
            facets=self._dataset_facets(topic, topic_schemas, topic_metadata, topic_throughput),
        )

    # ------------------------------------------------------------------
    # Facet builders
    # ------------------------------------------------------------------

    def _dataset_facets(
        self,
        topic: str,
        topic_schemas: dict[str, TopicSchema],
        topic_metadata: dict[str, TopicMetadata],
        topic_throughput: dict[str, TopicThroughput],
    ) -> dict | None:
        """Return the facet dict for a topic Dataset, or None if no facets apply."""
        facets: dict = {}

        ts = topic_schemas.get(topic)
        if ts is not None:
            schema = self._schema_facet(ts)
            if schema is not None:
                facets["schema"] = schema

        tm = topic_metadata.get(topic)
        if tm is not None:
            facets["kafkaTopic"] = KafkaTopicDatasetFacet(
                partitions=tm.partitions,
                replicationFactor=tm.replication_factor,
                isInternal=tm.is_internal,
            )

        tp = topic_throughput.get(topic)
        if tp is not None:
            facets["kafkaThroughput"] = KafkaTopicThroughputDatasetFacet(
                bytesIn=tp.bytes_in,
                bytesOut=tp.bytes_out,
                recordsIn=tp.records_in,
                recordsOut=tp.records_out,
                windowMinutes=tp.window_minutes,
            )

        return facets or None

    def _schema_facet(self, ts: TopicSchema) -> schema_dataset.SchemaDatasetFacet | None:
        """Combine key + value schemas into one SchemaDatasetFacet.

        Kafka has two independent schemas per record (key, value); OpenLineage's
        SchemaDatasetFacet only models a single field list. We render both as
        prefixed top-level fields:
          key.<field>      — fields from the key subject
          value.<field>    — fields from the value subject
        """
        all_fields: list[schema_dataset.SchemaDatasetFacetFields] = []
        for prefix, sf_list in (("key", ts.key_fields), ("value", ts.value_fields)):
            for sf in sf_list:
                all_fields.append(_to_ol_field(sf, name_prefix=f"{prefix}."))

        if not all_fields:
            return None
        return schema_dataset.SchemaDatasetFacet(fields=all_fields)


def _to_ol_field(
    sf: SchemaField, *, name_prefix: str = "",
) -> schema_dataset.SchemaDatasetFacetFields:
    """Convert internal SchemaField → OpenLineage SchemaDatasetFacetFields."""
    nested = [_to_ol_field(child) for child in sf.fields] if sf.fields else None
    return schema_dataset.SchemaDatasetFacetFields(
        name=f"{name_prefix}{sf.name}",
        type=sf.type,
        description=sf.description,
        fields=nested,
    )

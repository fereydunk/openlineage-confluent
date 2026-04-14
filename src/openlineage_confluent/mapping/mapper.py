"""Map Confluent LineageGraph edges to OpenLineage RunEvents.

One RunEvent per unique job (connector or Flink statement).
Each event carries all input datasets and all output datasets for that job.

Namespace conventions
─────────────────────
kafka_topic   → Dataset   namespace=kafka://<bootstrap>      name=<topic>
source conn.  → Job       namespace=kafka-connect://<env>    name=<connector>
sink conn.    → Job       namespace=kafka-connect://<env>    name=<connector>
flink stmt.   → Job       namespace=flink://<env>            name=<statement>
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
from openlineage.client.facet_v2 import documentation_job, ownership_job

from openlineage_confluent.confluent.models import LineageEdge, LineageGraph
from openlineage_confluent.config import ConfluentConfig, OpenLineageConfig

log = logging.getLogger(__name__)


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
    ) -> RunEvent:
        job = Job(namespace=ns_hint, name=job_name)
        run = Run(runId=_stable_run_id(ns_hint, job_name, cycle_key))

        inputs:  list[InputDataset]  = []
        outputs: list[OutputDataset] = []

        seen_inputs:  set[str] = set()
        seen_outputs: set[str] = set()

        for edge in edges:
            # Source side
            src_key = f"{edge.source_type}:{edge.source_name}"
            if src_key not in seen_inputs and edge.source_type == "kafka_topic":
                inputs.append(self._make_input(edge.source_name))
                seen_inputs.add(src_key)

            # Target side
            tgt_key = f"{edge.target_type}:{edge.target_name}"
            if tgt_key not in seen_outputs and edge.target_type == "kafka_topic":
                outputs.append(self._make_output(edge.target_name))
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

    def _kafka_namespace(self) -> str:
        return f"kafka://{self._ol.kafka_bootstrap}"

    def _make_input(self, topic: str) -> InputDataset:
        return InputDataset(namespace=self._kafka_namespace(), name=topic)

    def _make_output(self, topic: str) -> OutputDataset:
        return OutputDataset(namespace=self._kafka_namespace(), name=topic)

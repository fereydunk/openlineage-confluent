"""LineageGraph — converts the flat Atlas response into a typed directed graph.

The Atlas lineage API returns:
  - guidEntityMap : guid → AtlasEntity
  - relations     : list of (fromEntityId, toEntityId)

We build a directed graph where edges represent data flow:
  producer_entity  ──▶  consumer_entity

For each "job" node (connector, ksqlDB, Flink) we want:
  inputs  = upstream topic/dataset nodes
  outputs = downstream topic/dataset nodes
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Iterator

from openlineage_confluent.confluent.models import (
    AtlasEntity,
    EntityType,
    LineageRelation,
    LineageResponse,
)

log = logging.getLogger(__name__)


@dataclass
class GraphNode:
    entity: AtlasEntity

    @property
    def guid(self) -> str:
        return self.entity.guid

    @property
    def is_dataset(self) -> bool:
        return EntityType.is_dataset(self.entity.type_name)

    @property
    def is_job(self) -> bool:
        return EntityType.is_job(self.entity.type_name)


@dataclass
class LineageEdge:
    """Directed edge: data flows from `source` to `target`."""
    source: GraphNode
    target: GraphNode


@dataclass
class JobLineage:
    """Fully resolved lineage for a single job node."""
    job: GraphNode
    inputs: list[GraphNode] = field(default_factory=list)
    outputs: list[GraphNode] = field(default_factory=list)

    @property
    def has_lineage(self) -> bool:
        return bool(self.inputs or self.outputs)


class LineageGraph:
    """In-memory directed graph built from a Confluent LineageResponse."""

    def __init__(self, response: LineageResponse) -> None:
        self._nodes: dict[str, GraphNode] = {
            guid: GraphNode(entity=entity)
            for guid, entity in response.guid_entity_map.items()
            if entity.status.value == "ACTIVE"
        }
        self._edges: list[LineageEdge] = self._build_edges(response.relations)

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def _build_edges(self, relations: list[LineageRelation]) -> list[LineageEdge]:
        edges: list[LineageEdge] = []
        for rel in relations:
            src = self._nodes.get(rel.from_entity_id)
            tgt = self._nodes.get(rel.to_entity_id)
            if src is None or tgt is None:
                log.debug(
                    "Skipping relation %s→%s — one endpoint not in guidEntityMap",
                    rel.from_entity_id,
                    rel.to_entity_id,
                )
                continue
            edges.append(LineageEdge(source=src, target=tgt))
        return edges

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    @property
    def nodes(self) -> list[GraphNode]:
        return list(self._nodes.values())

    @property
    def job_nodes(self) -> list[GraphNode]:
        return [n for n in self._nodes.values() if n.is_job]

    @property
    def dataset_nodes(self) -> list[GraphNode]:
        return [n for n in self._nodes.values() if n.is_dataset]

    def upstream_of(self, guid: str) -> list[GraphNode]:
        """All nodes that have an edge pointing *to* guid (direct inputs)."""
        return [e.source for e in self._edges if e.target.guid == guid]

    def downstream_of(self, guid: str) -> list[GraphNode]:
        """All nodes that guid points *to* (direct outputs)."""
        return [e.target for e in self._edges if e.source.guid == guid]

    # ------------------------------------------------------------------
    # Core extraction: per-job lineage
    # ------------------------------------------------------------------

    def iter_job_lineages(self) -> Iterator[JobLineage]:
        """Yield a JobLineage for every job node in the graph.

        For a topic-centric graph the relations go:
            topic_A  ──▶  connector  ──▶  topic_B

        So a job's inputs  = upstream nodes that are datasets
             a job's outputs = downstream nodes that are datasets

        We also handle job→job edges (e.g., ksqlDB materialising into another
        stream) by recursively collecting dataset leaves.
        """
        for node in self.job_nodes:
            inputs = self._collect_dataset_leaves(node.guid, direction="up", visited=set())
            outputs = self._collect_dataset_leaves(node.guid, direction="down", visited=set())
            yield JobLineage(job=node, inputs=inputs, outputs=outputs)

    def _collect_dataset_leaves(
        self,
        guid: str,
        *,
        direction: str,
        visited: set[str],
    ) -> list[GraphNode]:
        """BFS to find dataset nodes in a given direction, skipping job-to-job hops."""
        if guid in visited:
            return []
        visited.add(guid)

        neighbours = self.upstream_of(guid) if direction == "up" else self.downstream_of(guid)
        datasets: list[GraphNode] = []
        for neighbour in neighbours:
            if neighbour.is_dataset:
                datasets.append(neighbour)
            elif neighbour.is_job:
                # Traverse through intermediate job nodes (e.g., ksqlDB chain)
                datasets.extend(
                    self._collect_dataset_leaves(neighbour.guid, direction=direction, visited=visited)
                )
        return datasets

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def summary(self) -> dict[str, int]:
        type_counts: dict[str, int] = {}
        for node in self._nodes.values():
            type_counts[node.entity.type_name] = type_counts.get(node.entity.type_name, 0) + 1
        return {
            "total_nodes": len(self._nodes),
            "total_edges": len(self._edges),
            "job_nodes": len(self.job_nodes),
            "dataset_nodes": len(self.dataset_nodes),
            **type_counts,
        }

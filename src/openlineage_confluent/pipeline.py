"""Pipeline — orchestrates poll → map → emit cycle."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Callable

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from openlineage_confluent.config import AppConfig
from openlineage_confluent.confluent.client import ConfluentLineageClient
from openlineage_confluent.emitter.emitter import LineageEmitter
from openlineage_confluent.mapping.mapper import ConfluentOpenLineageMapper

log = logging.getLogger(__name__)


class LineagePipeline:
    """End-to-end pipeline: Confluent Cloud → OpenLineage."""

    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg
        self._client = ConfluentLineageClient(cfg.confluent)
        self._mapper = ConfluentOpenLineageMapper(cfg.confluent, cfg.openlineage)
        self._emitter = LineageEmitter(cfg.openlineage)
        self._cycle = 0

    # ------------------------------------------------------------------
    # Single poll cycle
    # ------------------------------------------------------------------

    def run_once(self) -> dict[str, int]:
        """Execute one poll → map → emit cycle."""
        self._cycle += 1
        cycle_key = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        log.info("=== Poll cycle #%d  key=%s ===", self._cycle, cycle_key)

        t0 = time.monotonic()
        graph = self._client.get_lineage_graph()
        fetch_ms = int((time.monotonic() - t0) * 1000)

        summary = graph.summary()
        log.info("Lineage graph: %s  (fetched in %dms)", summary, fetch_ms)

        events = self._mapper.map_all(graph, cycle_key=cycle_key)
        force = self._cfg.pipeline.emit_full_refresh or self._cycle == 1
        emitted, skipped = self._emitter.emit_batch(events, force=force)

        stats = {
            "cycle": self._cycle,
            "connectors": summary["connectors"],
            "flink_statements": summary["flink_statements"],
            "edges": summary["edges"],
            "events_mapped": len(events),
            "events_emitted": emitted,
            "events_skipped": skipped,
            "fetch_ms": fetch_ms,
        }
        log.info("Cycle #%d stats: %s", self._cycle, stats)
        return stats

    # ------------------------------------------------------------------
    # Continuous polling
    # ------------------------------------------------------------------

    def run_forever(self, *, on_cycle: Callable[[dict[str, int]], None] | None = None) -> None:
        interval = self._cfg.pipeline.poll_interval_seconds
        log.info("Starting scheduled pipeline — interval=%ds", interval)
        scheduler = BlockingScheduler()
        scheduler.add_job(
            func=self._scheduled_cycle,
            trigger=IntervalTrigger(seconds=interval),
            kwargs={"on_cycle": on_cycle},
            next_run_time=datetime.now(timezone.utc),
        )
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            log.info("Pipeline stopped.")

    def _scheduled_cycle(self, on_cycle: Callable | None) -> None:
        try:
            stats = self.run_once()
            if on_cycle:
                on_cycle(stats)
        except Exception:
            log.exception("Poll cycle failed — will retry next interval")

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "LineagePipeline":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

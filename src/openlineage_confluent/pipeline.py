"""Pipeline — orchestrates poll → map → emit cycle."""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from openlineage_confluent.config import AppConfig, ConfluentConfig, EnvDeployment
from openlineage_confluent.confluent.client import ConfluentLineageClient
from openlineage_confluent.emitter.emitter import LineageEmitter
from openlineage_confluent.emitter.state_store import StateStore
from openlineage_confluent.mapping.mapper import ConfluentOpenLineageMapper

log = logging.getLogger(__name__)


def _env_signature(env: EnvDeployment) -> tuple:
    """Stable signature for an EnvDeployment — used to detect material changes.

    Includes credential fields so a key rotation triggers a client rebuild,
    not just env_id additions/removals. SecretStr values are unwrapped once
    so the tuple compares cleanly across pydantic re-loads.
    """
    sr_sig: tuple | None = None
    if env.schema_registry is not None:
        sr_sig = (
            env.schema_registry.endpoint,
            env.schema_registry.api_key,
            env.schema_registry.api_secret.get_secret_value(),
        )
    return (
        env.env_id,
        env.cluster_id,
        env.kafka_bootstrap,
        env.flink_compute_pool,
        env.kafka_rest_endpoint,
        env.kafka_api_key,
        env.kafka_api_secret.get_secret_value() if env.kafka_api_secret else None,
        sr_sig,
    )


def _envs_signature(cfg: ConfluentConfig) -> frozenset:
    """Order-insensitive signature of the full env list."""
    return frozenset(_env_signature(e) for e in cfg.environments)


class LineagePipeline:
    """End-to-end pipeline: Confluent Cloud → OpenLineage."""

    def __init__(self, cfg: AppConfig, *, config_path: Path | None = None) -> None:
        self._cfg = cfg
        # Optional path used for hot-reload — when set, run_once() re-reads it
        # at the top of each cycle and rebuilds the underlying lineage client
        # if the env list (or any env's credentials) changed. None disables
        # hot-reload (used by tests and `run-once` invocations).
        self._config_path: Path | None = config_path
        self._client = ConfluentLineageClient(cfg.confluent)
        self._envs_sig = _envs_signature(cfg.confluent)
        self._mapper = ConfluentOpenLineageMapper(cfg.confluent, cfg.openlineage)
        self._store  = StateStore(cfg.pipeline.state_db)
        self._emitter = LineageEmitter(
            cfg.openlineage,
            state_store=self._store,
            max_workers=cfg.pipeline.max_workers,
        )
        self._cycle = 0
        # Set by stop() (or a SIGTERM handler installed by the CLI) to break the
        # run_forever loop and short-circuit the post-cycle wait.
        self._stop_event = threading.Event()

    # ------------------------------------------------------------------
    # Hot-reload
    # ------------------------------------------------------------------

    def _maybe_reload_envs(self) -> None:
        """Re-read config.yaml; rebuild the lineage client if envs changed.

        No-op if `config_path` was not provided. Silently keeps the existing
        client on YAML/validation errors so a transient bad write from the
        wizard doesn't crash the polling loop. The emitter and its state
        store are never rebuilt — diff-tracking continuity survives.
        """
        if self._config_path is None:
            return
        try:
            new_cfg = AppConfig.from_yaml(self._config_path)
        except Exception as exc:    # noqa: BLE001
            log.warning("Hot-reload skipped — failed to parse %s: %s",
                        self._config_path, exc)
            return

        new_sig = _envs_signature(new_cfg.confluent)
        if new_sig == self._envs_sig:
            return

        old_ids = {e.env_id for e in self._cfg.confluent.environments}
        new_ids = {e.env_id for e in new_cfg.confluent.environments}
        added   = new_ids - old_ids
        removed = old_ids - new_ids
        rotated = new_ids & old_ids    # same env_id, different signature
        log.info(
            "Hot-reload: env list changed — added=%s removed=%s rotated=%s",
            sorted(added), sorted(removed),
            sorted(rotated) if (added or removed) is False else "(see added/removed)",
        )

        self._client.close()
        self._cfg     = new_cfg
        self._client  = ConfluentLineageClient(new_cfg.confluent)
        self._mapper  = ConfluentOpenLineageMapper(new_cfg.confluent, new_cfg.openlineage)
        self._envs_sig = new_sig

    # ------------------------------------------------------------------
    # Single poll cycle
    # ------------------------------------------------------------------

    def run_once(self) -> dict[str, int]:
        """Execute one poll → map → emit cycle."""
        self._maybe_reload_envs()
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
        emitted, skipped, removed = self._emitter.emit_batch(
            events, force=force, failed_namespaces=graph.failed_namespaces,
        )

        stats = {
            "cycle":                   self._cycle,
            "managed_connectors":      summary["managed_connectors"],
            "self_managed_connectors": summary["self_managed_connectors"],
            "flink_statements":        summary["flink_statements"],
            "kafka_producers":         summary["kafka_producers"],
            "consumer_groups":         summary["consumer_groups"],
            "ksql_queries":            summary["ksql_queries"],
            "tableflow_topics":        summary["tableflow_topics"],
            "edges":                   summary["edges"],
            "events_mapped":           len(events),
            "events_emitted":          emitted,
            "events_skipped":          skipped,
            "events_removed":          removed,
            "fetch_ms":                fetch_ms,
        }
        log.info("Cycle #%d stats: %s", self._cycle, stats)
        return stats

    # ------------------------------------------------------------------
    # Continuous polling
    # ------------------------------------------------------------------

    def run_forever(self, *, on_cycle: Callable[[dict[str, int]], None] | None = None) -> None:
        """Loop run_once() then wait `poll_interval_seconds` AFTER completion.

        The interval is start-of-wait → end-of-wait, so a slow cycle never
        triggers an overlapping run. The wait is interruptible via stop() or
        whatever signal handler the caller installs that calls stop(), so a
        SIGTERM during the sleep returns within ~milliseconds rather than
        dragging out shutdown by up to one full interval.
        """
        interval = self._cfg.pipeline.poll_interval_seconds
        log.info(
            "Starting pipeline — wait %ds after each cycle completes (Ctrl+C or SIGTERM to stop)",
            interval,
        )
        # Intentionally do NOT clear the event here — if stop() was called
        # before run_forever() (e.g. SIGTERM during startup), we want to honor
        # it and bail before doing any work.
        while not self._stop_event.is_set():
            try:
                stats = self.run_once()
                if on_cycle:
                    on_cycle(stats)
            except KeyboardInterrupt:
                log.info("Pipeline stopped (KeyboardInterrupt).")
                return
            except Exception:
                log.exception("Poll cycle failed — will retry after the usual wait")

            if self._stop_event.is_set():
                break
            # Event.wait returns True if set, False on timeout. Either way the
            # loop condition will exit cleanly on the next iteration.
            self._stop_event.wait(interval)
        log.info("Pipeline stopped.")

    def stop(self) -> None:
        """Signal run_forever() to exit at the next safe point.

        If a cycle is in flight, it finishes; the post-cycle wait is skipped.
        Idempotent and safe to call from a signal handler.
        """
        self._stop_event.set()

    def close(self) -> None:
        self._client.close()
        self._store.close()

    def __enter__(self) -> "LineagePipeline":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

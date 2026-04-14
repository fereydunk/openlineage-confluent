"""OpenLineage event emitter.

Wraps openlineage-python's transport layer and adds:
  - console transport for local debugging
  - diff tracking (only emit changed job lineages)
  - structured logging
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import RunEvent
from openlineage.client.transport.http import ApiKeyTokenProvider, HttpConfig, HttpTransport
from openlineage.client.transport.console import ConsoleConfig, ConsoleTransport

from openlineage_confluent.config import OpenLineageConfig

log = logging.getLogger(__name__)


def _event_fingerprint(event: RunEvent) -> str:
    """Stable hash of an event's semantic content (inputs + outputs).

    Used to detect whether a job's lineage has changed since last poll.
    """
    key = {
        "job": f"{event.job.namespace}/{event.job.name}",
        "inputs": sorted(f"{d.namespace}/{d.name}" for d in (event.inputs or [])),
        "outputs": sorted(f"{d.namespace}/{d.name}" for d in (event.outputs or [])),
    }
    return hashlib.sha256(json.dumps(key, sort_keys=True).encode()).hexdigest()


class LineageEmitter:
    """Thin wrapper around OpenLineageClient with diff-tracking."""

    def __init__(self, cfg: OpenLineageConfig) -> None:
        self._cfg = cfg
        self._client = self._build_client(cfg)
        # fingerprint → last emitted fingerprint
        self._last_fingerprints: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Client factory
    # ------------------------------------------------------------------

    @staticmethod
    def _build_client(cfg: OpenLineageConfig) -> OpenLineageClient:
        if cfg.transport == "console":
            return OpenLineageClient(transport=ConsoleTransport(ConsoleConfig()))

        auth = ApiKeyTokenProvider({"apiKey": cfg.api_key}) if cfg.api_key else None
        http_cfg = HttpConfig(
            url=cfg.url,
            **({"auth": auth} if auth else {}),
        )
        return OpenLineageClient(transport=HttpTransport(http_cfg))

    # ------------------------------------------------------------------
    # Emit
    # ------------------------------------------------------------------

    def emit(self, event: RunEvent, *, force: bool = False) -> bool:
        """Emit a single RunEvent.

        If *force* is False, skip events whose lineage fingerprint matches
        the last emission (no-op when nothing changed).

        Returns True if the event was emitted, False if skipped.
        """
        job_key = f"{event.job.namespace}/{event.job.name}"
        fingerprint = _event_fingerprint(event)

        if not force and self._last_fingerprints.get(job_key) == fingerprint:
            log.debug("Skipping unchanged lineage for %s", job_key)
            return False

        try:
            self._client.emit(event)
            self._last_fingerprints[job_key] = fingerprint
            log.info("Emitted lineage for %s (in=%d, out=%d)",
                     job_key,
                     len(event.inputs or []),
                     len(event.outputs or []))
            return True
        except Exception:
            log.exception("Failed to emit event for %s", job_key)
            return False

    def emit_batch(self, events: list[RunEvent], *, force: bool = False) -> tuple[int, int]:
        """Emit a list of events. Returns (emitted_count, skipped_count)."""
        emitted = skipped = 0
        for event in events:
            if self.emit(event, force=force):
                emitted += 1
            else:
                skipped += 1
        log.info("Batch complete — emitted=%d skipped=%d", emitted, skipped)
        return emitted, skipped

    def reset_state(self) -> None:
        """Clear diff state — next emit_batch will send everything."""
        self._last_fingerprints.clear()

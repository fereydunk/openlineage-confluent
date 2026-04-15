"""OpenLineage event emitter.

Wraps openlineage-python's transport layer and adds:
  - console transport for local debugging
  - diff tracking (only emit changed job lineages)
  - removal detection (ABORT event when a job disappears from the graph)
  - state persistence across runs (so run-once can detect removals)
  - structured logging
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.transport.http import ApiKeyTokenProvider, HttpConfig, HttpTransport
from openlineage.client.transport.console import ConsoleConfig, ConsoleTransport

from openlineage_confluent.config import OpenLineageConfig

log = logging.getLogger(__name__)

# Default state file location — persists known-jobs across process restarts.
DEFAULT_STATE_FILE = Path.home() / ".openlineage-confluent" / "state.json"


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
    """Thin wrapper around OpenLineageClient with diff-tracking and removal detection.

    State (known jobs + fingerprints) is persisted to *state_file* after every
    emit_batch() call, so that run-once invocations across restarts can still
    detect when a job has been removed from Confluent Cloud.
    """

    def __init__(
        self,
        cfg: OpenLineageConfig,
        *,
        state_file: Path = DEFAULT_STATE_FILE,
    ) -> None:
        self._cfg = cfg
        self._client = self._build_client(cfg)
        self._state_file = state_file

        # Persisted across runs: job_key → (namespace, name)
        self._known_jobs: dict[str, tuple[str, str]] = {}
        # Persisted across runs: job_key → last-emitted fingerprint
        self._last_fingerprints: dict[str, str] = {}

        self._load_state()

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def _load_state(self) -> None:
        """Restore known-jobs and fingerprints from the state file if it exists."""
        if not self._state_file.exists():
            return
        try:
            raw = json.loads(self._state_file.read_text())
            self._known_jobs = {k: tuple(v) for k, v in raw.get("known_jobs", {}).items()}  # type: ignore[misc]
            self._last_fingerprints = raw.get("fingerprints", {})
            log.debug("Loaded state: %d known jobs from %s",
                      len(self._known_jobs), self._state_file)
        except Exception:
            log.warning("Could not load state file %s — starting fresh", self._state_file)

    def _save_state(self) -> None:
        """Persist current known-jobs and fingerprints to the state file."""
        try:
            self._state_file.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "known_jobs": {k: list(v) for k, v in self._known_jobs.items()},
                "fingerprints": self._last_fingerprints,
            }
            self._state_file.write_text(json.dumps(payload, indent=2))
        except Exception:
            log.warning("Could not save state file %s", self._state_file)

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

        # Register so removal can be detected in a future cycle.
        self._known_jobs[job_key] = (event.job.namespace, event.job.name)

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

    def emit_batch(self, events: list[RunEvent], *, force: bool = False) -> tuple[int, int, int]:
        """Emit a list of events. Returns (emitted, skipped, removed).

        Removal detection: any job previously seen that is absent from *events*
        receives an ABORT RunEvent so the backend can mark it as gone.
        State is persisted to disk after every call.
        """
        current_keys = {f"{e.job.namespace}/{e.job.name}" for e in events}

        # Jobs seen before but absent this cycle → they have been removed.
        removed_keys = [k for k in self._known_jobs if k not in current_keys]
        for job_key in removed_keys:
            namespace, name = self._known_jobs.pop(job_key)
            self._last_fingerprints.pop(job_key, None)
            self._emit_removal(namespace, name)

        emitted = skipped = 0
        for event in events:
            if self.emit(event, force=force):
                emitted += 1
            else:
                skipped += 1

        self._save_state()

        log.info("Batch complete — emitted=%d skipped=%d removed=%d",
                 emitted, skipped, len(removed_keys))
        return emitted, skipped, len(removed_keys)

    def _emit_removal(self, namespace: str, name: str) -> None:
        """Emit an ABORT event for a job that has disappeared from the lineage graph."""
        event = RunEvent(
            eventType=RunState.ABORT,
            eventTime=datetime.now(timezone.utc).isoformat(),
            job=Job(namespace=namespace, name=name),
            run=Run(runId=str(uuid.uuid4())),
            producer=self._cfg.producer,
        )
        try:
            self._client.emit(event)
            log.info("Emitted removal (ABORT) for %s/%s", namespace, name)
        except Exception:
            log.exception("Failed to emit removal event for %s/%s", namespace, name)

    def reset_state(self) -> None:
        """Clear all diff state and delete the state file."""
        self._last_fingerprints.clear()
        self._known_jobs.clear()
        if self._state_file.exists():
            self._state_file.unlink()

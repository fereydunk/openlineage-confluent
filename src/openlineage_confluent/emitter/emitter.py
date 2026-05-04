"""OpenLineage event emitter.

Wraps openlineage-python's transport layer and adds:
  - Parallel emission via ThreadPoolExecutor
  - Diff tracking (only emit changed job lineages)
  - Removal detection (ABORT event when a job disappears from the graph)
  - SQLite-backed state persistence (via StateStore)
  - Structured logging
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from functools import partial

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.transport.http import ApiKeyTokenProvider, HttpConfig, HttpTransport
from openlineage.client.transport.console import ConsoleConfig, ConsoleTransport

from openlineage_confluent.config import OpenLineageConfig
from openlineage_confluent.emitter.state_store import StateStore

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
    """Parallel OpenLineageClient wrapper with diff-tracking and removal detection.

    Events are emitted concurrently via a ThreadPoolExecutor. The HTTP call is
    always made outside any lock; only the in-memory dict reads/writes are
    guarded. State is persisted to SQLite via the injected StateStore after
    each emit_batch() call.
    """

    def __init__(
        self,
        cfg: OpenLineageConfig,
        *,
        state_store: StateStore,
        max_workers: int = 8,
    ) -> None:
        self._cfg = cfg
        self._client = self._build_client(cfg)
        self._store = state_store
        self._max_workers = max_workers
        self._lock = threading.Lock()

        # Initialise in-memory state from the persisted store.
        self._known_jobs: dict[str, tuple[str, str]] = state_store.get_known_jobs()
        self._last_fingerprints: dict[str, str] = state_store.get_fingerprints()

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
    # Emit (thread-safe — called concurrently from emit_batch)
    # ------------------------------------------------------------------

    def emit(self, event: RunEvent, *, force: bool = False) -> bool:
        """Emit a single RunEvent.

        Thread-safe: the lock is held only around dict reads/writes, not
        around the HTTP call. This allows the ThreadPoolExecutor in
        emit_batch() to send events concurrently.

        Returns True if the event was emitted, False if skipped.
        """
        job_key = f"{event.job.namespace}/{event.job.name}"
        fingerprint = _event_fingerprint(event)

        # — guard dict reads and the skip check —
        with self._lock:
            self._known_jobs[job_key] = (event.job.namespace, event.job.name)
            if not force and self._last_fingerprints.get(job_key) == fingerprint:
                log.debug("Skipping unchanged lineage for %s", job_key)
                return False

        # — HTTP call outside the lock —
        try:
            self._client.emit(event)
        except Exception:
            log.exception("Failed to emit event for %s", job_key)
            return False

        # — guard fingerprint write —
        with self._lock:
            self._last_fingerprints[job_key] = fingerprint

        log.info("Emitted lineage for %s (in=%d, out=%d)",
                 job_key,
                 len(event.inputs or []),
                 len(event.outputs or []))
        return True

    def emit_batch(
        self,
        events: list[RunEvent],
        *,
        force: bool = False,
        failed_namespaces: set[str] | None = None,
    ) -> tuple[int, int, int]:
        """Emit a list of events concurrently. Returns (emitted, skipped, removed).

        Removal detection: any job previously seen that is absent from *events*
        normally receives a serial ABORT RunEvent. EXCEPT for jobs whose
        namespace is in `failed_namespaces` — those are quarantined: we don't
        know whether they were really removed or just unfetchable this cycle
        (auth, network, 5xx), so we leave them in `_known_jobs` untouched and
        skip the ABORT. The next successful poll will either re-emit them
        normally or finally remove them.

        State is flushed to SQLite after all events are processed.
        """
        failed_namespaces = failed_namespaces or set()
        current_keys = {f"{e.job.namespace}/{e.job.name}" for e in events}

        # — detect removals under lock (needs full dict scan) —
        # Suppress ABORTs for jobs whose source failed: those jobs stay in
        # _known_jobs so a subsequent successful poll can pick up where we
        # left off without losing diff-state.
        with self._lock:
            removed_keys = []
            quarantined  = 0
            for k, (ns, _) in self._known_jobs.items():
                if k in current_keys:
                    continue
                if ns in failed_namespaces:
                    quarantined += 1
                    continue
                removed_keys.append(k)

        if quarantined:
            log.info(
                "Suppressed removal-detection for %d known job(s) in failed "
                "namespaces (%s) — will retry next cycle.",
                quarantined, sorted(failed_namespaces),
            )

        # — emit ABORT for each removed job (serial — typically a small set) —
        for job_key in removed_keys:
            with self._lock:
                namespace, name = self._known_jobs.pop(job_key)
                self._last_fingerprints.pop(job_key, None)
            self._emit_removal(namespace, name)

        # — emit current events in parallel —
        emitted = skipped = 0
        _emit = partial(self.emit, force=force)
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = {pool.submit(_emit, event): event for event in events}
            for future in as_completed(futures):
                if future.result():
                    emitted += 1
                else:
                    skipped += 1

        # — persist state to SQLite —
        with self._lock:
            jobs_snapshot = dict(self._known_jobs)
            fp_snapshot   = dict(self._last_fingerprints)
        self._store.flush(jobs_snapshot, fp_snapshot)

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
        """Clear all diff state and wipe the persistent store."""
        with self._lock:
            self._last_fingerprints.clear()
            self._known_jobs.clear()
        self._store.flush({}, {})

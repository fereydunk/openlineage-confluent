"""SQLite-backed state store for known jobs and lineage fingerprints.

Replaces the flat JSON state file. Advantages at scale:
- WAL mode allows concurrent reads during a write transaction
- Atomic full-replace flush: no partial-write corruption
- O(log n) point queries for future optimisation
- Single file, no serialisation overhead for large graphs
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import UTC
from pathlib import Path

log = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS known_jobs (
    job_key      TEXT PRIMARY KEY,
    namespace    TEXT NOT NULL,
    name         TEXT NOT NULL,
    fingerprint  TEXT NOT NULL DEFAULT '',
    last_seen_at TEXT NOT NULL
)
"""


class StateStore:
    """Persists emitter state (known jobs + fingerprints) to a SQLite database.

    The store eagerly loads the full table into two in-memory dicts on startup
    so the hot path (per-event fingerprint checks) is pure dict lookups.
    SQLite is only written at the end of each emit_batch() via flush().
    """

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db_path = db_path
        self._lock = threading.Lock()

        self._known_jobs: dict[str, tuple[str, str]] = {}
        self._last_fingerprints: dict[str, str] = {}

        try:
            self._conn = sqlite3.connect(
                str(db_path),
                check_same_thread=False,  # guarded by self._lock for writes
            )
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute(_DDL)
            self._conn.commit()
            self._load_all()
            log.debug("State store loaded: %d known jobs from %s",
                      len(self._known_jobs), db_path)
        except sqlite3.DatabaseError:
            log.warning("State DB at %s is corrupt or unreadable — starting fresh", db_path)
            self._known_jobs = {}
            self._last_fingerprints = {}

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load_all(self) -> None:
        cur = self._conn.execute(
            "SELECT job_key, namespace, name, fingerprint FROM known_jobs"
        )
        for job_key, namespace, name, fingerprint in cur.fetchall():
            self._known_jobs[job_key] = (namespace, name)
            if fingerprint:
                self._last_fingerprints[job_key] = fingerprint

    # ------------------------------------------------------------------
    # Public read API (return snapshots — callers own their copies)
    # ------------------------------------------------------------------

    def get_known_jobs(self) -> dict[str, tuple[str, str]]:
        with self._lock:
            return dict(self._known_jobs)

    def get_fingerprints(self) -> dict[str, str]:
        with self._lock:
            return dict(self._last_fingerprints)

    # ------------------------------------------------------------------
    # Persist
    # ------------------------------------------------------------------

    def flush(
        self,
        known_jobs: dict[str, tuple[str, str]],
        fingerprints: dict[str, str],
    ) -> None:
        """Atomically replace the persisted state with the given snapshot.

        Called once per emit_batch() after all events have been processed.
        The full-replace strategy keeps the SQLite write simple and correct
        without requiring per-row upsert logic.
        """
        from datetime import datetime
        now = datetime.now(UTC).isoformat()

        rows = [
            (job_key, ns, name, fingerprints.get(job_key, ""), now)
            for job_key, (ns, name) in known_jobs.items()
        ]

        try:
            with self._lock:
                self._known_jobs = dict(known_jobs)
                self._last_fingerprints = dict(fingerprints)
                with self._conn:   # auto-commit transaction
                    self._conn.execute("DELETE FROM known_jobs")
                    self._conn.executemany(
                        "INSERT INTO known_jobs "
                        "(job_key, namespace, name, fingerprint, last_seen_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        rows,
                    )
        except sqlite3.DatabaseError:
            log.warning("Failed to flush state to %s", self._db_path)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    def __enter__(self) -> StateStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

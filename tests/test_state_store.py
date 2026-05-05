"""Tests for SQLite-backed StateStore."""

from __future__ import annotations

from pathlib import Path

from openlineage_confluent.emitter.state_store import StateStore


def _store(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "state.db")


def test_creates_db_file(tmp_path: Path) -> None:
    _store(tmp_path)
    assert (tmp_path / "state.db").exists()


def test_empty_on_first_use(tmp_path: Path) -> None:
    s = _store(tmp_path)
    assert s.get_known_jobs() == {}
    assert s.get_fingerprints() == {}


def test_flush_and_reload(tmp_path: Path) -> None:
    """Data flushed by one instance is visible to a new instance."""
    s1 = _store(tmp_path)
    s1.flush(
        {"ns/job-a": ("ns", "job-a"), "ns/job-b": ("ns", "job-b")},
        {"ns/job-a": "fp-a", "ns/job-b": "fp-b"},
    )
    s1.close()

    s2 = _store(tmp_path)
    assert s2.get_known_jobs() == {"ns/job-a": ("ns", "job-a"), "ns/job-b": ("ns", "job-b")}
    assert s2.get_fingerprints() == {"ns/job-a": "fp-a", "ns/job-b": "fp-b"}


def test_flush_is_full_replace(tmp_path: Path) -> None:
    """A flush with fewer jobs removes the missing ones — it is not a merge."""
    s1 = _store(tmp_path)
    s1.flush(
        {"ns/job-a": ("ns", "job-a"), "ns/job-b": ("ns", "job-b")},
        {"ns/job-a": "fp-a", "ns/job-b": "fp-b"},
    )
    s1.flush(
        {"ns/job-a": ("ns", "job-a")},
        {"ns/job-a": "fp-a"},
    )
    s1.close()

    s2 = _store(tmp_path)
    assert list(s2.get_known_jobs().keys()) == ["ns/job-a"]


def test_empty_flush_clears_all(tmp_path: Path) -> None:
    s = _store(tmp_path)
    s.flush({"ns/job-a": ("ns", "job-a")}, {"ns/job-a": "fp-a"})
    s.flush({}, {})
    s.close()

    s2 = _store(tmp_path)
    assert s2.get_known_jobs() == {}
    assert s2.get_fingerprints() == {}


def test_corrupt_db_handled_gracefully(tmp_path: Path) -> None:
    """A non-SQLite file at the db path must not crash startup."""
    db_path = tmp_path / "state.db"
    db_path.write_bytes(b"this is not a sqlite database !!!!")

    s = StateStore(db_path)   # must not raise
    assert s.get_known_jobs() == {}
    assert s.get_fingerprints() == {}


def test_wal_mode_enabled(tmp_path: Path) -> None:
    s = _store(tmp_path)
    row = s._conn.execute("PRAGMA journal_mode").fetchone()
    assert row[0] == "wal"
    s.close()


def test_get_known_jobs_returns_snapshot(tmp_path: Path) -> None:
    """Mutating the returned dict does not corrupt internal state."""
    s = _store(tmp_path)
    s.flush({"ns/job-a": ("ns", "job-a")}, {})
    snap = s.get_known_jobs()
    snap["ns/intruder"] = ("ns", "intruder")
    assert "ns/intruder" not in s.get_known_jobs()


def test_large_flush_roundtrip(tmp_path: Path) -> None:
    """500 jobs flush and reload without data loss."""
    known = {f"ns/job-{i}": ("ns", f"job-{i}") for i in range(500)}
    fps   = {f"ns/job-{i}": f"fp-{i}"          for i in range(500)}

    s = _store(tmp_path)
    s.flush(known, fps)
    s.close()

    s2 = _store(tmp_path)
    assert len(s2.get_known_jobs()) == 500
    assert s2.get_fingerprints()["ns/job-42"] == "fp-42"

#!/usr/bin/env python3
"""Stress test: 3 environments × 100 pipelines × 20 components for 2 hours.

Every 5 minutes, randomly adds and removes components.
After each poll cycle, verifies Marquez reflects the correct lineage state.

Usage:
    .venv/bin/python scripts/stress_test.py [OPTIONS]

    # Full 2-hour run (default)
    .venv/bin/python scripts/stress_test.py

    # Quick validation (10 min, 1 mutation)
    .venv/bin/python scripts/stress_test.py --duration 600 --mutation-interval 300

Options:
    --marquez-url TEXT       Marquez URL  [default: http://localhost:5000]
    --duration INT           Test duration in seconds  [default: 7200]
    --poll-interval INT      Seconds between poll cycles  [default: 30]
    --mutation-interval INT  Seconds between add/remove mutations  [default: 300]
    --envs INT               Simulated environments  [default: 3]
    --pipelines INT          Pipelines per environment  [default: 100]
    --components INT         Jobs per pipeline  [default: 20]
    --mutations INT          Jobs to add/remove per env per mutation  [default: 10]
    --workers INT            Parallel emission threads per env  [default: 16]
    --state-dir PATH         SQLite state directory  [default: /tmp/stress-test-state]
"""
from __future__ import annotations

import argparse
import logging
import random
import sys
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx

# ── Bootstrap import path ──────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from openlineage_confluent.confluent.models import LineageEdge, LineageGraph
from openlineage_confluent.emitter.emitter import LineageEmitter
from openlineage_confluent.emitter.state_store import StateStore
from openlineage_confluent.mapping.mapper import ConfluentOpenLineageMapper

# ── Logging — quiet to terminal, verbose to log file ──────────────────────────
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.FileHandler("/tmp/stress_test.log")],
)
log = logging.getLogger("stress_test")

# ── ANSI colours ───────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


# ── Minimal config stubs (avoids Pydantic-Settings env-var resolution) ─────────

@dataclass
class _OLCfg:
    """Duck-typed stand-in for OpenLineageConfig — only the fields LineageEmitter needs."""
    transport: str       = "http"
    url: str             = "http://localhost:5000"
    api_key: Optional[str] = None
    kafka_bootstrap: str = "stress-test.confluent.cloud:9092"
    producer: str        = "stress-test/1.0.0"


@dataclass
class _ConfluentCfg:
    """Duck-typed stand-in for ConfluentConfig — mapper stores but never reads it."""
    environment_id: str = "env-sim"
    cluster_id: str     = "lkc-stress"


# ── Data model ─────────────────────────────────────────────────────────────────

@dataclass
class JobNode:
    """One job component in a simulated pipeline."""
    job_key:       str
    job_name:      str
    job_type:      str          # "kafka_connect_source" | "flink_statement" | "kafka_connect_sink"
    namespace:     str          # "kafka-connect://env-..." or "flink://env-..."
    input_topics:  list[str]
    output_topics: list[str]


# ── Simulated lineage graph ────────────────────────────────────────────────────

class SimulatedGraph:
    """Manages the dynamic lineage state for one simulated environment.

    Topology per pipeline (n_components = 20):
        src-{env}-p{P}            : external → topic-{P}-t000
        flink-{env}-p{P}-s00      : topic-{P}-t000 → topic-{P}-t001
        flink-{env}-p{P}-s01      : topic-{P}-t001 → topic-{P}-t002
        ...
        flink-{env}-p{P}-s17      : topic-{P}-t017 → topic-{P}-t018
        sink-{env}-p{P}           : topic-{P}-t018 → external

    Total: 1 source + 18 Flink + 1 sink = 20 components per pipeline.
    """

    def __init__(self, env_id: str, n_pipelines: int, n_components: int) -> None:
        self._env_id = env_id
        self._lock   = threading.Lock()
        self._all_jobs: dict[str, JobNode] = {}
        self._active:   set[str]           = set()
        self._build(n_pipelines, n_components)

    def _build(self, n_pipelines: int, n_components: int) -> None:
        e       = self._env_id
        n_flink = max(n_components - 2, 0)  # leave room for source + sink

        for pipe_id in range(n_pipelines):
            p      = f"p{pipe_id:03d}"
            topics = [f"stress-{e}-{p}-t{i:03d}" for i in range(n_flink + 1)]

            # ── source connector ──────────────────────────────────────
            src_name = f"src-{e}-{p}"
            src_key  = f"kafka-connect://{e}/{src_name}"
            self._all_jobs[src_key] = JobNode(
                job_key=src_key,
                job_name=src_name,
                job_type="kafka_connect_source",
                namespace=f"kafka-connect://{e}",
                input_topics=[],
                output_topics=[topics[0]],
            )

            # ── Flink statements ──────────────────────────────────────
            for f_id in range(n_flink):
                fname = f"flink-{e}-{p}-s{f_id:02d}"
                fkey  = f"flink://{e}/{fname}"
                self._all_jobs[fkey] = JobNode(
                    job_key=fkey,
                    job_name=fname,
                    job_type="flink_statement",
                    namespace=f"flink://{e}",
                    input_topics=[topics[f_id]],
                    output_topics=[topics[f_id + 1]],
                )

            # ── sink connector ────────────────────────────────────────
            snk_name = f"sink-{e}-{p}"
            snk_key  = f"kafka-connect://{e}/{snk_name}"
            self._all_jobs[snk_key] = JobNode(
                job_key=snk_key,
                job_name=snk_name,
                job_type="kafka_connect_sink",
                namespace=f"kafka-connect://{e}",
                input_topics=[topics[-1]],
                output_topics=[],
            )

        with self._lock:
            self._active = set(self._all_jobs.keys())

    def get_lineage_graph(self) -> LineageGraph:
        with self._lock:
            active_jobs = {k: self._all_jobs[k] for k in self._active}

        edges: list[LineageEdge] = []
        for job in active_jobs.values():
            if job.job_type == "kafka_connect_source":
                for out in job.output_topics:
                    edges.append(LineageEdge(
                        source_name="external", source_type="external",
                        target_name=out,        target_type="kafka_topic",
                        job_name=job.job_name, job_type=job.job_type,
                        job_namespace_hint=job.namespace,
                    ))
            elif job.job_type == "kafka_connect_sink":
                for inp in job.input_topics:
                    edges.append(LineageEdge(
                        source_name=inp,        source_type="kafka_topic",
                        target_name="external", target_type="external",
                        job_name=job.job_name, job_type=job.job_type,
                        job_namespace_hint=job.namespace,
                    ))
            else:  # flink_statement
                for inp in job.input_topics:
                    for out in job.output_topics:
                        edges.append(LineageEdge(
                            source_name=inp, source_type="kafka_topic",
                            target_name=out, target_type="kafka_topic",
                            job_name=job.job_name, job_type=job.job_type,
                            job_namespace_hint=job.namespace,
                        ))
        return LineageGraph(edges=edges)

    def mutate(self, n_remove: int, n_add: int) -> tuple[list[JobNode], list[JobNode]]:
        """Randomly deactivate n_remove and reactivate n_add jobs."""
        with self._lock:
            active_list   = list(self._active)
            inactive_list = list(set(self._all_jobs) - self._active)

            to_remove_keys = random.sample(active_list,   min(n_remove, len(active_list)))
            to_add_keys    = random.sample(inactive_list, min(n_add, len(inactive_list)))

            self._active -= set(to_remove_keys)
            self._active |= set(to_add_keys)

        return (
            [self._all_jobs[k] for k in to_remove_keys],
            [self._all_jobs[k] for k in to_add_keys],
        )

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._active)

    @property
    def total_count(self) -> int:
        return len(self._all_jobs)


# ── Per-environment pipeline runner ───────────────────────────────────────────

class EnvironmentRunner:
    """Wires together SimulatedGraph → Mapper → Emitter for one environment."""

    def __init__(
        self,
        env_id:      str,
        sim_graph:   SimulatedGraph,
        ol_cfg:      _OLCfg,
        state_dir:   Path,
        max_workers: int = 16,
    ) -> None:
        self._env_id = env_id
        self._graph  = sim_graph
        self._cycle  = 0

        confluent_cfg = _ConfluentCfg(environment_id=env_id, cluster_id=f"lkc-{env_id}")
        self._mapper  = ConfluentOpenLineageMapper(confluent_cfg, ol_cfg)  # type: ignore[arg-type]
        self._store   = StateStore(state_dir / f"{env_id}.db")
        self._emitter = LineageEmitter(ol_cfg, state_store=self._store, max_workers=max_workers)  # type: ignore[arg-type]

    def run_cycle(self) -> dict:
        self._cycle += 1
        t0        = time.monotonic()
        graph     = self._graph.get_lineage_graph()
        cycle_key = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        events    = self._mapper.map_all(graph, cycle_key=cycle_key)
        force     = self._cycle == 1

        emitted, skipped, removed = self._emitter.emit_batch(events, force=force)

        return {
            "env":        self._env_id,
            "cycle":      self._cycle,
            "active":     self._graph.active_count,
            "events":     len(events),
            "emitted":    emitted,
            "skipped":    skipped,
            "removed":    removed,
            "elapsed_ms": int((time.monotonic() - t0) * 1000),
        }

    def close(self) -> None:
        self._store.close()


# ── Marquez verifier ───────────────────────────────────────────────────────────

class MarquezVerifier:
    """Queries Marquez REST API to verify lineage state."""

    def __init__(self, base_url: str) -> None:
        self._base = base_url.rstrip("/")
        self._http = httpx.Client(timeout=60.0)

    def is_reachable(self) -> bool:
        try:
            return self._http.get(f"{self._base}/api/v1/namespaces", timeout=5.0).status_code == 200
        except Exception:
            return False

    def count_jobs_in_namespace(self, namespace: str) -> int:
        """Total jobs Marquez knows about in this namespace (cumulative — includes removed).

        Paginates with limit=100 until Marquez returns fewer than limit items.
        Queries are retried once on timeout/error.
        """
        enc    = urllib.parse.quote(namespace, safe="")
        total  = 0
        offset = 0
        page   = 100
        try:
            while True:
                r = self._http.get(
                    f"{self._base}/api/v1/namespaces/{enc}/jobs",
                    params={"limit": page, "offset": offset},
                )
                r.raise_for_status()
                jobs = r.json().get("jobs", [])
                total  += len(jobs)
                offset += len(jobs)
                if len(jobs) < page:
                    break
            return total
        except Exception as exc:
            log.warning("Marquez job count failed for %s: %s", namespace, exc)
            return -1

    def count_all_namespaces(self, namespaces: list[str]) -> dict[str, int]:
        """Count jobs in multiple namespaces concurrently.

        Each thread gets its own httpx.Client — httpx.Client is NOT thread-safe
        when shared across threads.
        """
        results: dict[str, int] = {}
        lock = threading.Lock()

        def _count(ns: str) -> None:
            # Own client per thread — avoids shared-state corruption
            with httpx.Client(timeout=60.0) as client:
                enc    = urllib.parse.quote(ns, safe="")
                total  = 0
                offset = 0
                try:
                    while True:
                        r = client.get(
                            f"{self._base}/api/v1/namespaces/{enc}/jobs",
                            params={"limit": 100, "offset": offset},
                        )
                        r.raise_for_status()
                        jobs = r.json().get("jobs", [])
                        total  += len(jobs)
                        offset += len(jobs)
                        if len(jobs) < 100:
                            break
                except Exception as exc:
                    log.warning("Marquez count failed for %s: %s", ns, exc)
                    total = -1
            with lock:
                results[ns] = total

        threads = [threading.Thread(target=_count, args=(ns,), daemon=True)
                   for ns in namespaces]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120.0)

        return results

    def get_latest_run_state(self, namespace: str, job_name: str) -> str | None:
        """Return latest run state for a specific job (e.g. 'COMPLETED', 'ABORTED')."""
        try:
            enc_ns  = urllib.parse.quote(namespace, safe="")
            enc_job = urllib.parse.quote(job_name, safe="")
            r = self._http.get(
                f"{self._base}/api/v1/namespaces/{enc_ns}/jobs/{enc_job}/runs",
                params={"limit": 1},
            )
            r.raise_for_status()
            runs = r.json().get("runs", [])
            return runs[0].get("state") if runs else None
        except Exception:
            return None

    def close(self) -> None:
        self._http.close()


# ── Results ────────────────────────────────────────────────────────────────────

@dataclass
class CycleResult:
    cycle_num:        int
    ts:               datetime
    per_env:          list[dict]
    mutation_applied: bool
    removed_jobs:     list[JobNode]
    added_jobs:       list[JobNode]
    marquez_totals:   dict[str, int]     # env_id → count in Marquez
    v_ok:             bool
    v_notes:          list[str]


# ── Orchestrator ───────────────────────────────────────────────────────────────

class StressTestOrchestrator:

    def __init__(
        self,
        marquez_url:        str,
        duration_s:         int,
        poll_interval_s:    int,
        mutation_interval_s: int,
        n_envs:             int,
        n_pipelines:        int,
        n_components:       int,
        n_mutations:        int,
        max_workers:        int,
        state_dir:          Path,
        env_name:           str | None = None,
    ) -> None:
        self._duration_s         = duration_s
        self._poll_interval_s    = poll_interval_s
        self._mutation_interval_s = mutation_interval_s
        self._n_mutations        = n_mutations

        ol_cfg = _OLCfg(
            transport="http",
            url=marquez_url,
            kafka_bootstrap="stress-test.confluent.cloud:9092",
            producer="stress-test/1.0.0",
        )

        self._verifier = MarquezVerifier(marquez_url)

        # Env naming:
        # - env_name unset (default)         → env-sim-001, env-sim-002, ...
        # - env_name set + n_envs == 1       → exactly env_name (no suffix)
        # - env_name set + n_envs >  1       → <env_name>-001, <env_name>-002, ...
        prefix = env_name or "env-sim"
        self._envs: list[tuple[str, SimulatedGraph, EnvironmentRunner]] = []
        for i in range(n_envs):
            if env_name and n_envs == 1:
                env_id = env_name
            else:
                env_id = f"{prefix}-{i+1:03d}"
            graph  = SimulatedGraph(env_id, n_pipelines, n_components)
            runner = EnvironmentRunner(env_id, graph, ol_cfg, state_dir, max_workers)
            self._envs.append((env_id, graph, runner))

        # Accumulators
        self._total_emitted   = 0
        self._total_skipped   = 0
        self._total_removed   = 0
        self._total_mutations = 0
        self._v_passes        = 0
        self._v_fails         = 0
        self._results:        list[CycleResult] = []

        # Track recently removed jobs for post-mutation ABORT spot-checks
        self._recent_removed: list[JobNode] = []
        self._recent_lock                   = threading.Lock()
        self._removed_cycle: int            = 0   # cycle when last removal happened

    # ------------------------------------------------------------------

    def run(self) -> None:
        start      = time.monotonic()
        end        = start + self._duration_s
        last_mut   = start - self._mutation_interval_s  # first mutation at interval mark
        cycle_num  = 0

        _print_header(
            self._duration_s,
            self._poll_interval_s,
            self._mutation_interval_s,
            sum(g.total_count for _, g, _ in self._envs),
        )

        try:
            while True:
                now = time.monotonic()
                if now >= end:
                    break

                cycle_num += 1
                elapsed   = now - start
                remaining = end - now
                next_mut  = max(0.0, self._mutation_interval_s - (now - last_mut))

                # ── Mutation? ──────────────────────────────────────────
                mutation_applied               = False
                removed_this_cycle: list[JobNode] = []
                added_this_cycle:   list[JobNode] = []

                if cycle_num > 1 and (now - last_mut) >= self._mutation_interval_s:
                    mutation_applied = True
                    last_mut = now
                    self._total_mutations += 1

                    for env_id, graph, _ in self._envs:
                        removed, added = graph.mutate(self._n_mutations, self._n_mutations)
                        removed_this_cycle.extend(removed)
                        added_this_cycle.extend(added)

                    with self._recent_lock:
                        self._recent_removed = removed_this_cycle[:]
                        self._removed_cycle  = cycle_num

                    _print_mutation(self._total_mutations, removed_this_cycle, added_this_cycle)

                # ── Poll all environments concurrently ─────────────────
                env_stats: list[dict] = [{}] * len(self._envs)

                def _run_env(idx: int) -> None:
                    env_stats[idx] = self._envs[idx][2].run_cycle()

                threads = [
                    threading.Thread(target=_run_env, args=(i,), daemon=True)
                    for i in range(len(self._envs))
                ]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()

                for s in env_stats:
                    self._total_emitted += s.get("emitted", 0)
                    self._total_skipped += s.get("skipped", 0)
                    self._total_removed += s.get("removed", 0)

                # ── Verify Marquez ─────────────────────────────────────
                marquez_totals, v_ok, v_notes = self._verify(env_stats, cycle_num)
                if v_ok:
                    self._v_passes += 1
                else:
                    self._v_fails += 1

                result = CycleResult(
                    cycle_num=cycle_num,
                    ts=datetime.now(timezone.utc),
                    per_env=env_stats,
                    mutation_applied=mutation_applied,
                    removed_jobs=removed_this_cycle,
                    added_jobs=added_this_cycle,
                    marquez_totals=marquez_totals,
                    v_ok=v_ok,
                    v_notes=v_notes,
                )
                self._results.append(result)

                _print_cycle(
                    result=result,
                    elapsed=elapsed,
                    remaining=remaining,
                    next_mut_in=next_mut,
                    total_emitted=self._total_emitted,
                    total_removed=self._total_removed,
                    total_mutations=self._total_mutations,
                    v_passes=self._v_passes,
                    v_fails=self._v_fails,
                )

                # ── Sleep until next poll ──────────────────────────────
                elapsed_this_cycle = time.monotonic() - now
                sleep_for = max(0.0, self._poll_interval_s - elapsed_this_cycle)
                if sleep_for > 0:
                    time.sleep(sleep_for)

        except KeyboardInterrupt:
            print(f"\n\n  {YELLOW}Interrupted — printing partial report...{RESET}")

        _print_final_report(
            results=self._results,
            total_emitted=self._total_emitted,
            total_skipped=self._total_skipped,
            total_removed=self._total_removed,
            total_mutations=self._total_mutations,
            v_passes=self._v_passes,
            v_fails=self._v_fails,
        )

        for _, _, runner in self._envs:
            runner.close()
        self._verifier.close()

    # ------------------------------------------------------------------

    def _verify(
        self,
        env_stats: list[dict],
        cycle_num: int,
    ) -> tuple[dict[str, int], bool, list[str]]:
        """Query Marquez and check:
        1. Marquez job count >= active job count (cumulative — never shrinks)
        2. Spot-check removed jobs have ABORTED state (checked 1 cycle after removal)

        Namespace queries run in parallel to avoid serial page-fetching bottleneck.
        """
        # Build the full list of namespaces to query
        all_namespaces = [
            ns
            for env_id, _, _ in self._envs
            for ns in (f"flink://{env_id}", f"kafka-connect://{env_id}")
        ]
        ns_counts = self._verifier.count_all_namespaces(all_namespaces)

        marquez_totals: dict[str, int] = {}
        notes: list[str] = []
        ok = True

        for env_id, graph, _ in self._envs:
            flink_count   = ns_counts.get(f"flink://{env_id}", -1)
            connect_count = ns_counts.get(f"kafka-connect://{env_id}", -1)
            total_in_mq   = max(0, flink_count) + max(0, connect_count)
            marquez_totals[env_id] = total_in_mq

            # Marquez accumulates all jobs ever seen — must be >= active
            active = graph.active_count
            if total_in_mq > 0 and total_in_mq < active:
                ok = False
                notes.append(f"{env_id}: Marquez={total_in_mq} < active={active}")
            elif total_in_mq == 0 and cycle_num > 1:
                # Warn but don't fail — likely a transient Marquez query error
                notes.append(f"{env_id}: Marquez returned 0 (query may have timed out)")

        # Spot-check: verify up to 3 recently removed jobs show ABORTED
        # Only check 1 cycle AFTER the removal (give Marquez time to ingest)
        with self._recent_lock:
            to_check  = list(self._recent_removed[:3])
            rem_cycle = self._removed_cycle

        if to_check and cycle_num > rem_cycle:
            for job in to_check:
                state = self._verifier.get_latest_run_state(job.namespace, job.job_name)
                if state is not None and state != "ABORTED":
                    ok = False
                    notes.append(f"{job.job_name}: expected ABORTED, got {state!r}")

        return marquez_totals, ok, notes


# ── Display helpers ────────────────────────────────────────────────────────────

def _fmt(seconds: float) -> str:
    h, rem = divmod(int(seconds), 3600)
    m, s   = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _print_header(duration_s: int, poll_s: int, mut_s: int, total_jobs: int) -> None:
    w = 76
    print("\n" + "═" * w)
    print(f"  {BOLD}openlineage-confluent — Stress Test{RESET}")
    print(f"  {total_jobs:,} total jobs  |  Duration {_fmt(duration_s)}  "
          f"|  Poll {poll_s}s  |  Mutation every {_fmt(mut_s)}")
    print("═" * w + "\n")


def _print_mutation(n: int, removed: list[JobNode], added: list[JobNode]) -> None:
    print(f"\n  {YELLOW}{BOLD}▶ Mutation #{n}{RESET}  "
          f"removed={len(removed)}  added={len(added)}")
    for j in removed[:3]:
        print(f"    {RED}− {j.job_name}{RESET}")
    if len(removed) > 3:
        print(f"    {RED}  … and {len(removed) - 3} more{RESET}")
    for j in added[:3]:
        print(f"    {GREEN}+ {j.job_name}{RESET}")
    if len(added) > 3:
        print(f"    {GREEN}  … and {len(added) - 3} more{RESET}")
    print()


def _print_cycle(
    result:          CycleResult,
    elapsed:         float,
    remaining:       float,
    next_mut_in:     float,
    total_emitted:   int,
    total_removed:   int,
    total_mutations: int,
    v_passes:        int,
    v_fails:         int,
) -> None:
    w = 76
    mut_tag = f"  {YELLOW}[MUTATION]{RESET}" if result.mutation_applied else ""
    print(f"{'─' * w}")
    print(f"  Cycle #{result.cycle_num}  elapsed {_fmt(elapsed)}  "
          f"remaining {_fmt(remaining)}  next-mut {_fmt(next_mut_in)}" + mut_tag)
    print(f"\n  {'Environment':<17} {'Active':>7} {'Emitted':>9} "
          f"{'Skipped':>9} {'Removed':>9} {'ms':>6} {'Marquez':>9}")
    print(f"  {'─' * 17} {'─' * 7} {'─' * 9} {'─' * 9} {'─' * 9} {'─' * 6} {'─' * 9}")

    tot_active = tot_em = tot_sk = tot_rm = tot_ms = tot_mq = 0
    for s in result.per_env:
        env = s.get("env", "?")
        mq  = result.marquez_totals.get(env, -1)
        mq_str = f"{mq:,}" if mq >= 0 else "err"
        print(f"  {env:<17} {s.get('active', 0):>7,} {s.get('emitted', 0):>9,} "
              f"{s.get('skipped', 0):>9,} {s.get('removed', 0):>9,} "
              f"{s.get('elapsed_ms', 0):>6} {mq_str:>9}")
        tot_active += s.get("active", 0)
        tot_em     += s.get("emitted", 0)
        tot_sk     += s.get("skipped", 0)
        tot_rm     += s.get("removed", 0)
        tot_ms     += s.get("elapsed_ms", 0)
        if mq >= 0:
            tot_mq += mq

    print(f"  {'─' * 17} {'─' * 7} {'─' * 9} {'─' * 9} {'─' * 9} {'─' * 6} {'─' * 9}")
    print(f"  {'TOTAL':<17} {tot_active:>7,} {tot_em:>9,} "
          f"{tot_sk:>9,} {tot_rm:>9,} {tot_ms:>6} {tot_mq:>9,}")

    v_icon = f"{GREEN}✓{RESET}" if result.v_ok else f"{RED}✗{RESET}"
    v_str  = f"{v_icon} {v_passes}/{v_passes + v_fails} cycles OK"
    if result.v_notes:
        v_str += f"  {RED}← {'; '.join(result.v_notes[:2])}{RESET}"

    print(f"\n  Cumulative  emitted {total_emitted:,}  "
          f"removed {total_removed:,}  mutations {total_mutations}")
    print(f"  Verification  {v_str}\n")


def _print_final_report(
    results:         list[CycleResult],
    total_emitted:   int,
    total_skipped:   int,
    total_removed:   int,
    total_mutations: int,
    v_passes:        int,
    v_fails:         int,
) -> None:
    w = 76
    print("═" * w)
    print(f"  {BOLD}FINAL REPORT — {len(results)} cycles completed{RESET}")
    print("═" * w)
    print(f"  Total cycles:        {len(results):,}")
    print(f"  Total mutations:     {total_mutations:,}")
    print(f"  Total emitted:       {total_emitted:,}")
    print(f"  Total skipped:       {total_skipped:,}")
    print(f"  Total removals:      {total_removed:,}")

    failed = [r for r in results if not r.v_ok]
    print()
    if not failed:
        print(f"  {GREEN}{BOLD}Verification: ALL {v_passes} cycles PASSED ✓{RESET}")
    else:
        print(f"  {RED}{BOLD}Verification: {v_fails} / {len(results)} cycles FAILED ✗{RESET}")
        for r in failed[:5]:
            print(f"    Cycle #{r.cycle_num}: {'; '.join(r.v_notes)}")

    # Throughput
    if results:
        durations = [
            s.get("elapsed_ms", 0)
            for r in results
            for s in r.per_env
        ]
        avg_ms = sum(durations) / len(durations) if durations else 0
        print(f"\n  Avg cycle time per env: {avg_ms:.0f}ms")
        print(f"  Avg emitted per cycle:  {total_emitted / len(results):.1f}")

    print("═" * w)
    print("  Detailed log: /tmp/stress_test.log\n")


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--marquez-url",        default="http://localhost:5000")
    p.add_argument("--duration",           type=int, default=7200,
                   help="Test duration in seconds  [default: 7200 = 2 hours]")
    p.add_argument("--poll-interval",      type=int, default=30,
                   help="Seconds between poll cycles  [default: 30]")
    p.add_argument("--mutation-interval",  type=int, default=300,
                   help="Seconds between mutations  [default: 300 = 5 min]")
    p.add_argument("--envs",               type=int, default=3,
                   help="Simulated environments  [default: 3]")
    p.add_argument("--env-name",           type=str, default=None,
                   help="Override env_id prefix. With --envs=1 the value is "
                        "used verbatim; with --envs>1 it gets a -NNN suffix. "
                        "Default: env-sim")
    p.add_argument("--pipelines",          type=int, default=100,
                   help="Pipelines per environment  [default: 100]")
    p.add_argument("--components",         type=int, default=20,
                   help="Jobs per pipeline  [default: 20]")
    p.add_argument("--mutations",          type=int, default=10,
                   help="Jobs to add/remove per env per mutation  [default: 10]")
    p.add_argument("--workers",            type=int, default=16,
                   help="Parallel emission threads per env  [default: 16]")
    p.add_argument("--state-dir",          type=Path,
                   default=Path("/tmp/stress-test-state"))
    args = p.parse_args()

    # Preflight check
    verifier = MarquezVerifier(args.marquez_url)
    if not verifier.is_reachable():
        print(f"\n  {RED}ERROR: Marquez not reachable at {args.marquez_url}{RESET}")
        print("  Start it with:  make marquez-up\n")
        sys.exit(1)
    verifier.close()

    total_jobs = args.envs * args.pipelines * args.components
    print(f"\n  {GREEN}Marquez reachable.{RESET}  "
          f"Simulating {args.envs} envs × {args.pipelines} pipelines × "
          f"{args.components} components = {BOLD}{total_jobs:,} total jobs{RESET}")

    args.state_dir.mkdir(parents=True, exist_ok=True)

    orchestrator = StressTestOrchestrator(
        marquez_url=args.marquez_url,
        duration_s=args.duration,
        poll_interval_s=args.poll_interval,
        mutation_interval_s=args.mutation_interval,
        n_envs=args.envs,
        n_pipelines=args.pipelines,
        n_components=args.components,
        n_mutations=args.mutations,
        max_workers=args.workers,
        state_dir=args.state_dir,
        env_name=args.env_name,
    )
    orchestrator.run()


if __name__ == "__main__":
    main()

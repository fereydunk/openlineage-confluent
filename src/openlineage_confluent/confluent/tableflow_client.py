"""Confluent Tableflow lineage source.

Uses the Confluent CLI to list active Tableflow topics — the Tableflow REST API
requires a bearer token not available via Cloud API keys (same restriction as
the Flink REST API). The CLI holds valid credentials and returns JSON.

Each active Tableflow topic becomes a lineage edge:
  job: tableflow://<env_id>  /  <topic_name>
  input:  Kafka topic dataset   (kafka://<bootstrap> / <topic_name>)
  output: Iceberg/Glue dataset  (glue://<region> / <glue_db>.<table_name>)
"""

from __future__ import annotations

import json
import logging
import subprocess

from openlineage_confluent.confluent.models import TableflowTopic

log = logging.getLogger(__name__)


class TableflowClient:
    """Lists active Tableflow topics via the Confluent CLI."""

    def __init__(self, env_id: str, cluster_id: str) -> None:
        self._env     = env_id
        self._cluster = cluster_id
        # True after the most recent list_topics() call returned authoritatively
        # (incl. an empty list when the CLI confirms "None found"). False when
        # the CLI was missing, timed out, or errored — callers must treat an
        # empty result in that case as "unknown" rather than "no topics."
        self.last_ok: bool = True

    def list_topics(self) -> list[TableflowTopic]:
        """Return all active Tableflow topics for the configured cluster."""
        self.last_ok = True
        try:
            result = subprocess.run(
                [
                    "confluent", "tableflow", "topic", "list",
                    "--environment", self._env,
                    "--cluster", self._cluster,
                    "-o", "json",
                ],
                capture_output=True, text=True, timeout=30,
            )
        except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
            log.warning("confluent tableflow topic list failed: %s", exc)
            self.last_ok = False
            return []

        if result.returncode != 0:
            stderr = result.stderr.strip()
            # "None found." prints to stdout with returncode 0 — but handle 1 too
            if "None found" in result.stdout or not stderr:
                return []
            log.warning("confluent tableflow topic list error: %s", stderr[:300])
            self.last_ok = False
            return []

        stdout = result.stdout.strip()
        if not stdout or stdout.lower() in ("null", "none found.", "[]"):
            return []

        try:
            raw: list[dict] = json.loads(stdout)
        except json.JSONDecodeError as exc:
            log.warning("Failed to parse tableflow topic list JSON: %s | output=%s", exc, stdout[:200])
            self.last_ok = False
            return []

        topics: list[TableflowTopic] = []
        for item in raw or []:
            # CLI JSON uses "topic_name" + "phase"; older API used "name"/"display_name"/"status"
            name   = (item.get("topic_name")
                      or item.get("display_name")
                      or item.get("name")
                      or "")
            status = (item.get("phase")
                      or item.get("status")
                      or "UNKNOWN")
            if not name:
                continue
            # Exclude explicitly disabled/suspended topics
            if status.upper() in ("DISABLED", "DELETED"):
                log.debug("Skipping Tableflow topic %s (status=%s)", name, status)
                continue
            topics.append(TableflowTopic(topic_name=name, status=status))
            log.debug("Tableflow topic: %s  status=%s", name, status)

        log.info("Fetched %d Tableflow topics", len(topics))
        return topics

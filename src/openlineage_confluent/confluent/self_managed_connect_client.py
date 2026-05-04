"""Self-managed Kafka Connect REST API client.

Scrapes the standard Kafka Connect Worker REST API from an on-premises
or customer-hosted Connect cluster (not Confluent Cloud managed Connect).

Primary path: GET /connectors?expand=info,status
  Returns all connectors with config and status in a single request.
  Available since Kafka Connect 2.3 (KIP-449).

Fallback path (for older clusters that return 404 on ?expand):
  GET /connectors           → list of connector names
  GET /connectors/{name}    → info (type, config)
  GET /connectors/{name}/status → status (connector.state)

Auth: optional HTTP Basic. Omit username/password for unauthenticated clusters.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from openlineage_confluent.config import SelfManagedConnectClusterConfig
from openlineage_confluent.confluent.models import ConnectorInfo

log = logging.getLogger(__name__)


class SelfManagedConnectClient:
    """Reads connector configs from a self-managed Kafka Connect cluster."""

    def __init__(
        self,
        cluster: SelfManagedConnectClusterConfig,
        *,
        timeout: float = 15.0,
    ) -> None:
        self._name = cluster.name
        auth: tuple[str, str] | None = None
        if cluster.username and cluster.password:
            auth = (cluster.username, cluster.password.get_secret_value())

        self._http = httpx.Client(
            base_url=cluster.rest_endpoint.rstrip("/"),
            auth=auth,
            timeout=timeout,
            headers={"Accept": "application/json"},
        )
        # True after the most recent get_connectors() call returned authoritatively.
        # False on any HTTP error — empty result must then be treated as
        # "unknown," not "no connectors."
        self.last_ok: bool = True

    @property
    def cluster_name(self) -> str:
        """Public accessor used by ConfluentLineageClient when building the
        merged graph's failed_namespaces set. Exposing this avoids reaching
        into the private _name attribute from another module."""
        return self._name

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    def get_connectors(self) -> list[ConnectorInfo]:
        """Return all connectors from this self-managed Connect cluster.

        The returned ConnectorInfo objects have connect_cluster set to
        the cluster's configured name, so the mapper uses the correct
        namespace: kafka-connect://<cluster_name>.
        """
        self.last_ok = True
        raw = self._fetch_all()
        connectors: list[ConnectorInfo] = []

        for name, data in raw.items():
            info        = data.get("info", {})
            status_data = data.get("status", {})
            state       = status_data.get("connector", {}).get("state", "UNKNOWN")
            config      = info.get("config", {})
            c_type      = info.get("type", "source").lower()

            connectors.append(ConnectorInfo(
                id=f"{self._name}:{name}",
                name=name,
                status=state,
                type=c_type,
                config=config,
                connect_cluster=self._name,
            ))
            log.debug("[%s] connector=%s type=%s state=%s", self._name, name, c_type, state)

        log.info("[%s] found %d connectors", self._name, len(connectors))
        return connectors

    # ──────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _fetch_all(self) -> dict[str, Any]:
        """Fetch all connectors, trying the batch expand endpoint first."""
        try:
            resp = self._http.get("/connectors", params={"expand": "info,status"})
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                # Older cluster — fall back to individual calls
                log.debug(
                    "[%s] ?expand not supported, falling back to individual fetches",
                    self._name,
                )
                return self._fetch_individually()
            self.last_ok = False
            log.warning("[%s] failed to list connectors: %s", self._name, exc)
            return {}
        except Exception as exc:
            self.last_ok = False
            log.warning("[%s] failed to list connectors: %s", self._name, exc)
            return {}

    def _fetch_individually(self) -> dict[str, Any]:
        """Fallback: list connector names, then fetch each one individually."""
        try:
            resp = self._http.get("/connectors")
            resp.raise_for_status()
            names: list[str] = resp.json()
        except Exception as exc:
            self.last_ok = False
            log.warning("[%s] failed to list connector names: %s", self._name, exc)
            return {}

        result: dict[str, Any] = {}
        for name in names:
            try:
                info_r   = self._http.get(f"/connectors/{name}")
                status_r = self._http.get(f"/connectors/{name}/status")
                info_r.raise_for_status()
                status_r.raise_for_status()
                # Normalise to the same shape as ?expand=info,status
                result[name] = {
                    "info":   info_r.json(),
                    "status": status_r.json(),
                }
            except Exception as exc:
                # Per-connector failure means we don't know the connector's
                # current state. Mark the whole fetch as untrustworthy so the
                # emitter quarantines this cluster's namespace and doesn't
                # synthesize a phantom ABORT for the missing connector.
                self.last_ok = False
                log.warning("[%s] failed to fetch connector %s: %s", self._name, name, exc)

        return result

    # ──────────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────────────────────────────────

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "SelfManagedConnectClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

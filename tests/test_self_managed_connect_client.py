"""Tests for SelfManagedConnectClient — connector configs from self-managed clusters."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from openlineage_confluent.config import SelfManagedConnectClusterConfig
from openlineage_confluent.confluent.self_managed_connect_client import SelfManagedConnectClient


@pytest.fixture()
def cluster_cfg() -> SelfManagedConnectClusterConfig:
    return SelfManagedConnectClusterConfig(
        name="on-prem-connect",
        rest_endpoint="http://connect-host:8083",
    )


@pytest.fixture()
def client(cluster_cfg) -> SelfManagedConnectClient:
    return SelfManagedConnectClient(cluster_cfg)


# ── Helper ─────────────────────────────────────────────────────────────────────

def _make_expand_response(connectors: dict) -> MagicMock:
    """Simulate GET /connectors?expand=info,status response."""
    resp = MagicMock()
    resp.json.return_value = connectors
    resp.raise_for_status.return_value = None
    return resp


# ── get_connectors (expand path) ───────────────────────────────────────────────

def test_get_connectors_source(client) -> None:
    data = {
        "jdbc-source": {
            "info":   {"name": "jdbc-source", "type": "source",
                       "config": {"kafka.topic": "orders", "connector.class": "JdbcSourceConnector"}},
            "status": {"connector": {"state": "RUNNING"}},
        }
    }
    with patch.object(client._http, "get", return_value=_make_expand_response(data)):
        connectors = client.get_connectors()

    assert len(connectors) == 1
    c = connectors[0]
    assert c.name == "jdbc-source"
    assert c.status == "RUNNING"
    assert c.connect_cluster == "on-prem-connect"
    assert c.topics_produced() == ["orders"]


def test_get_connectors_sink(client) -> None:
    data = {
        "s3-sink": {
            "info":   {"name": "s3-sink", "type": "sink",
                       "config": {"topics": "events", "connector.class": "S3SinkConnector"}},
            "status": {"connector": {"state": "RUNNING"}},
        }
    }
    with patch.object(client._http, "get", return_value=_make_expand_response(data)):
        connectors = client.get_connectors()

    assert len(connectors) == 1
    c = connectors[0]
    assert c.topics_consumed() == ["events"]
    assert c.topics_produced() == []


def test_get_connectors_returns_multiple(client) -> None:
    data = {
        "src-a": {
            "info":   {"name": "src-a", "type": "source", "config": {"kafka.topic": "t1"}},
            "status": {"connector": {"state": "RUNNING"}},
        },
        "sink-b": {
            "info":   {"name": "sink-b", "type": "sink", "config": {"topics": "t2"}},
            "status": {"connector": {"state": "PAUSED"}},
        },
    }
    with patch.object(client._http, "get", return_value=_make_expand_response(data)):
        connectors = client.get_connectors()

    assert len(connectors) == 2
    names = {c.name for c in connectors}
    assert names == {"src-a", "sink-b"}


def test_connect_cluster_set_on_all_connectors(client) -> None:
    data = {
        "c1": {
            "info":   {"name": "c1", "type": "source", "config": {"kafka.topic": "t1"}},
            "status": {"connector": {"state": "RUNNING"}},
        },
        "c2": {
            "info":   {"name": "c2", "type": "sink", "config": {"topics": "t2"}},
            "status": {"connector": {"state": "RUNNING"}},
        },
    }
    with patch.object(client._http, "get", return_value=_make_expand_response(data)):
        connectors = client.get_connectors()

    assert all(c.connect_cluster == "on-prem-connect" for c in connectors)


def test_get_connectors_accepts_non_standard_status(client) -> None:
    """Self-managed clusters may return statuses like UNASSIGNED or RESTARTING."""
    data = {
        "quirky-connector": {
            "info":   {"name": "quirky-connector", "type": "source", "config": {"kafka.topic": "t"}},
            "status": {"connector": {"state": "UNASSIGNED"}},
        }
    }
    with patch.object(client._http, "get", return_value=_make_expand_response(data)):
        connectors = client.get_connectors()

    assert connectors[0].status == "UNASSIGNED"


def test_get_connectors_returns_empty_on_error(client) -> None:
    import httpx
    with patch.object(client._http, "get", side_effect=httpx.HTTPError("connection refused")):
        connectors = client.get_connectors()
    assert connectors == []


# ── Fallback (individual fetch) ────────────────────────────────────────────────

def test_falls_back_to_individual_fetch_on_404(client) -> None:
    """Older clusters return 404 on ?expand — client falls back gracefully."""
    import httpx

    # First GET (expand): 404
    expand_err = MagicMock()
    expand_err.status_code = 404
    not_found = httpx.HTTPStatusError("404", request=MagicMock(), response=expand_err)

    # Second GET (list names)
    names_resp = MagicMock()
    names_resp.json.return_value = ["legacy-source"]
    names_resp.raise_for_status.return_value = None

    # Third GET (connector info)
    info_resp = MagicMock()
    info_resp.json.return_value = {
        "name": "legacy-source", "type": "source",
        "config": {"kafka.topic": "legacy-topic"},
    }
    info_resp.raise_for_status.return_value = None

    # Fourth GET (connector status)
    status_resp = MagicMock()
    status_resp.json.return_value = {"name": "legacy-source", "connector": {"state": "RUNNING"}}
    status_resp.raise_for_status.return_value = None

    with patch.object(
        client._http, "get",
        side_effect=[not_found, names_resp, info_resp, status_resp],
    ):
        connectors = client.get_connectors()

    assert len(connectors) == 1
    assert connectors[0].name == "legacy-source"
    assert connectors[0].topics_produced() == ["legacy-topic"]


# ── Auth ───────────────────────────────────────────────────────────────────────

def test_no_auth_when_credentials_not_set(cluster_cfg) -> None:
    c = SelfManagedConnectClient(cluster_cfg)
    assert c._http.auth is None


def test_basic_auth_set_when_credentials_provided() -> None:
    cfg = SelfManagedConnectClusterConfig(
        name="secured-connect",
        rest_endpoint="http://secure-host:8083",
        username="admin",
        password="secret",
    )
    c = SelfManagedConnectClient(cfg)
    assert c._http.auth is not None

"""Tests for the stateless sweep in provision_demo_pipelines.py teardown.

The sweep makes "start from scratch" work even when the per-env state file
is gone or out of sync. It lists every connector / Flink statement / topic /
SR subject in the env, filters by the ol-* prefix, and deletes each.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(scope="module")
def script_module():
    spec = importlib.util.spec_from_file_location(
        "_provdemo_sweep_under_test",
        Path(__file__).resolve().parent.parent / "scripts" / "provision_demo_pipelines.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


# ── Filter ────────────────────────────────────────────────────────────────

def test_demo_prefix_constant_is_ol_dash(script_module):
    assert script_module.DEMO_NAME_PREFIX == "ol-"


def test_is_demo_name_accepts_ol_prefix(script_module):
    assert script_module._is_demo_name("ol-orders00-t1") is True
    assert script_module._is_demo_name("ol-payments-enrich-0") is True
    assert script_module._is_demo_name("ol-cg") is True


def test_is_demo_name_rejects_non_ol(script_module):
    assert script_module._is_demo_name("my-app-topic") is False
    assert script_module._is_demo_name("orders") is False
    assert script_module._is_demo_name("") is False
    # "ol" by itself without dash isn't ours either
    assert script_module._is_demo_name("ole-thing") is False


# ── List helpers ──────────────────────────────────────────────────────────

def _fake_completed(stdout: str = "", stderr: str = "", returncode: int = 0):
    p = MagicMock()
    p.stdout, p.stderr, p.returncode = stdout, stderr, returncode
    return p


def test_list_env_flink_statements_parses_json(script_module):
    fake = _fake_completed(stdout=json.dumps([
        {"name": "ol-orders00-enrich-0"},
        {"name": "ol-payments01-filter-1"},
        {"name": "user-query-3"},      # not ol-*; filter happens at the call site
    ]))
    with patch.object(script_module.subprocess, "run", return_value=fake):
        result = script_module._list_env_flink_statements()
    assert result == ["ol-orders00-enrich-0", "ol-payments01-filter-1", "user-query-3"]


def test_list_env_flink_statements_returns_empty_on_cli_failure(script_module):
    fake = _fake_completed(returncode=1, stderr="auth failed")
    with patch.object(script_module.subprocess, "run", return_value=fake):
        assert script_module._list_env_flink_statements() == []


def test_list_env_flink_statements_returns_empty_on_invalid_json(script_module):
    fake = _fake_completed(stdout="not json at all")
    with patch.object(script_module.subprocess, "run", return_value=fake):
        assert script_module._list_env_flink_statements() == []


def test_list_env_connectors_returns_name_id_tuples(script_module):
    fake = _fake_completed(stdout=json.dumps([
        {"name": "ol-orders00-datagen", "id": "lcc-aaaa"},
        {"name": "ol-payments01-datagen", "id": "lcc-bbbb"},
    ]))
    with patch.object(script_module.subprocess, "run", return_value=fake):
        result = script_module._list_env_connectors()
    assert result == [("ol-orders00-datagen", "lcc-aaaa"),
                      ("ol-payments01-datagen", "lcc-bbbb")]


def test_list_env_connectors_skips_unnamed(script_module):
    fake = _fake_completed(stdout=json.dumps([
        {"name": "ol-orders-datagen", "id": "lcc-1"},
        {"id": "lcc-2"},                  # no name → skipped
        {"name": "", "id": "lcc-3"},      # empty name → skipped
    ]))
    with patch.object(script_module.subprocess, "run", return_value=fake):
        result = script_module._list_env_connectors()
    assert result == [("ol-orders-datagen", "lcc-1")]


def test_list_env_topics_parses_json(script_module):
    fake = _fake_completed(stdout=json.dumps([
        {"name": "ol-orders00-t0"},
        {"name": "ol-orders00-t1"},
        {"name": "_internal"},
    ]))
    with patch.object(script_module.subprocess, "run", return_value=fake):
        assert script_module._list_env_topics() == ["ol-orders00-t0", "ol-orders00-t1", "_internal"]


def test_list_env_topics_returns_empty_on_cli_failure(script_module):
    with patch.object(script_module.subprocess, "run",
                      return_value=_fake_completed(returncode=1)):
        assert script_module._list_env_topics() == []


def test_list_sr_subjects_returns_empty_when_sr_unconfigured(script_module, monkeypatch):
    monkeypatch.setattr(script_module, "SR_ENDPOINT", "")
    assert script_module._list_sr_subjects() == []


def test_list_sr_subjects_parses_response(script_module, monkeypatch):
    monkeypatch.setattr(script_module, "SR_ENDPOINT", "https://sr.example")
    monkeypatch.setattr(script_module, "SR_KEY",      "K")
    monkeypatch.setattr(script_module, "SR_SECRET",   "S")
    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.json.return_value = ["ol-orders-t1-value", "ol-payments-t1-value", "user-subject-value"]
    with patch.object(script_module.httpx, "get", return_value=fake_resp):
        assert script_module._list_sr_subjects() == [
            "ol-orders-t1-value", "ol-payments-t1-value", "user-subject-value",
        ]


def test_list_sr_subjects_returns_empty_on_http_error(script_module, monkeypatch):
    monkeypatch.setattr(script_module, "SR_ENDPOINT", "https://sr.example")
    monkeypatch.setattr(script_module, "SR_KEY",      "K")
    monkeypatch.setattr(script_module, "SR_SECRET",   "S")
    fake_resp = MagicMock()
    fake_resp.status_code = 401
    with patch.object(script_module.httpx, "get", return_value=fake_resp):
        assert script_module._list_sr_subjects() == []


# ── Sweep filter integration: only ol-* names are touched ────────────────

def test_sweep_filter_only_matches_demo_names(script_module):
    """Concrete check that the sweep would never delete user-owned resources."""
    candidates = [
        "ol-orders00-t1",            # demo
        "ol-payments-enrich-0",      # demo
        "user-app-topic",            # user
        "_confluent-internal",       # CC internal
        "production-orders",         # user
    ]
    swept = [n for n in candidates if script_module._is_demo_name(n)]
    assert swept == ["ol-orders00-t1", "ol-payments-enrich-0"]

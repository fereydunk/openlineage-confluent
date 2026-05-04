"""Tests for the wizard's lazy-save flow (web/server.py).

Verifies that picking envs persists only env_id+env_name to confluent.selected_envs
without doing any CLI work, and that _configured_env_ids correctly unions
selected_envs (pending) with environments (provisioned).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml


@pytest.fixture()
def server_module(tmp_path, monkeypatch):
    """Load web/server.py with CONFIG_YML pointed at a tmp file."""
    repo_root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "_wiz_server_under_test", repo_root / "web" / "server.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "CONFIG_YML", tmp_path / "config.yaml")
    monkeypatch.setattr(mod, "EXAMPLE_YML", tmp_path / "config.example.yaml")
    return mod


def test_write_then_read_selected_envs_roundtrip(server_module):
    server_module._write_selected_envs([
        {"env_id": "env-aaa", "env_name": "prod"},
        {"env_id": "env-bbb", "env_name": "staging"},
    ])
    got = server_module._read_selected_envs()
    assert got == [
        {"env_id": "env-aaa", "env_name": "prod"},
        {"env_id": "env-bbb", "env_name": "staging"},
    ]


def test_write_empty_selection_removes_field(server_module):
    server_module._write_selected_envs([{"env_id": "env-aaa", "env_name": "prod"}])
    server_module._write_selected_envs([])
    raw = yaml.safe_load(server_module.CONFIG_YML.read_text())
    assert "selected_envs" not in (raw.get("confluent") or {})
    assert server_module._read_selected_envs() == []


def test_write_drops_entries_without_env_id(server_module):
    server_module._write_selected_envs([
        {"env_id": "env-aaa", "env_name": "ok"},
        {"env_name": "no id"},
        {"env_id": "", "env_name": "blank id"},
    ])
    assert server_module._read_selected_envs() == [{"env_id": "env-aaa", "env_name": "ok"}]


def test_configured_env_ids_unions_pending_and_provisioned(server_module):
    # Seed config.yaml with one fully-provisioned env and one pending one.
    cfg = {
        "confluent": {
            "environments": [
                {
                    "env_id": "env-prov",
                    "cluster_id": "lkc-prov",
                    "kafka_bootstrap": "pkc.us-west-2.aws.confluent.cloud:9092",
                    "kafka_api_key": "PROVKEY",
                    "schema_registry": {"endpoint": "https://sr", "api_key": "K", "api_secret": "S"},
                    "flink_compute_pool": "lfcp-1",
                },
            ],
            "selected_envs": [
                {"env_id": "env-pending", "env_name": "stg"},
            ],
        },
    }
    server_module.CONFIG_YML.write_text(yaml.safe_dump(cfg))

    got = server_module._configured_env_ids()
    by_id = {e["env_id"]: e for e in got}

    assert by_id["env-prov"] == {
        "env_id": "env-prov",
        "has_kafka_creds": True,
        "has_sr": True,
        "flink_pool": "lfcp-1",
        "pending": False,
    }
    assert by_id["env-pending"] == {
        "env_id": "env-pending",
        "has_kafka_creds": False,
        "has_sr": False,
        "flink_pool": "",
        "pending": True,
    }


def test_configured_env_ids_dedupes_when_provisioned_and_pending(server_module):
    # If an env_id appears in both lists, the provisioned entry wins (pending=False).
    cfg = {
        "confluent": {
            "environments": [
                {
                    "env_id": "env-dup",
                    "cluster_id": "lkc-dup",
                    "kafka_bootstrap": "pkc:9092",
                    "kafka_api_key": "K",
                },
            ],
            "selected_envs": [
                {"env_id": "env-dup", "env_name": "should-be-shadowed"},
            ],
        },
    }
    server_module.CONFIG_YML.write_text(yaml.safe_dump(cfg))

    got = server_module._configured_env_ids()
    assert len(got) == 1
    assert got[0]["env_id"] == "env-dup"
    assert got[0]["pending"] is False


def test_write_preserves_other_yaml_sections(server_module):
    # An existing config.yaml with openlineage + pipeline blocks should not lose them.
    server_module.CONFIG_YML.write_text(yaml.safe_dump({
        "confluent": {"CONFLUENT_CLOUD_API_KEY": "K", "CONFLUENT_CLOUD_API_SECRET": "S"},
        "openlineage": {"OPENLINEAGE_URL": "http://localhost:5000"},
        "pipeline": {"PIPELINE_POLL_INTERVAL": 60},
    }))

    server_module._write_selected_envs([{"env_id": "env-x", "env_name": "x"}])
    raw = yaml.safe_load(server_module.CONFIG_YML.read_text())

    assert raw["openlineage"]["OPENLINEAGE_URL"] == "http://localhost:5000"
    assert raw["pipeline"]["PIPELINE_POLL_INTERVAL"] == 60
    assert raw["confluent"]["CONFLUENT_CLOUD_API_KEY"] == "K"
    assert raw["confluent"]["selected_envs"] == [{"env_id": "env-x", "env_name": "x"}]

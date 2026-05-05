"""Tests for env hot-reload — picks up wizard-added envs without restart.

The pipeline re-reads its config file at the top of each cycle. If the env
signature set changed (env added, removed, or its credentials rotated), the
underlying ConfluentLineageClient is rebuilt. The emitter and its diff state
are intentionally left untouched so lineage continuity survives the rebuild.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
import yaml

from openlineage_confluent.config import (
    AppConfig,
    ConfluentConfig,
    EnvDeployment,
    OpenLineageConfig,
    PipelineConfig,
)
from openlineage_confluent.pipeline import LineagePipeline, _env_signature, _envs_signature

# ── Fixtures ────────────────────────────────────────────────────────────────

def _write_config(path, env_entries: list[dict]) -> None:
    raw = {
        "confluent": {
            "CONFLUENT_CLOUD_API_KEY":    "K",
            "CONFLUENT_CLOUD_API_SECRET": "S",
            "environments":               env_entries,
        },
        "openlineage": {"OPENLINEAGE_TRANSPORT": "console"},
        "pipeline":    {"PIPELINE_POLL_INTERVAL": 60},
    }
    path.write_text(yaml.safe_dump(raw))


@pytest.fixture()
def pipeline_with_one_env(tmp_path):
    config_path = tmp_path / "config.yaml"
    _write_config(config_path, [
        {"env_id": "env-aaa", "cluster_id": "lkc-aaa",
         "kafka_bootstrap": "pkc-aaa:9092"},
    ])
    cfg = AppConfig.from_yaml(config_path)
    # Stub all collaborators so __init__ doesn't try real network/IO.
    with patch("openlineage_confluent.pipeline.ConfluentLineageClient") as cli_cls, \
         patch("openlineage_confluent.pipeline.ConfluentOpenLineageMapper"), \
         patch("openlineage_confluent.pipeline.LineageEmitter"), \
         patch("openlineage_confluent.pipeline.StateStore"):
        pipe = LineagePipeline(cfg, config_path=config_path)
        # Capture the constructor and reset call_count so we can assert rebuilds.
        cli_cls.reset_mock()
        yield pipe, config_path, cli_cls


# ── Signature helpers ──────────────────────────────────────────────────────

def test_env_signature_stable_across_pydantic_loads():
    e1 = EnvDeployment(env_id="env-1", cluster_id="lkc-1", kafka_bootstrap="b:9092")
    e2 = EnvDeployment(env_id="env-1", cluster_id="lkc-1", kafka_bootstrap="b:9092")
    assert _env_signature(e1) == _env_signature(e2)


def test_envs_signature_is_order_insensitive():
    e1 = EnvDeployment(env_id="env-a", cluster_id="lkc-a", kafka_bootstrap="b:9092")
    e2 = EnvDeployment(env_id="env-b", cluster_id="lkc-b", kafka_bootstrap="b:9092")
    cfg_ab = ConfluentConfig(CONFLUENT_CLOUD_API_KEY="k", CONFLUENT_CLOUD_API_SECRET="s",
                             environments=[e1, e2])
    cfg_ba = ConfluentConfig(CONFLUENT_CLOUD_API_KEY="k", CONFLUENT_CLOUD_API_SECRET="s",
                             environments=[e2, e1])
    assert _envs_signature(cfg_ab) == _envs_signature(cfg_ba)


def test_envs_signature_changes_on_credential_rotation():
    e1 = EnvDeployment(env_id="env-a", cluster_id="lkc-a", kafka_bootstrap="b:9092",
                       kafka_api_key="K1", kafka_api_secret="S1")
    e2 = EnvDeployment(env_id="env-a", cluster_id="lkc-a", kafka_bootstrap="b:9092",
                       kafka_api_key="K2", kafka_api_secret="S2")
    cfg1 = ConfluentConfig(CONFLUENT_CLOUD_API_KEY="k", CONFLUENT_CLOUD_API_SECRET="s",
                           environments=[e1])
    cfg2 = ConfluentConfig(CONFLUENT_CLOUD_API_KEY="k", CONFLUENT_CLOUD_API_SECRET="s",
                           environments=[e2])
    assert _envs_signature(cfg1) != _envs_signature(cfg2)


# ── Hot-reload behaviour ───────────────────────────────────────────────────

def test_unchanged_config_does_not_rebuild_client(pipeline_with_one_env):
    pipe, _, cli_cls = pipeline_with_one_env
    pipe._maybe_reload_envs()
    assert cli_cls.call_count == 0


def test_added_env_rebuilds_client(pipeline_with_one_env):
    pipe, config_path, cli_cls = pipeline_with_one_env
    _write_config(config_path, [
        {"env_id": "env-aaa", "cluster_id": "lkc-aaa", "kafka_bootstrap": "pkc-aaa:9092"},
        {"env_id": "env-bbb", "cluster_id": "lkc-bbb", "kafka_bootstrap": "pkc-bbb:9092"},
    ])
    pipe._maybe_reload_envs()
    assert cli_cls.call_count == 1
    assert {e.env_id for e in pipe._cfg.confluent.environments} == {"env-aaa", "env-bbb"}


def test_removed_env_rebuilds_client(pipeline_with_one_env):
    pipe, config_path, cli_cls = pipeline_with_one_env
    _write_config(config_path, [])    # all envs removed
    pipe._maybe_reload_envs()
    assert cli_cls.call_count == 1
    assert pipe._cfg.confluent.environments == []


def test_rotated_credentials_rebuild_client(pipeline_with_one_env):
    pipe, config_path, cli_cls = pipeline_with_one_env
    _write_config(config_path, [
        {"env_id": "env-aaa", "cluster_id": "lkc-aaa", "kafka_bootstrap": "pkc-aaa:9092",
         "kafka_api_key": "ROTATED", "kafka_api_secret": "ROTATEDSECRET"},
    ])
    pipe._maybe_reload_envs()
    assert cli_cls.call_count == 1


def test_invalid_yaml_keeps_existing_client(pipeline_with_one_env):
    pipe, config_path, cli_cls = pipeline_with_one_env
    config_path.write_text(":::: invalid yaml ::::")
    pipe._maybe_reload_envs()
    assert cli_cls.call_count == 0
    # The pipeline kept its prior cfg
    assert {e.env_id for e in pipe._cfg.confluent.environments} == {"env-aaa"}


def test_pipeline_without_config_path_never_reloads(tmp_path):
    cfg = AppConfig(
        confluent=ConfluentConfig(CONFLUENT_CLOUD_API_KEY="k", CONFLUENT_CLOUD_API_SECRET="s"),
        openlineage=OpenLineageConfig(OPENLINEAGE_TRANSPORT="console"),
        pipeline=PipelineConfig(PIPELINE_POLL_INTERVAL=60, PIPELINE_STATE_DB=tmp_path / "s.db"),
    )
    with patch("openlineage_confluent.pipeline.ConfluentLineageClient") as cli_cls, \
         patch("openlineage_confluent.pipeline.ConfluentOpenLineageMapper"), \
         patch("openlineage_confluent.pipeline.LineageEmitter"), \
         patch("openlineage_confluent.pipeline.StateStore"):
        pipe = LineagePipeline(cfg)    # no config_path
        cli_cls.reset_mock()
        pipe._maybe_reload_envs()
        assert cli_cls.call_count == 0


def test_emitter_state_survives_reload(pipeline_with_one_env):
    """Rebuilding the lineage client must NOT touch the emitter or state store."""
    pipe, config_path, _ = pipeline_with_one_env
    emitter_before = pipe._emitter
    store_before   = pipe._store

    _write_config(config_path, [
        {"env_id": "env-aaa", "cluster_id": "lkc-aaa", "kafka_bootstrap": "pkc-aaa:9092"},
        {"env_id": "env-bbb", "cluster_id": "lkc-bbb", "kafka_bootstrap": "pkc-bbb:9092"},
    ])
    pipe._maybe_reload_envs()

    assert pipe._emitter is emitter_before
    assert pipe._store   is store_before

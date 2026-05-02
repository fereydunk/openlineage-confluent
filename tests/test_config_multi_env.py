"""Tests for the multi-env config schema and the legacy single-env shim."""

from __future__ import annotations

import textwrap
from pathlib import Path

from openlineage_confluent.config import AppConfig


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(body))
    return p


# ── New multi-env shape ──────────────────────────────────────────────────────

def test_multi_env_config_loads(tmp_path: Path) -> None:
    cfg_path = _write(tmp_path, """\
        confluent:
          CONFLUENT_CLOUD_API_KEY:    "k"
          CONFLUENT_CLOUD_API_SECRET: "s"
          environments:
            - env_id: env-a
              cluster_id: lkc-a
              kafka_bootstrap: pkc-a.us-west-2.aws.confluent.cloud:9092
            - env_id: env-b
              cluster_id: lkc-b
              kafka_bootstrap: pkc-b.eu-central-1.aws.confluent.cloud:9092
              schema_registry:
                endpoint:   "https://psrc-b.eu-central-1.aws.confluent.cloud"
                api_key:    "sr-key-b"
                api_secret: "sr-secret-b"
        openlineage:
          OPENLINEAGE_KAFKA_BOOTSTRAP: "fallback.confluent.cloud:9092"
        """)
    cfg = AppConfig.from_yaml(cfg_path)

    assert len(cfg.confluent.environments) == 2
    assert cfg.confluent.environments[0].env_id == "env-a"
    assert cfg.confluent.environments[0].kafka_bootstrap.startswith("pkc-a")
    assert cfg.confluent.environments[1].schema_registry is not None
    assert cfg.confluent.environments[1].schema_registry.api_key == "sr-key-b"


# ── Legacy shim: top-level env fields synthesize a single-element list ───────

def test_legacy_single_env_config_still_loads(tmp_path: Path) -> None:
    cfg_path = _write(tmp_path, """\
        confluent:
          CONFLUENT_CLOUD_API_KEY:    "k"
          CONFLUENT_CLOUD_API_SECRET: "s"
          CONFLUENT_ENV_ID:           "env-legacy"
          CONFLUENT_CLUSTER_ID:       "lkc-legacy"
          CONFLUENT_KAFKA_REST_ENDPOINT: "https://pkc-legacy.us-west-2.aws.confluent.cloud:443"
          CONFLUENT_KAFKA_API_KEY:    "kafka-key-legacy"
          CONFLUENT_KAFKA_API_SECRET: "kafka-secret-legacy"
          schema_registry:
            endpoint:   "https://psrc-legacy.us-west-2.aws.confluent.cloud"
            api_key:    "sr-key-legacy"
            api_secret: "sr-secret-legacy"
        openlineage:
          OPENLINEAGE_KAFKA_BOOTSTRAP: "pkc-legacy.us-west-2.aws.confluent.cloud:9092"
        """)
    cfg = AppConfig.from_yaml(cfg_path)

    envs = cfg.confluent.environments
    assert len(envs) == 1
    e = envs[0]
    assert e.env_id == "env-legacy"
    assert e.cluster_id == "lkc-legacy"
    assert e.kafka_bootstrap == "pkc-legacy.us-west-2.aws.confluent.cloud:9092"
    assert e.kafka_rest_endpoint and "pkc-legacy" in e.kafka_rest_endpoint
    assert e.kafka_api_key == "kafka-key-legacy"
    assert e.schema_registry is not None
    assert e.schema_registry.api_key == "sr-key-legacy"


def test_legacy_shim_is_no_op_when_environments_already_set(tmp_path: Path) -> None:
    """If environments: is populated, legacy fields (if accidentally present) are ignored."""
    cfg_path = _write(tmp_path, """\
        confluent:
          CONFLUENT_CLOUD_API_KEY:    "k"
          CONFLUENT_CLOUD_API_SECRET: "s"
          environments:
            - env_id: env-new
              cluster_id: lkc-new
              kafka_bootstrap: pkc-new.us-west-2.aws.confluent.cloud:9092
        openlineage:
          OPENLINEAGE_KAFKA_BOOTSTRAP: "anything"
        """)
    cfg = AppConfig.from_yaml(cfg_path)
    assert len(cfg.confluent.environments) == 1
    assert cfg.confluent.environments[0].env_id == "env-new"

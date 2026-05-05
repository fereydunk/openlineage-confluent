"""Coverage for the gaps surfaced by the deep-review session.

Five focused areas:
  1. /envs/select HTTP handler — dedup branch + auth gate
  2. _bridge_provision_pending — failure paths (provision returns _error,
     _upsert_environment raises, uncaught exception in _provision_env)
  3. ConfluentLineageClient.get_lineage_graph — global ksqlDB and
     self-managed-Connect failures land in graph.failed_namespaces
  4. _envs_signature — Schema Registry credential rotation triggers a
     signature change (Kafka rotation already covered)
  5. provision_demo_pipelines.teardown — runs Phase 2 sweep even when
     state file is missing
"""

from __future__ import annotations

import importlib.util
import json
import sys
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml


# ── 1. /envs/select HTTP handler ──────────────────────────────────────────────

@pytest.fixture()
def server_module(tmp_path, monkeypatch):
    """Load web/server.py with CONFIG_YML pointed at a tmp file."""
    repo = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "_server_review_gaps", repo / "web" / "server.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "CONFIG_YML",  tmp_path / "config.yaml")
    monkeypatch.setattr(mod, "EXAMPLE_YML", tmp_path / "config.example.yaml")
    return mod


def _post_json(handler_cls, server_mod, path: str, body: dict, signed_in: bool = True):
    """Drive Handler.do_POST with an in-memory request. Returns (status, json)."""
    payload = json.dumps(body).encode()

    request_lines = [
        f"POST {path} HTTP/1.1",
        "Host: localhost",
        "Content-Type: application/json",
        f"Content-Length: {len(payload)}",
        "",
        "",
    ]
    raw_request = ("\r\n".join(request_lines)).encode() + payload

    rfile = BytesIO(raw_request)
    wfile = BytesIO()

    handler = handler_cls.__new__(handler_cls)
    handler.rfile = rfile
    handler.wfile = wfile
    handler.client_address = ("127.0.0.1", 0)
    handler.requestline = f"POST {path} HTTP/1.1"
    handler.command = "POST"
    handler.path = path
    handler.request_version = "HTTP/1.1"
    # Drive the BaseHTTPRequestHandler header parsing
    rfile.seek(0)
    rfile.readline()    # consume request-line; raw_requestline is set below
    handler.raw_requestline = (request_lines[0] + "\r\n").encode()
    handler.parse_request()

    with patch.object(server_mod, "_current_user",
                      return_value="vahid@confluent.io" if signed_in else ""):
        handler.do_POST()

    response = wfile.getvalue().decode()
    head, _, body_str = response.partition("\r\n\r\n")
    status_line = head.split("\r\n", 1)[0]
    status = int(status_line.split()[1])
    return status, json.loads(body_str) if body_str.strip() else {}


def test_envs_select_returns_401_when_not_signed_in(server_module):
    status, body = _post_json(
        server_module.Handler, server_module,
        "/envs/select", {"envs": [{"env_id": "env-aaa", "env_name": "prod"}]},
        signed_in=False,
    )
    assert status == 401
    assert "not signed in" in body["error"].lower()


def test_envs_select_dedupes_already_provisioned_env(server_module):
    """A user re-checking an env that's already in `environments:` must not
    shadow it with a `selected_envs` entry — that would force a re-mint of
    Kafka + SR keys on the next Start."""
    server_module.CONFIG_YML.write_text(yaml.safe_dump({
        "confluent": {
            "environments": [{
                "env_id": "env-aaa", "cluster_id": "lkc-aaa",
                "kafka_bootstrap": "pkc:9092", "kafka_api_key": "K",
            }],
        },
    }))

    status, body = _post_json(
        server_module.Handler, server_module,
        "/envs/select",
        {"envs": [
            {"env_id": "env-aaa", "env_name": "prod"},      # already provisioned
            {"env_id": "env-bbb", "env_name": "stg"},       # new
        ]},
    )
    assert status == 200
    assert all(r["ok"] for r in body["results"])

    raw = yaml.safe_load(server_module.CONFIG_YML.read_text())
    selected = raw["confluent"].get("selected_envs", [])
    selected_ids = [e["env_id"] for e in selected]
    assert "env-aaa" not in selected_ids, "shadowed an already-provisioned env"
    assert "env-bbb" in selected_ids


def test_envs_select_persists_pending_envs(server_module):
    status, _ = _post_json(
        server_module.Handler, server_module,
        "/envs/select",
        {"envs": [{"env_id": "env-xxx", "env_name": "test"}]},
    )
    assert status == 200
    assert server_module._read_selected_envs() == [
        {"env_id": "env-xxx", "env_name": "test"}
    ]


# ── 2. _bridge_provision_pending failure paths ────────────────────────────────

def test_bridge_provision_pending_returns_true_when_no_pending(server_module):
    server_module._write_selected_envs([])
    assert server_module._bridge_provision_pending() is True


def test_bridge_provision_pending_aborts_on_provision_error(server_module):
    server_module._write_selected_envs([
        {"env_id": "env-aaa", "env_name": "prod"},
        {"env_id": "env-bbb", "env_name": "stg"},
    ])

    def fake_provision(env_id, env_name):
        if env_id == "env-aaa":
            return {"env_id": env_id, "_log": ["x"], "_error": "no clusters in env"}
        return {"env_id": env_id, "_log": [], "cluster_id": "lkc-x",
                "kafka_bootstrap": "pkc:9092", "kafka_api_key": "K",
                "kafka_api_secret": "S"}

    with patch.object(server_module, "_provision_env", side_effect=fake_provision):
        ok = server_module._bridge_provision_pending()

    assert ok is False
    # env-aaa stays pending, env-bbb stays pending too (we abort on first error)
    pending_ids = {e["env_id"] for e in server_module._read_selected_envs()}
    assert "env-aaa" in pending_ids
    assert "env-bbb" in pending_ids


def test_bridge_provision_pending_aborts_on_upsert_failure(server_module):
    server_module._write_selected_envs([{"env_id": "env-aaa", "env_name": "prod"}])

    good_entry = {"env_id": "env-aaa", "_log": [], "cluster_id": "lkc-x",
                  "kafka_bootstrap": "pkc:9092", "kafka_api_key": "K",
                  "kafka_api_secret": "S"}

    with patch.object(server_module, "_provision_env", return_value=good_entry), \
         patch.object(server_module, "_upsert_environment",
                      side_effect=PermissionError("disk full")):
        ok = server_module._bridge_provision_pending()

    assert ok is False
    # env-aaa stays in selected_envs (write failed → state un-changed)
    assert any(e["env_id"] == "env-aaa" for e in server_module._read_selected_envs())


def test_bridge_provision_pending_handles_uncaught_exception(server_module):
    server_module._write_selected_envs([{"env_id": "env-aaa", "env_name": "prod"}])

    with patch.object(server_module, "_provision_env",
                      side_effect=RuntimeError("CLI vanished")):
        ok = server_module._bridge_provision_pending()

    assert ok is False


def test_bridge_provision_pending_succeeds_for_all_envs(server_module):
    server_module._write_selected_envs([
        {"env_id": "env-aaa", "env_name": "prod"},
        {"env_id": "env-bbb", "env_name": "stg"},
    ])

    def fake_provision(env_id, env_name):
        return {"env_id": env_id, "_log": [], "cluster_id": f"lkc-{env_id[-3:]}",
                "kafka_bootstrap": "pkc:9092", "kafka_api_key": "K",
                "kafka_api_secret": "S"}

    with patch.object(server_module, "_provision_env", side_effect=fake_provision):
        ok = server_module._bridge_provision_pending()

    assert ok is True
    # Both envs moved out of selected_envs, both now in environments
    assert server_module._read_selected_envs() == []
    raw = yaml.safe_load(server_module.CONFIG_YML.read_text())
    env_ids = {e["env_id"] for e in raw["confluent"]["environments"]}
    assert env_ids == {"env-aaa", "env-bbb"}


# ── 3. Global ksqlDB / self-managed-Connect failed_namespaces ──────────────

def test_global_graph_quarantines_ksqldb_namespace_on_client_failure():
    from openlineage_confluent.confluent.client import ConfluentLineageClient
    from openlineage_confluent.config import ConfluentConfig, KsqlClusterConfig

    cfg = ConfluentConfig(
        CONFLUENT_CLOUD_API_KEY="k", CONFLUENT_CLOUD_API_SECRET="s",
        environments=[],
        ksql_clusters=[
            KsqlClusterConfig(cluster_id="lksqlc-test", rest_endpoint="https://ksql",
                              api_key="K", api_secret="S"),
        ],
    )
    client = ConfluentLineageClient(cfg)
    # Force the ksql client to look unhealthy
    client._ksql_clients[0].last_ok = False
    client._ksql_clients[0].get_queries = MagicMock(return_value=[])

    graph = client.get_lineage_graph()
    assert "ksqldb://lksqlc-test" in graph.failed_namespaces


def test_global_graph_quarantines_self_managed_connect_on_failure():
    from openlineage_confluent.confluent.client import ConfluentLineageClient
    from openlineage_confluent.config import ConfluentConfig, SelfManagedConnectClusterConfig

    cfg = ConfluentConfig(
        CONFLUENT_CLOUD_API_KEY="k", CONFLUENT_CLOUD_API_SECRET="s",
        environments=[],
        self_managed_connect_clusters=[
            SelfManagedConnectClusterConfig(name="on-prem-etl",
                                            rest_endpoint="http://connect:8083"),
        ],
    )
    client = ConfluentLineageClient(cfg)
    client._sm_connect_clients[0].last_ok = False
    client._sm_connect_clients[0].get_connectors = MagicMock(return_value=[])

    graph = client.get_lineage_graph()
    assert "kafka-connect://on-prem-etl" in graph.failed_namespaces


def test_global_graph_no_failed_namespaces_when_all_ok():
    from openlineage_confluent.confluent.client import ConfluentLineageClient
    from openlineage_confluent.config import ConfluentConfig, KsqlClusterConfig

    cfg = ConfluentConfig(
        CONFLUENT_CLOUD_API_KEY="k", CONFLUENT_CLOUD_API_SECRET="s",
        environments=[],
        ksql_clusters=[
            KsqlClusterConfig(cluster_id="lksqlc-test", rest_endpoint="https://ksql",
                              api_key="K", api_secret="S"),
        ],
    )
    client = ConfluentLineageClient(cfg)
    client._ksql_clients[0].last_ok = True
    client._ksql_clients[0].get_queries = MagicMock(return_value=[])

    graph = client.get_lineage_graph()
    assert graph.failed_namespaces == set()


# ── 4. _envs_signature — Schema Registry rotation ─────────────────────────────

def test_envs_signature_changes_on_sr_credential_rotation():
    from openlineage_confluent.config import ConfluentConfig, EnvDeployment, SchemaRegistryConfig
    from openlineage_confluent.pipeline import _envs_signature

    base = dict(env_id="env-a", cluster_id="lkc-a", kafka_bootstrap="pkc:9092")
    sig1 = _envs_signature(ConfluentConfig(
        CONFLUENT_CLOUD_API_KEY="k", CONFLUENT_CLOUD_API_SECRET="s",
        environments=[EnvDeployment(
            **base,
            schema_registry=SchemaRegistryConfig(endpoint="https://sr",
                                                 api_key="SR1", api_secret="OLD"),
        )],
    ))
    sig2 = _envs_signature(ConfluentConfig(
        CONFLUENT_CLOUD_API_KEY="k", CONFLUENT_CLOUD_API_SECRET="s",
        environments=[EnvDeployment(
            **base,
            schema_registry=SchemaRegistryConfig(endpoint="https://sr",
                                                 api_key="SR1", api_secret="NEW"),
        )],
    ))
    assert sig1 != sig2


def test_envs_signature_changes_when_sr_added_to_an_env():
    from openlineage_confluent.config import ConfluentConfig, EnvDeployment, SchemaRegistryConfig
    from openlineage_confluent.pipeline import _envs_signature

    base = dict(env_id="env-a", cluster_id="lkc-a", kafka_bootstrap="pkc:9092")
    sig_no_sr = _envs_signature(ConfluentConfig(
        CONFLUENT_CLOUD_API_KEY="k", CONFLUENT_CLOUD_API_SECRET="s",
        environments=[EnvDeployment(**base)],
    ))
    sig_with_sr = _envs_signature(ConfluentConfig(
        CONFLUENT_CLOUD_API_KEY="k", CONFLUENT_CLOUD_API_SECRET="s",
        environments=[EnvDeployment(
            **base,
            schema_registry=SchemaRegistryConfig(endpoint="https://sr",
                                                 api_key="SR", api_secret="S"),
        )],
    ))
    assert sig_no_sr != sig_with_sr


# ── 5. teardown sweeps even when state file missing ─────────────────────────

@pytest.fixture()
def provdemo_module():
    spec = importlib.util.spec_from_file_location(
        "_provdemo_review_gaps",
        Path(__file__).resolve().parent.parent / "scripts" / "provision_demo_pipelines.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_teardown_runs_phase_2_sweep_when_state_file_missing(provdemo_module, tmp_path, monkeypatch):
    """Even with no state file, teardown should still discover and delete
    ol-* resources via the stateless sweep — that's the whole point."""
    monkeypatch.setattr(provdemo_module, "STATE_FILE", tmp_path / "missing.json")
    monkeypatch.setattr(provdemo_module, "PIDS_FILE",  tmp_path / "missing.pids")
    monkeypatch.setattr(provdemo_module, "WORKER_SCRIPT", tmp_path / "worker.py")
    monkeypatch.setattr(provdemo_module, "ENV_ID",     "env-test")
    monkeypatch.setattr(provdemo_module, "CLUSTER_ID", "lkc-test")
    monkeypatch.setattr(provdemo_module, "SR_ENDPOINT", "")    # skip SR sweep

    monkeypatch.setattr(provdemo_module, "_list_env_flink_statements",
                        lambda: ["ol-orders00-enrich-0", "user-real-query"])
    monkeypatch.setattr(provdemo_module, "_list_env_connectors",
                        lambda: [("ol-orders00-datagen", "lcc-1"), ("user-jdbc", "lcc-2")])
    monkeypatch.setattr(provdemo_module, "_list_env_topics",
                        lambda: ["ol-orders00-t0", "ol-orders00-t1", "user-prod-topic"])

    delete_calls: list[list[str]] = []
    def fake_run(args, *, capture=True):
        delete_calls.append(args)
        m = MagicMock()
        m.returncode, m.stdout, m.stderr = 0, "", ""
        return m

    monkeypatch.setattr(provdemo_module, "run", fake_run)

    provdemo_module.teardown()

    # Each ol-* resource got a delete; the user-owned ones did NOT.
    flink_deletes = [a for a in delete_calls
                     if "flink" in a and "delete" in a]
    assert any("ol-orders00-enrich-0" in a for a in flink_deletes)
    assert not any("user-real-query" in a for a in flink_deletes)

    connector_deletes = [a for a in delete_calls
                         if "connect" in a and "cluster" in a and "delete" in a]
    assert any("lcc-1" in a for a in connector_deletes)
    assert not any("lcc-2" in a for a in connector_deletes)

    # Topics: phase 3 nukes EVERY non-system topic in the env (per user
    # decision 2026-05-04 — teardown is for dedicated test envs and must
    # leave nothing behind, including dlq-lcc-*/error-lcc-*/success-lcc-*
    # connect-spawned topics and any other user-owned topics).
    topic_deletes = [a for a in delete_calls
                     if "kafka" in a and "topic" in a and "delete" in a]
    deleted_topics = {a[a.index("delete") + 1] for a in topic_deletes}
    assert "ol-orders00-t0" in deleted_topics
    assert "ol-orders00-t1" in deleted_topics
    assert "user-prod-topic" in deleted_topics

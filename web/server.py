"""OpenLineage-Confluent setup wizard.

Single-page web UI on http://localhost:8892 that walks the user through a
four-card flow: Marquez control → CC sign-in → multi-env selection (lazy save
to confluent.selected_envs; provisioning deferred until Start) → demo-pipeline
provisioning + bridge runner. Mints Kafka + SR API keys on demand and writes
them into config.yaml's `environments:` list.

Endpoints:
  GET  /                 — HTML wizard page
  GET  /status           — {user, env_count}
  GET  /environments     — `confluent environment list -o json`
  GET  /envs/configured  — currently-configured envs (applies legacy shim)
  GET  /marquez/status   — marquez + patched UI process state
  GET  /loadtest/status  — demo-pipeline provisioner state
  GET  /loadtest/stream  — SSE log of provisioner subprocess
  GET  /bridge/status    — bridge phase + run state
  GET  /bridge/stream    — SSE log of bridge provisioning + ol-confluent run
  POST /cc-login         — {email, password} → `confluent login --save`
  POST /envs/select      — [{env_id, env_name}] → writes confluent.selected_envs
  POST /marquez/{up,down,wipe} — docker compose up/down (or down -v + up -d for wipe) + patched UI dev server
  POST /loadtest/{start,stop,teardown} — provision-demo-pipelines worker
  POST /bridge/{start,stop} — `ol-confluent run` worker (provisions pending
                              envs first, then spawns long-lived subprocess)
"""

from __future__ import annotations

import json
import os
import re
import select
import shlex
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

PORT       = 8892
REPO_DIR   = Path(__file__).resolve().parent.parent
CONFIG_YML = REPO_DIR / "config.yaml"
EXAMPLE_YML = REPO_DIR / "config.example.yaml"

# Patched Marquez UI lives in a sibling repo. The upstream marquezproject/marquez-web
# image (port 3000 in docker-compose.yml) renders generic job labels and is missing
# the typed CONNECTOR/FLINK/CONSUMER/PRODUCER pills + topic labels we need for
# the demo. The patched fork's webpack dev server runs on :1337.
PATCHED_UI_DIR = Path(os.environ.get(
    "MARQUEZ_UI_PATCH_DIR",
    str(Path.home() / "marquez-ui-patch" / "web"),
))
PATCHED_UI_PORT = 1337
PATCHED_UI_LOG = Path("/tmp/marquez-ui-patch-dev.log")

# ── Load-test process state (single concurrent run) ──────────────────────────
# A single dict guarded by _lt_lock holds the running subprocess + a ring
# buffer of recent stdout lines. /loadtest/stream tails this buffer via SSE.
_LT_RING_SIZE = 2_000
_lt_lock = threading.Lock()
_lt_state: dict = {
    "proc":   None,        # subprocess.Popen | None
    "args":   [],          # last-launched argv (for display)
    "lines":  deque(maxlen=_LT_RING_SIZE),
    "done":   False,
    "rc":     None,
    "started_at": None,
}

# ── Bridge process state (separate from load-test; both can run concurrently)
# Phases: idle → provisioning → running → done. The proc handle is None during
# provisioning; the launcher thread sets it once `ol-confluent run` is spawned.
_BRIDGE_RING_SIZE = 2_000
_bridge_lock = threading.Lock()
_bridge_state: dict = {
    "proc":       None,
    "phase":      "idle",        # idle | provisioning | running | done
    "args":       [],
    "lines":      deque(maxlen=_BRIDGE_RING_SIZE),
    "done":       False,
    "rc":         None,
    "started_at": None,
}


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _run_confluent(args: list[str], env: dict | None = None,
                   timeout: int = 30) -> tuple[int, str, str]:
    """Run `confluent <args>` and return (rc, stdout, stderr)."""
    try:
        proc = subprocess.run(
            ["confluent", *args],
            env={**os.environ, **(env or {})},
            capture_output=True, text=True, timeout=timeout,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except FileNotFoundError:
        return 127, "", "`confluent` CLI not found in PATH"
    except subprocess.TimeoutExpired:
        return 124, "", f"timed out after {timeout}s"


def _current_user() -> str:
    """Return the currently logged-in CC user email, or '' if not logged in."""
    rc, out, _ = _run_confluent(["context", "list", "-o", "json"], timeout=8)
    if rc != 0:
        return ""
    try:
        for ctx in json.loads(out):
            if ctx.get("is_current"):
                # name format: "login-<email>-https://confluent.cloud"
                m = re.match(r"login-([^-]+@[^-]+)-", ctx.get("name", ""))
                if m:
                    return m.group(1)
                return ctx.get("name", "")
    except Exception:
        pass
    return ""


def _list_environments() -> list[dict]:
    """Return [{id, name}] for all CC environments visible to current user."""
    rc, out, err = _run_confluent(["environment", "list", "-o", "json"], timeout=15)
    if rc != 0:
        return []
    try:
        envs = json.loads(out)
    except json.JSONDecodeError:
        return []
    return [{"id": e.get("id", ""), "name": e.get("name", "")}
            for e in envs if e.get("id")]


def _read_config_value(key: str) -> str:
    """Read a top-level config.yaml value. Returns '' if not present."""
    if not CONFIG_YML.exists():
        return ""
    pat = re.compile(rf'^\s*{re.escape(key)}\s*:\s*"?([^"\n]*)"?\s*$', re.M)
    m = pat.search(CONFIG_YML.read_text())
    return m.group(1).strip() if m else ""


def _upsert_config_value(key: str, val: str) -> None:
    """Set `key: "val"` in config.yaml under confluent: section.

    Bootstraps from config.example.yaml if config.yaml doesn't exist.
    Only mutates the named key — preserves all other lines + comments.
    """
    if not CONFIG_YML.exists():
        if EXAMPLE_YML.exists():
            CONFIG_YML.write_text(EXAMPLE_YML.read_text())
        else:
            CONFIG_YML.write_text("confluent:\n")

    text = CONFIG_YML.read_text()
    pat = re.compile(rf'^(\s*{re.escape(key)}\s*:\s*).*$', re.M)
    new_line = rf'\g<1>"{val}"'
    if pat.search(text):
        text = pat.sub(new_line, text)
    else:
        # Append under `confluent:` section (assume it exists)
        text = re.sub(
            r'^(confluent:\s*\n)',
            rf'\1  {key}: "{val}"\n',
            text, count=1, flags=re.M,
        )
    CONFIG_YML.write_text(text)


# ──────────────────────────────────────────────────────────────────────────────
# Per-env auto-provisioning (mint Kafka + SR API keys, populate config.yaml)
# ──────────────────────────────────────────────────────────────────────────────

def _run_confluent_json(args: list[str], timeout: int = 30) -> tuple[bool, object, str]:
    """Run a `confluent ... -o json` command. Returns (ok, parsed_json, error_msg)."""
    rc, out, err = _run_confluent(args, timeout=timeout)
    if rc != 0:
        return False, None, (err or out).strip()[:300]
    try:
        return True, json.loads(out), ""
    except json.JSONDecodeError as exc:
        return False, None, f"invalid JSON from `confluent {' '.join(args[:3])}`: {exc}"


def _list_clusters(env_id: str) -> tuple[list[dict], str]:
    """Return [{id, name, ...}, ...] for Kafka clusters in env. Empty + error msg on failure."""
    ok, data, err = _run_confluent_json(
        ["kafka", "cluster", "list", "--environment", env_id, "-o", "json"]
    )
    if not ok or not isinstance(data, list):
        return [], err or "no clusters returned"
    return data, ""


def _describe_cluster(cluster_id: str, env_id: str) -> tuple[dict, str]:
    ok, data, err = _run_confluent_json(
        ["kafka", "cluster", "describe", cluster_id,
         "--environment", env_id, "-o", "json"]
    )
    if not ok or not isinstance(data, dict):
        return {}, err or "cluster describe failed"
    return data, ""


def _describe_sr(env_id: str) -> tuple[dict, str]:
    """Schema Registry endpoint + lsrc-id for an env. Empty dict if SR not enabled."""
    ok, data, err = _run_confluent_json(
        ["schema-registry", "cluster", "describe",
         "--environment", env_id, "-o", "json"]
    )
    if not ok:
        # SR may simply not be enabled in this env — surface the error to caller
        return {}, err or "SR cluster describe failed"
    if isinstance(data, dict):
        return data, ""
    return {}, "unexpected SR JSON shape"


def _list_flink_pools(env_id: str) -> tuple[list[dict], str]:
    ok, data, err = _run_confluent_json(
        ["flink", "compute-pool", "list", "--environment", env_id, "-o", "json"]
    )
    if not ok or not isinstance(data, list):
        return [], err or "flink pool list failed"
    return data, ""


def _create_api_key(resource_id: str, env_id: str, description: str) -> tuple[dict, str]:
    """Mint a fresh API key. resource_id is lkc-... or lsrc-... .

    The CLI prints both the key and secret only on the create call; subsequent
    `api-key describe` does NOT return the secret. We capture both fields here.
    """
    args = ["api-key", "create",
            "--resource", resource_id,
            "--environment", env_id,
            "--description", description,
            "-o", "json"]
    ok, data, err = _run_confluent_json(args, timeout=45)
    if not ok or not isinstance(data, dict):
        return {}, err or "api-key create failed"
    # CLI returns either {"key": "...", "secret": "..."} or {"api_key": "...", "api_secret": "..."}
    key    = data.get("key")    or data.get("api_key")
    secret = data.get("secret") or data.get("api_secret")
    if not key or not secret:
        return {}, f"api-key create returned no key/secret: {data}"
    return {"api_key": key, "api_secret": secret}, ""


def _provision_env(env_id: str, env_name: str) -> dict:
    """Discover cluster + SR + Flink pool for env_id and mint API keys.

    Returns a dict shaped like the EnvDeployment YAML:
      {env_id, env_name, cluster_id, kafka_bootstrap, kafka_rest_endpoint,
       kafka_api_key, kafka_api_secret, flink_compute_pool,
       schema_registry: {endpoint, api_key, api_secret} | None,
       _log: [...]}

    The _log list contains human-readable progress messages for the UI.
    """
    log: list[str] = []
    out: dict = {"env_id": env_id, "env_name": env_name, "_log": log}

    # 1. Pick a Kafka cluster in this env (first one)
    clusters, err = _list_clusters(env_id)
    if err:
        log.append(f"✗ kafka cluster list: {err}")
        out["_error"] = err
        return out
    if not clusters:
        log.append("✗ no Kafka clusters in env")
        out["_error"] = "no Kafka clusters in env"
        return out
    cluster = clusters[0]
    cluster_id = cluster.get("id", "")
    log.append(f"✓ cluster {cluster_id} ({cluster.get('name', '')})")
    out["cluster_id"] = cluster_id

    # 2. Describe cluster → bootstrap + REST endpoint
    desc, err = _describe_cluster(cluster_id, env_id)
    if err:
        log.append(f"⚠ cluster describe: {err}")
    out["kafka_bootstrap"]     = desc.get("endpoint", "").replace("SASL_SSL://", "")
    out["kafka_rest_endpoint"] = desc.get("rest_endpoint", "")
    if out["kafka_bootstrap"]:
        log.append(f"✓ bootstrap {out['kafka_bootstrap']}")

    # 3. Mint Kafka cluster API key
    kkey, err = _create_api_key(cluster_id, env_id, "openlineage-confluent")
    if err:
        log.append(f"✗ kafka api-key: {err}")
        out["_error"] = err
        return out
    out["kafka_api_key"]    = kkey["api_key"]
    out["kafka_api_secret"] = kkey["api_secret"]
    log.append(f"✓ kafka API key minted ({kkey['api_key']})")

    # 4. Schema Registry — optional
    sr, err = _describe_sr(env_id)
    if sr.get("id"):
        sr_id = sr.get("id", "")
        sr_endpoint = sr.get("endpoint", "") or sr.get("endpoint_url", "")
        sr_key, err = _create_api_key(sr_id, env_id, "openlineage-confluent-sr")
        if err:
            log.append(f"⚠ SR api-key: {err}")
        else:
            out["schema_registry"] = {
                "endpoint":   sr_endpoint,
                "api_key":    sr_key["api_key"],
                "api_secret": sr_key["api_secret"],
            }
            log.append(f"✓ SR key minted for {sr_id}")
    else:
        log.append("⚠ SR not enabled in env (skipping)")

    # 5. Flink compute pool — best-effort, used by load-test provisioning
    pools, err = _list_flink_pools(env_id)
    if pools:
        out["flink_compute_pool"] = pools[0].get("id", "")
        log.append(f"✓ Flink pool {out['flink_compute_pool']}")
    else:
        log.append(f"⚠ no Flink compute pools (load-test will skip Flink steps)")

    return out


def _upsert_environment(env_entry: dict) -> None:
    """Add or replace an EnvDeployment entry in config.yaml's environments: list.

    Uses yaml round-tripping (loses comments outside `environments:` if any),
    but preserves the rest of the document structure.
    """
    import yaml

    if not CONFIG_YML.exists():
        if EXAMPLE_YML.exists():
            CONFIG_YML.write_text(EXAMPLE_YML.read_text())
        else:
            CONFIG_YML.write_text("confluent: {}\n")

    raw = yaml.safe_load(CONFIG_YML.read_text()) or {}
    confluent = raw.setdefault("confluent", {})
    envs = confluent.setdefault("environments", [])

    # Strip private/UI-only fields before persisting
    persistable = {k: v for k, v in env_entry.items() if not k.startswith("_") and k != "env_name"}

    # Replace by env_id, else append
    env_id = persistable.get("env_id", "")
    for i, existing in enumerate(envs):
        if existing.get("env_id") == env_id:
            envs[i] = persistable
            break
    else:
        envs.append(persistable)

    # Strip legacy single-env top-level fields if present (now superseded)
    for legacy in ("CONFLUENT_ENV_ID", "CONFLUENT_CLUSTER_ID",
                   "CONFLUENT_KAFKA_REST_ENDPOINT",
                   "CONFLUENT_KAFKA_API_KEY", "CONFLUENT_KAFKA_API_SECRET",
                   "schema_registry"):
        confluent.pop(legacy, None)

    CONFIG_YML.write_text(
        yaml.safe_dump(raw, sort_keys=False, default_flow_style=False, width=200)
    )


def _read_selected_envs() -> list[dict]:
    """Return the wizard's pending selection: [{env_id, env_name}, ...].

    Lives at confluent.selected_envs in config.yaml. Pydantic ignores unknown
    fields, so the bridge runtime never sees this list — it is wizard-private
    state for envs the user has picked but that have not yet been provisioned
    (cluster + Kafka/SR API keys minted) on a Start OpenLineage run.
    """
    if not CONFIG_YML.exists():
        return []
    import yaml
    try:
        raw = yaml.safe_load(CONFIG_YML.read_text()) or {}
    except yaml.YAMLError:
        return []
    out: list[dict] = []
    for e in (raw.get("confluent") or {}).get("selected_envs") or []:
        if isinstance(e, dict) and e.get("env_id"):
            out.append({
                "env_id":   e.get("env_id", ""),
                "env_name": e.get("env_name", ""),
            })
    return out


def _write_selected_envs(envs: list[dict]) -> None:
    """Replace confluent.selected_envs in config.yaml.

    Each entry should have env_id and (optionally) env_name. Empty list removes
    the field entirely. Preserves the rest of the YAML document via round-trip.
    """
    import yaml

    if not CONFIG_YML.exists():
        if EXAMPLE_YML.exists():
            CONFIG_YML.write_text(EXAMPLE_YML.read_text())
        else:
            CONFIG_YML.write_text("confluent: {}\n")

    raw = yaml.safe_load(CONFIG_YML.read_text()) or {}
    confluent = raw.setdefault("confluent", {})

    cleaned = [
        {"env_id": e["env_id"], "env_name": e.get("env_name", "")}
        for e in envs if e.get("env_id")
    ]
    if cleaned:
        confluent["selected_envs"] = cleaned
    else:
        confluent.pop("selected_envs", None)

    CONFIG_YML.write_text(
        yaml.safe_dump(raw, sort_keys=False, default_flow_style=False, width=200)
    )


def _configured_env_ids() -> list[dict]:
    """Return [{env_id, has_kafka_creds, has_sr, flink_pool, pending}, ...].

    Unions confluent.selected_envs (pending — not yet provisioned) with
    confluent.environments (fully provisioned). Pre-checks both lists when the
    wizard reloads, so users see their previous selection regardless of whether
    the bridge has been started yet. Applies the same legacy-single-env shim
    used by AppConfig.from_yaml so an older config.yaml still appears here.
    """
    if not CONFIG_YML.exists():
        return []
    import yaml
    try:
        raw = yaml.safe_load(CONFIG_YML.read_text()) or {}
    except yaml.YAMLError:
        return []

    # Apply legacy shim — best-effort import to avoid hard dependency at server import time.
    try:
        sys.path.insert(0, str(REPO_DIR / "src"))
        from openlineage_confluent.config import _apply_legacy_shim  # type: ignore
        raw = _apply_legacy_shim(raw)
    except Exception:    # noqa: BLE001
        pass

    confluent = raw.get("confluent") or {}
    out: list[dict] = []
    seen: set[str] = set()

    for e in confluent.get("environments") or []:
        if not isinstance(e, dict) or not e.get("env_id"):
            continue
        seen.add(e["env_id"])
        out.append({
            "env_id":           e.get("env_id", ""),
            "has_kafka_creds":  bool(e.get("kafka_api_key")),
            "has_sr":           bool(e.get("schema_registry")),
            "flink_pool":       e.get("flink_compute_pool", ""),
            "pending":          False,
        })

    for e in confluent.get("selected_envs") or []:
        if not isinstance(e, dict) or not e.get("env_id") or e["env_id"] in seen:
            continue
        out.append({
            "env_id":           e.get("env_id", ""),
            "has_kafka_creds":  False,
            "has_sr":           False,
            "flink_pool":       "",
            "pending":          True,
        })

    return out


# ──────────────────────────────────────────────────────────────────────────────
# Marquez control (docker compose wrappers)
# ──────────────────────────────────────────────────────────────────────────────

def _port_listening(port: int, host: str = "127.0.0.1") -> bool:
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(0.4)
    try:
        return s.connect_ex((host, port)) == 0
    finally:
        s.close()


def _marquez_status() -> dict:
    """Status of the Marquez API + the patched UI dev server.

    api: docker-compose marquez-api on :5000 (upstream image, fine for ingest)
    patched_ui: marquez-ui-patch dev server on :1337 (typed labels, topic pills)
    """
    import http.client
    api_running = False
    api_status  = None
    try:
        conn = http.client.HTTPConnection("127.0.0.1", 5000, timeout=2)
        conn.request("GET", "/api/v1/namespaces")
        resp = conn.getresponse()
        api_status = resp.status
        api_running = 200 <= resp.status < 500
    except Exception:
        pass
    return {
        "running":          api_running,        # legacy field — kept for compat
        "http_status":      api_status,         # legacy field
        "api_running":      api_running,
        "api_http_status":  api_status,
        "patched_ui_running": _port_listening(PATCHED_UI_PORT),
        "patched_ui_dir":     str(PATCHED_UI_DIR),
        "patched_ui_dir_present": PATCHED_UI_DIR.exists(),
    }


def _patched_ui_start() -> tuple[bool, str]:
    """Start the patched UI dev server in background. Returns (ok, msg)."""
    if not PATCHED_UI_DIR.exists():
        return False, f"patched UI dir not found: {PATCHED_UI_DIR} " \
                      "(set MARQUEZ_UI_PATCH_DIR env var or skip — upstream UI is at :3000)"
    if _port_listening(PATCHED_UI_PORT):
        return True, f"already running on :{PATCHED_UI_PORT}"
    if not (PATCHED_UI_DIR / "node_modules").exists():
        return False, f"node_modules missing in {PATCHED_UI_DIR} — run `npm install` there first"

    log = open(PATCHED_UI_LOG, "ab")
    log.write(f"\n=== _patched_ui_start at {time.strftime('%F %T')} ===\n".encode())
    log.flush()
    try:
        proc = subprocess.Popen(
            ["npm", "run", "dev"],
            cwd=str(PATCHED_UI_DIR),
            stdout=log, stderr=log,
            start_new_session=True,    # detach so wizard exit doesn't kill it
        )
    except FileNotFoundError:
        return False, "`npm` not found in PATH"
    # Webpack dev server takes ~10-15s to compile; wait briefly for the port
    for _ in range(40):
        if _port_listening(PATCHED_UI_PORT):
            return True, f"started (pid {proc.pid}) — log at {PATCHED_UI_LOG}"
        if proc.poll() is not None:
            return False, f"npm run dev exited early (rc={proc.returncode}) — check {PATCHED_UI_LOG}"
        time.sleep(0.5)
    return False, f"started but :{PATCHED_UI_PORT} not listening after 20s — check {PATCHED_UI_LOG}"


def _patched_ui_stop() -> tuple[bool, str]:
    """Kill the patched UI dev server (anything bound to :1337)."""
    if not _port_listening(PATCHED_UI_PORT):
        return True, "not running"
    try:
        # lsof is fast + reliable on macOS / Linux for finding the listener PID
        out = subprocess.run(
            ["lsof", "-nP", f"-iTCP:{PATCHED_UI_PORT}", "-sTCP:LISTEN", "-t"],
            capture_output=True, text=True, timeout=5,
        ).stdout
        pids = [int(p) for p in out.split() if p.strip().isdigit()]
        if not pids:
            return False, f"port :{PATCHED_UI_PORT} listening but no PID found"
        for pid in pids:
            try:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
            except PermissionError:
                os.kill(pid, signal.SIGTERM)
        # Wait up to 4s for port to free
        for _ in range(20):
            if not _port_listening(PATCHED_UI_PORT):
                return True, f"stopped (killed pid {pids[0]})"
            time.sleep(0.2)
        return False, f"sent SIGTERM but :{PATCHED_UI_PORT} still listening"
    except Exception as e:    # noqa: BLE001
        return False, str(e)


def _marquez_action(action: str) -> tuple[int, str]:
    """Bring the FULL Marquez stack up, down, or wipe-and-restart.

    up:    docker compose up (postgres + marquez-api + upstream UI)
           + `npm run dev` for the patched UI on :1337 (best-effort)
    down:  docker compose down + kill the patched UI dev server
    wipe:  docker compose down -v + up -d (drops the postgres volume —
           every namespace/dataset/job/run is gone) + bounce the patched UI
    """
    if action not in ("up", "down", "wipe"):
        return 2, f"unknown action: {action}"
    target = {"up": "marquez-up", "down": "marquez-down", "wipe": "marquez-wipe"}[action]
    parts: list[str] = []

    try:
        proc = subprocess.run(
            ["make", target],
            cwd=str(REPO_DIR),
            capture_output=True, text=True, timeout=180,
        )
    except FileNotFoundError:
        return 127, "`make` not found in PATH"
    except subprocess.TimeoutExpired:
        return 124, f"make {target} timed out (>180s)"
    parts.append((proc.stdout + proc.stderr).strip())
    if proc.returncode != 0:
        return proc.returncode, "\n".join(parts)

    # Patched UI — best-effort: don't fail the whole action if it can't start
    if action == "down":
        ok, msg = _patched_ui_stop()
        parts.append(f"[patched UI] {msg}")
    else:    # "up" or "wipe": bounce/start the dev server
        if action == "wipe":
            _patched_ui_stop()
        ok, msg = _patched_ui_start()
        parts.append(f"[patched UI] {msg}")

    return 0, "\n".join(parts)


# ──────────────────────────────────────────────────────────────────────────────
# Load test (scripts/provision_demo_pipelines.py) — single concurrent run
#
# Provisions REAL Confluent Cloud resources (Datagen connectors + Flink
# statements + consumer groups) in the chosen env. The bridge picks them up
# via the normal CC APIs and lineage flows to Marquez indirectly.
#
# Before launching the script, the worker thread ensures the env has the
# prerequisites (Kafka cluster, SR, Flink compute pool) and creates them if
# missing — all streaming live into the load-test ring buffer so the browser
# sees every step.
# ──────────────────────────────────────────────────────────────────────────────

PROVISION_SCRIPT = REPO_DIR / "scripts" / "provision_demo_pipelines.py"

# Wizard input bounds — clamped server-side regardless of what the browser sends.
_LT_MIN_PIPELINES = 1
_LT_MAX_PIPELINES = 50
_LT_MIN_NODES     = 2
_LT_MAX_NODES     = 20

# Defaults for new resources created in card 4 (hardcoded per user choice).
_DEFAULT_CLOUD       = "aws"
_DEFAULT_REGION      = "us-west-2"
_DEFAULT_SR_GEO      = "us"
_DEFAULT_KAFKA_TYPE  = "basic"
_DEFAULT_FLINK_CFU   = "10"


def _ensure_env_resources(env_id: str, emit) -> tuple[bool, str]:
    """Ensure env_id has Kafka cluster + SR + Flink pool; create what's missing.

    Mints API keys for Kafka + SR (whether reused or freshly created) and
    persists the resulting EnvDeployment entry to config.yaml. `emit(line)`
    is called for each progress line so the caller can pipe it to SSE.

    Returns (ok, error_msg).
    """
    # Snapshot current config entry (legacy shim applies in _configured_env_ids)
    existing = next((e for e in _configured_env_ids() if e["env_id"] == env_id), None)
    entry: dict = {"env_id": env_id, "_log": []}

    # ── 1. Kafka cluster ────────────────────────────────────────────────────
    clusters, err = _list_clusters(env_id)
    if err:
        return False, f"kafka cluster list failed: {err}"

    if clusters:
        cluster_id = clusters[0].get("id", "")
        emit(f"✓ Reusing existing Kafka cluster {cluster_id} ({clusters[0].get('name','')})")
    else:
        cname = f"openlineage-{env_id[-6:]}"
        emit(f"… Creating Kafka cluster '{cname}' ({_DEFAULT_KAFKA_TYPE.upper()}, "
             f"{_DEFAULT_CLOUD}/{_DEFAULT_REGION})")
        ok, data, err = _run_confluent_json(
            ["kafka", "cluster", "create", cname,
             "--type",         _DEFAULT_KAFKA_TYPE,
             "--cloud",        _DEFAULT_CLOUD,
             "--region",       _DEFAULT_REGION,
             "--availability", "single-zone",
             "--environment",  env_id,
             "-o", "json"],
            timeout=120,
        )
        if not ok or not isinstance(data, dict) or not data.get("id"):
            return False, f"kafka cluster create failed: {err or data}"
        cluster_id = data["id"]
        emit(f"  → cluster id {cluster_id} (provisioning)")

        # Poll until status reaches "UP" / "ACTIVE" — BASIC clusters take ~30s
        for _ in range(60):    # up to 5 minutes (60 * 5s)
            time.sleep(5)
            desc, _ = _describe_cluster(cluster_id, env_id)
            status = (desc.get("status") or "").upper()
            if status in ("UP", "ACTIVE", "PROVISIONED"):
                emit(f"  ✓ cluster status={status}")
                break
            emit(f"  … waiting (status={status or 'unknown'})")
        else:
            return False, f"kafka cluster {cluster_id} never reached UP"

    entry["cluster_id"] = cluster_id

    desc, err = _describe_cluster(cluster_id, env_id)
    if err:
        emit(f"⚠ cluster describe: {err}")
    bootstrap = desc.get("endpoint", "")
    if bootstrap.startswith("SASL_SSL://"):
        bootstrap = bootstrap[len("SASL_SSL://"):]
    entry["kafka_bootstrap"]     = bootstrap
    entry["kafka_rest_endpoint"] = desc.get("rest_endpoint", "")
    if bootstrap:
        emit(f"  bootstrap {bootstrap}")

    # Reuse Kafka API key if already saved; else mint a fresh one
    if existing and existing.get("has_kafka_creds"):
        # Pull existing secrets from config.yaml directly
        import yaml
        raw = yaml.safe_load(CONFIG_YML.read_text()) or {}
        try:
            from openlineage_confluent.config import _apply_legacy_shim
            raw = _apply_legacy_shim(raw)
        except Exception:    # noqa: BLE001
            pass
        for e in (raw.get("confluent") or {}).get("environments") or []:
            if e.get("env_id") == env_id:
                entry["kafka_api_key"]    = e.get("kafka_api_key", "")
                entry["kafka_api_secret"] = e.get("kafka_api_secret", "")
        emit(f"✓ Reusing existing Kafka API key ({entry.get('kafka_api_key','')})")
    else:
        emit(f"… Minting Kafka cluster API key for {cluster_id}")
        kkey, err = _create_api_key(cluster_id, env_id, "openlineage-confluent")
        if err:
            return False, f"kafka api-key: {err}"
        entry["kafka_api_key"]    = kkey["api_key"]
        entry["kafka_api_secret"] = kkey["api_secret"]
        emit(f"  ✓ key {kkey['api_key']}")

    # ── 2. Schema Registry ──────────────────────────────────────────────────
    sr, err = _describe_sr(env_id)
    if not sr.get("id"):
        emit(f"… Enabling Schema Registry ({_DEFAULT_CLOUD}/{_DEFAULT_SR_GEO})")
        ok, data, err = _run_confluent_json(
            ["schema-registry", "cluster", "enable",
             "--cloud", _DEFAULT_CLOUD, "--geo", _DEFAULT_SR_GEO,
             "--environment", env_id, "-o", "json"],
            timeout=60,
        )
        if not ok:
            emit(f"⚠ SR enable failed: {err}")
        else:
            sr, _ = _describe_sr(env_id)

    if sr.get("id"):
        sr_id       = sr.get("id", "")
        sr_endpoint = sr.get("endpoint") or sr.get("endpoint_url") or ""
        emit(f"✓ Schema Registry {sr_id} at {sr_endpoint}")
        if existing and existing.get("has_sr"):
            import yaml
            raw = yaml.safe_load(CONFIG_YML.read_text()) or {}
            try:
                from openlineage_confluent.config import _apply_legacy_shim
                raw = _apply_legacy_shim(raw)
            except Exception:    # noqa: BLE001
                pass
            for e in (raw.get("confluent") or {}).get("environments") or []:
                if e.get("env_id") == env_id and e.get("schema_registry"):
                    entry["schema_registry"] = e["schema_registry"]
            emit(f"  ✓ Reusing existing SR API key")
        else:
            emit(f"… Minting SR API key for {sr_id}")
            sr_key, err = _create_api_key(sr_id, env_id, "openlineage-confluent-sr")
            if err:
                emit(f"⚠ SR api-key: {err} (continuing without SR creds)")
            else:
                entry["schema_registry"] = {
                    "endpoint":   sr_endpoint,
                    "api_key":    sr_key["api_key"],
                    "api_secret": sr_key["api_secret"],
                }
                emit(f"  ✓ key {sr_key['api_key']}")
    else:
        emit("⚠ SR not available in env (skipping)")

    # ── 3. Flink compute pool ───────────────────────────────────────────────
    pools, err = _list_flink_pools(env_id)
    if pools:
        pool_id = pools[0].get("id", "")
        emit(f"✓ Reusing existing Flink compute pool {pool_id}")
    else:
        pname = f"openlineage-{env_id[-6:]}"
        emit(f"… Creating Flink compute pool '{pname}' "
             f"({_DEFAULT_CLOUD}/{_DEFAULT_REGION}, max {_DEFAULT_FLINK_CFU} CFU)")
        ok, data, err = _run_confluent_json(
            ["flink", "compute-pool", "create", pname,
             "--cloud",   _DEFAULT_CLOUD,
             "--region",  _DEFAULT_REGION,
             "--max-cfu", _DEFAULT_FLINK_CFU,
             "--environment", env_id,
             "-o", "json"],
            timeout=60,
        )
        if not ok or not isinstance(data, dict) or not data.get("id"):
            emit(f"⚠ Flink pool create failed: {err} (Flink statements will be skipped)")
            pool_id = ""
        else:
            pool_id = data["id"]
            emit(f"  ✓ pool {pool_id}")
    if pool_id:
        entry["flink_compute_pool"] = pool_id

    # ── 4. Persist to config.yaml ───────────────────────────────────────────
    _upsert_environment(entry)
    emit(f"✓ Saved env {env_id} to config.yaml")
    return True, ""


def _lt_running() -> bool:
    """True if the worker thread is mid-flight or its subprocess is alive."""
    if _lt_state["done"]:
        return False
    if _lt_state["started_at"] is None:
        return False
    proc = _lt_state["proc"]
    if proc is None:
        return True   # worker thread is still doing setup, hasn't spawned yet
    return proc.poll() is None


def _lt_pump(proc: subprocess.Popen) -> None:
    """Background thread: drain proc.stdout into the ring buffer."""
    try:
        for raw in iter(proc.stdout.readline, b""):
            line = raw.decode(errors="replace").rstrip("\n")
            with _lt_lock:
                _lt_state["lines"].append(line)
        proc.wait()
    finally:
        with _lt_lock:
            _lt_state["done"] = True
            _lt_state["rc"]   = proc.returncode


def _lt_emit(line: str) -> None:
    """Append a line to the load-test ring buffer (visible to SSE)."""
    with _lt_lock:
        _lt_state["lines"].append(line)


def _lt_provision_worker(env_id: str, num_pipelines: int, max_nodes: int) -> None:
    """Worker thread: ensure resources exist, then run the provision script."""
    rc_final = 1
    try:
        _lt_emit(f"=== Setting up env {env_id} (creating any missing resources) ===")
        ok, err = _ensure_env_resources(env_id, _lt_emit)
        if not ok:
            _lt_emit(f"✗ resource setup failed: {err}")
            return

        _lt_emit("")
        _lt_emit(f"=== Provisioning {num_pipelines} pipeline(s) (max {max_nodes} nodes each) "
                 f"via {PROVISION_SCRIPT.name} ===")
        argv = [sys.executable, str(PROVISION_SCRIPT),
                "--env", env_id, "--config", str(CONFIG_YML),
                "--num-pipelines", str(num_pipelines),
                "--max-nodes",     str(max_nodes),
                "--provision"]
        proc = subprocess.Popen(
            argv,
            cwd=str(REPO_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        with _lt_lock:
            _lt_state["proc"] = proc
            _lt_state["args"] = argv

        for raw in iter(proc.stdout.readline, b""):
            _lt_emit(raw.decode(errors="replace").rstrip("\n"))
        proc.wait()
        rc_final = proc.returncode
    except Exception as exc:    # noqa: BLE001
        _lt_emit(f"✗ uncaught error in provisioning worker: {exc}")
    finally:
        with _lt_lock:
            _lt_state["done"] = True
            _lt_state["rc"]   = rc_final


def _lt_start_provision(
    env_id: str,
    mode: str = "provision",
    *,
    num_pipelines: int = 10,
    max_nodes: int = 4,
) -> tuple[bool, str]:
    """Launch provision flow (with auto resource creation) or teardown for env_id.

    `provision` runs _lt_provision_worker (creates cluster/SR/Flink if missing,
    then runs the provision script). `teardown` just runs the script directly.
    num_pipelines and max_nodes are clamped to the wizard input bounds.
    """
    if not PROVISION_SCRIPT.exists():
        return False, f"{PROVISION_SCRIPT} not found"
    if not re.match(r"^env-[A-Za-z0-9]+$", env_id):
        return False, f"invalid env_id: {env_id}"
    if mode not in ("provision", "teardown"):
        return False, "mode must be 'provision' or 'teardown'"

    num_pipelines = max(_LT_MIN_PIPELINES, min(_LT_MAX_PIPELINES, num_pipelines))
    max_nodes     = max(_LT_MIN_NODES,     min(_LT_MAX_NODES,     max_nodes))

    if mode == "provision":
        # Atomic check-and-claim: prevents two simultaneous /loadtest/start POSTs
        # from both observing _lt_running()=False and both spawning a worker.
        with _lt_lock:
            still_alive = (
                not _lt_state["done"]
                and _lt_state["started_at"] is not None
                and (_lt_state["proc"] is None or _lt_state["proc"].poll() is None)
            )
            if still_alive:
                return False, "a provision/teardown is already running — Stop it first"
            _lt_state.update({
                "proc":       None,
                "args":       ["<provision worker>", env_id],
                "lines":      deque(maxlen=_LT_RING_SIZE),
                "done":       False,
                "rc":         None,
                "started_at": time.time(),
            })
        threading.Thread(
            target=_lt_provision_worker,
            args=(env_id, num_pipelines, max_nodes),
            daemon=True,
        ).start()
        return True, (f"provisioning started for {env_id} — "
                      f"{num_pipelines} pipeline(s), max {max_nodes} nodes each")

    # Teardown: env must already be configured with Kafka creds
    configured = {e["env_id"]: e for e in _configured_env_ids()}
    if env_id not in configured:
        return False, f"env {env_id} not configured in config.yaml"
    if not configured[env_id]["has_kafka_creds"]:
        return False, f"env {env_id} has no Kafka credentials — nothing to tear down"

    py = sys.executable
    argv = [py, str(PROVISION_SCRIPT),
            "--env", env_id, "--config", str(CONFIG_YML), "--teardown"]
    # Atomic check-and-claim — same pattern as the provision branch above.
    with _lt_lock:
        still_alive = (
            not _lt_state["done"]
            and _lt_state["started_at"] is not None
            and (_lt_state["proc"] is None or _lt_state["proc"].poll() is None)
        )
        if still_alive:
            return False, "a provision/teardown is already running — Stop it first"
        # Claim the slot before spawning so a concurrent caller sees us as busy.
        _lt_state.update({
            "proc":       None,
            "args":       argv,
            "lines":      deque(maxlen=_LT_RING_SIZE),
            "done":       False,
            "rc":         None,
            "started_at": time.time(),
        })
    proc = subprocess.Popen(
        argv,
        cwd=str(REPO_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=0,
    )
    with _lt_lock:
        _lt_state["proc"] = proc
    threading.Thread(target=_lt_pump, args=(proc,), daemon=True).start()
    return True, f"teardown started (pid {proc.pid}) — streaming output below"


def _lt_stop() -> tuple[bool, str]:
    proc = _lt_state["proc"]
    if proc is None:
        # Worker thread is mid-setup (no subprocess yet) — can't safely abort
        if _lt_running():
            return False, ("setup phase in progress — Stop only works once the "
                           "provision script subprocess is running")
        return False, "no provisioning is running"
    if proc.poll() is not None:
        return False, "no provisioning is running"
    try:
        proc.send_signal(signal.SIGTERM)
        for _ in range(20):
            if proc.poll() is not None:
                break
            time.sleep(0.1)
        if proc.poll() is None:
            proc.kill()
        return True, f"stopped (rc={proc.returncode})"
    except Exception as e:    # noqa: BLE001
        return False, str(e)


def _lt_status() -> dict:
    return {
        "running":   _lt_running(),
        "done":      _lt_state["done"],
        "rc":        _lt_state["rc"],
        "args":      _lt_state["args"],
        "started_at": _lt_state["started_at"],
        "line_count": len(_lt_state["lines"]),
    }


# Marquez-namespace teardown is no longer relevant — provisioning is now done
# via real CC resources, and tearing down means deleting those CC resources
# via `provision_demo_pipelines.py --teardown`. Lineage in Marquez disappears
# naturally on the next bridge poll cycle (with ABORT events for removed jobs).


# ──────────────────────────────────────────────────────────────────────────────
# Bridge runner — `ol-confluent run --config config.yaml` as a long-lived child
# ──────────────────────────────────────────────────────────────────────────────

def _bridge_running() -> bool:
    """True when the bridge is provisioning or the subprocess is alive."""
    if _bridge_state["phase"] in ("provisioning", "running"):
        proc = _bridge_state["proc"]
        if _bridge_state["phase"] == "running":
            return proc is not None and proc.poll() is None
        return True
    return False


def _bridge_emit(line: str) -> None:
    """Append a line to the bridge stream (used by the launcher thread)."""
    with _bridge_lock:
        _bridge_state["lines"].append(line)


def _bridge_pump(proc: subprocess.Popen) -> None:
    try:
        for raw in iter(proc.stdout.readline, b""):
            line = raw.decode(errors="replace").rstrip("\n")
            with _bridge_lock:
                _bridge_state["lines"].append(line)
        proc.wait()
    finally:
        with _bridge_lock:
            _bridge_state["done"]  = True
            _bridge_state["phase"] = "done"
            _bridge_state["rc"]    = proc.returncode


def _bridge_provision_pending() -> bool:
    """Provision every env in selected_envs; move successes into environments.

    Streams progress lines into the bridge log. Returns True if all selected
    envs were provisioned (or there were none to begin with), False on the
    first failure.
    """
    pending = _read_selected_envs()
    if not pending:
        return True

    _bridge_emit(f"⏳ provisioning {len(pending)} pending env(s) before bridge start…")
    remaining = list(pending)
    for entry in pending:
        env_id   = entry["env_id"]
        env_name = entry.get("env_name", "")
        _bridge_emit(f"  → {env_id} ({env_name or 'no name'}): minting Kafka + SR keys")
        try:
            cfg_entry = _provision_env(env_id, env_name)
        except Exception as exc:    # noqa: BLE001
            _bridge_emit(f"  ✗ {env_id}: uncaught {exc}")
            return False
        for log_line in cfg_entry.get("_log", []):
            _bridge_emit(f"    {log_line}")
        if cfg_entry.get("_error"):
            _bridge_emit(f"  ✗ {env_id}: {cfg_entry['_error']}")
            # Persist any progress made so far (move successes out, keep failures pending)
            _write_selected_envs(remaining)
            return False
        try:
            _upsert_environment(cfg_entry)
        except Exception as exc:    # noqa: BLE001
            _bridge_emit(f"  ✗ {env_id}: failed to write config.yaml: {exc}")
            _write_selected_envs(remaining)
            return False
        # Success: drop this env from the still-pending list
        remaining = [r for r in remaining if r["env_id"] != env_id]
        _write_selected_envs(remaining)
        _bridge_emit(f"  ✓ {env_id}: provisioned")

    return True


def _bridge_launch_worker() -> None:
    """Run provisioning then spawn ol-confluent run. Runs in a background thread.

    Honors a Stop click that lands during provisioning: _bridge_stop() flips
    phase to "done" while we're still in _bridge_provision_pending(). We
    re-check phase under the lock both before and after spawning the
    subprocess so we don't resurrect the bridge after the user cancelled.
    """
    try:
        ok = _bridge_provision_pending()
    except Exception as exc:    # noqa: BLE001
        _bridge_emit(f"✗ provisioning crashed: {exc}")
        ok = False

    # Provisioning failed OR user clicked Stop while we were provisioning.
    with _bridge_lock:
        cancelled = _bridge_state["phase"] == "done"
    if not ok or cancelled:
        with _bridge_lock:
            _bridge_state["done"]  = True
            _bridge_state["phase"] = "done"
            if _bridge_state["rc"] is None:
                _bridge_state["rc"] = 1
        return

    venv_bin = REPO_DIR / ".venv" / "bin" / "ol-confluent"
    if venv_bin.exists():
        argv = [str(venv_bin), "run", "--config", str(CONFIG_YML)]
    else:
        argv = [sys.executable, "-m", "openlineage_confluent.cli",
                "run", "--config", str(CONFIG_YML)]

    _bridge_emit(f"▶ spawning: {' '.join(argv)}")
    try:
        proc = subprocess.Popen(
            argv,
            cwd=str(REPO_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
    except Exception as exc:    # noqa: BLE001
        _bridge_emit(f"✗ failed to spawn bridge: {exc}")
        with _bridge_lock:
            _bridge_state["done"]  = True
            _bridge_state["phase"] = "done"
            _bridge_state["rc"]    = 1
        return

    # Recheck under lock — if Stop landed between the cancel check above and
    # this point (rare but possible), kill the just-spawned proc and bail
    # before _bridge_pump enters its blocking readline loop.
    with _bridge_lock:
        if _bridge_state["phase"] == "done":
            _bridge_emit("✗ stop arrived after spawn — terminating just-spawned bridge")
            try:
                proc.kill()
            except Exception:    # noqa: BLE001
                pass
            return
        _bridge_state["proc"]  = proc
        _bridge_state["args"]  = argv
        _bridge_state["phase"] = "running"

    _bridge_pump(proc)


def _bridge_start() -> tuple[bool, str]:
    """Kick off (provisioning + ol-confluent run) in a background thread.

    Returns immediately. Provisioning of any envs in confluent.selected_envs
    streams into the bridge log, then the subprocess is spawned.
    """
    pending      = _read_selected_envs()
    provisioned  = [e for e in _configured_env_ids() if not e["pending"]]
    if not pending and not provisioned:
        return False, "no envs configured — pick env(s) in card 3 first"

    # Atomic check-and-claim: prevents two simultaneous /bridge/start POSTs
    # from both observing phase=idle and both spawning a launcher thread.
    with _bridge_lock:
        if _bridge_state["phase"] in ("provisioning", "running"):
            proc = _bridge_state["proc"]
            still_alive = (
                _bridge_state["phase"] == "provisioning"
                or (proc is not None and proc.poll() is None)
            )
            if still_alive:
                return False, "OpenLineage bridge is already running"
        _bridge_state.update({
            "proc":       None,
            "args":       [],
            "lines":      deque(maxlen=_BRIDGE_RING_SIZE),
            "phase":      "provisioning" if pending else "running",
            "done":       False,
            "rc":         None,
            "started_at": time.time(),
        })

    threading.Thread(target=_bridge_launch_worker, daemon=True).start()

    if pending:
        return True, f"starting (provisioning {len(pending)} env(s) first)"
    return True, "starting"


def _bridge_stop() -> tuple[bool, str]:
    # If we're still provisioning, there's no subprocess to kill yet — flip the
    # phase to done and let the launcher thread bail on its next emit.
    if _bridge_state["phase"] == "provisioning" and _bridge_state["proc"] is None:
        with _bridge_lock:
            _bridge_state["done"]  = True
            _bridge_state["phase"] = "done"
            _bridge_state["rc"]    = 130    # SIGINT-ish
        _bridge_emit("✗ stopped during provisioning")
        return True, "stopped during provisioning"

    proc = _bridge_state["proc"]
    if proc is None or proc.poll() is not None:
        return False, "bridge is not running"
    try:
        proc.send_signal(signal.SIGTERM)
        for _ in range(30):
            if proc.poll() is not None:
                break
            time.sleep(0.1)
        if proc.poll() is None:
            proc.kill()
        return True, f"stopped (rc={proc.returncode})"
    except Exception as e:    # noqa: BLE001
        return False, str(e)


def _bridge_status() -> dict:
    return {
        "running":    _bridge_running(),
        "phase":      _bridge_state["phase"],
        "done":       _bridge_state["done"],
        "rc":         _bridge_state["rc"],
        "args":       _bridge_state["args"],
        "started_at": _bridge_state["started_at"],
        "line_count": len(_bridge_state["lines"]),
    }


# ──────────────────────────────────────────────────────────────────────────────
# HTML page
# ──────────────────────────────────────────────────────────────────────────────

PAGE = """<!doctype html>
<html><head>
<meta charset="utf-8">
<title>OpenLineage-Confluent Setup</title>
<style>
  *, *::before, *::after { box-sizing: border-box; }
  html, body { margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #0d1117; color: #c9d1d9;
    line-height: 1.45;
  }

  /* ── Header ─────────────────────────────────────────── */
  header {
    background: #010409; border-bottom: 1px solid #30363d;
    padding: .9rem 1.4rem; display: flex; justify-content: space-between;
    align-items: center; gap: 1rem;
    position: sticky; top: 0; z-index: 10;
  }
  header h1 { margin: 0; font-size: 1.05rem; font-weight: 600; }
  header h1 .accent { color: #58a6ff; }
  .global-status { font-size: .78rem; color: #8b949e; display: flex; gap: .8rem; align-items: center; }
  .badge { display: inline-block; padding: .15rem .5rem; border-radius: 12px;
           font-size: .7rem; font-weight: 600; }
  .badge.ok  { background: #1f3a23; color: #3fb950; }
  .badge.bad { background: #3d1d20; color: #f85149; }
  .badge.dim { background: #21262d; color: #8b949e; }

  /* ── Main column ───────────────────────────────────── */
  main { max-width: 920px; margin: 0 auto; padding: 1.4rem 1.4rem 4rem; }

  /* ── Cards ─────────────────────────────────────────── */
  .card {
    background: #161b22; border: 1px solid #30363d; border-radius: 8px;
    padding: 1.1rem 1.3rem 1.2rem; margin-bottom: 1rem;
  }
  .card-head { display: flex; align-items: center; gap: .65rem; margin-bottom: .75rem; }
  .step-badge {
    background: #1f6feb; color: #fff; font-weight: 700; font-size: .78rem;
    width: 1.6rem; height: 1.6rem; border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    flex-shrink: 0;
  }
  .card-title { font-weight: 600; color: #58a6ff; font-size: .97rem; }
  .card-help  { color: #8b949e; font-size: .78rem; margin-bottom: .8rem; }
  .card-help code { background: #0d1117; padding: .05rem .35rem; border-radius: 3px;
                    font-size: .78rem; color: #c9d1d9; }

  /* ── Form fields ────────────────────────────────────── */
  label { display: block; font-size: .75rem; color: #8b949e;
          margin: 0 0 .25rem; text-transform: uppercase; letter-spacing: .03em; }
  input[type=text], input[type=password] {
    width: 100%;
    background: #0d1117; border: 1px solid #30363d; border-radius: 4px;
    color: #c9d1d9; padding: .45rem .6rem; font-size: .85rem;
    font-family: 'SFMono-Regular', Consolas, monospace;
  }
  input:focus { outline: none; border-color: #58a6ff; }
  .form-grid    { display: grid; grid-template-columns: 1fr 1fr; gap: .55rem .9rem; }
  .form-grid-3  { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: .55rem .9rem; }
  @media (max-width: 700px) {
    .form-grid, .form-grid-3 { grid-template-columns: 1fr; }
  }
  .field-pair { display: flex; flex-direction: column; }

  /* ── Buttons ───────────────────────────────────────── */
  .btn-row { display: flex; flex-wrap: wrap; gap: .4rem; align-items: center;
             margin-top: .9rem; }
  button {
    background: #238636; color: #fff; border: 0; border-radius: 4px;
    padding: .5rem 1rem; font-size: .85rem; font-weight: 500;
    cursor: pointer; transition: background .12s;
  }
  button:disabled { background: #30363d; color: #6e7681; cursor: not-allowed; }
  button:hover:not(:disabled) { background: #2ea043; }
  button.danger    { background: #da3633; }
  button.danger:hover:not(:disabled)    { background: #f85149; }
  button.secondary { background: #30363d; color: #c9d1d9; }
  button.secondary:hover:not(:disabled) { background: #424a53; }
  .btn-link { color: #58a6ff; font-size: .78rem; text-decoration: none;
              margin-left: auto; }
  .btn-link:hover { text-decoration: underline; }

  /* ── Status / inline feedback ──────────────────────── */
  .status { font-size: .82rem; margin-top: .65rem; min-height: 1.2em; }
  .ok    { color: #3fb950; }
  .err   { color: #f85149; }
  .dim   { color: #8b949e; }

  /* ── Env list ──────────────────────────────────────── */
  .env-list { display: flex; flex-direction: column; gap: .35rem; margin-top: .25rem; }
  .env-row {
    display: flex; align-items: center; gap: .65rem;
    padding: .55rem .8rem; border: 1px solid #30363d; border-radius: 4px;
    cursor: pointer; transition: background .12s, border-color .12s;
  }
  .env-row:hover     { background: #1f242c; border-color: #484f58; }
  .env-row.selected  { background: #0d2d4d; border-color: #58a6ff; }
  .env-row input[type=checkbox] { margin: 0; cursor: pointer; flex-shrink: 0; }
  .env-row .env-name { font-size: .88rem; flex: 1; }
  .env-id   { font-family: 'SFMono-Regular', Consolas, monospace;
              font-size: .73rem; color: #8b949e; }
  .env-status { font-size: .72rem; color: #8b949e;
                margin-left: auto; padding-left: .5rem;
                font-family: 'SFMono-Regular', Consolas, monospace; }
  .env-status.saved   { color: #3fb950; }
  .env-status.failed  { color: #f85149; }
  .env-status.pending { color: #d29922; }

  /* ── Done banner ───────────────────────────────────── */
  .done-banner {
    background: #0c2818; border: 1px solid #1f6f3a; border-radius: 6px;
    padding: .65rem .9rem; margin-bottom: 1rem;
    display: flex; align-items: center; gap: .55rem;
    font-size: .85rem; color: #3fb950;
  }

  /* ── Logs (load test) ──────────────────────────────── */
  pre.log {
    background: #010409; border: 1px solid #21262d; border-radius: 4px;
    padding: .65rem .8rem; margin-top: .8rem; height: 260px; overflow-y: auto;
    font-family: 'SFMono-Regular', Consolas, monospace; font-size: .73rem;
    color: #c9d1d9; white-space: pre-wrap; word-break: break-all;
  }

  .hidden { display: none !important; }
  .divider { height: 1px; background: #21262d; margin: .9rem 0 .8rem; }
  code { font-family: 'SFMono-Regular', Consolas, monospace; }
</style>
</head><body>

<header>
  <h1><span class="accent">OpenLineage</span>-Confluent &nbsp;·&nbsp; Setup</h1>
  <div class="global-status">
    <span id="g-marquez" class="badge dim">Marquez …</span>
    <span id="g-user"    class="badge dim">Confluent Cloud: signed out</span>
  </div>
</header>

<main>

  <!-- ── Card 1: Marquez (prereq for everything else) ─────────────────────── -->
  <div class="card" id="marquez-card">
    <div class="card-head">
      <span class="step-badge">1</span>
      <span class="card-title">Start Marquez (the OpenLineage server)</span>
    </div>
    <div class="card-help">
      Starts Marquez — the OpenLineage server that ingests lineage events
      and visualizes them in a browser. Required before running the load
      test or the bridge.
    </div>
    <span id="mq-ui-dir" class="hidden"></span>
    <div class="btn-row">
      <button id="mq-up-btn"      onclick="mqAction('up')">Start Marquez</button>
      <button id="mq-down-btn"    onclick="mqAction('down')" class="danger">Stop</button>
      <button id="mq-wipe-btn"    onclick="mqWipe()"        class="danger">Delete all data</button>
      <button id="mq-refresh-btn" onclick="mqRefresh()" class="secondary">Refresh</button>
      <a class="btn-link" href="http://localhost:1337" target="_blank">Open Marquez UI ↗</a>
    </div>
    <div class="status" id="mq-status"></div>
  </div>

  <!-- ── Card 2: Sign in to Confluent Cloud ───────────────────────────────── -->
  <div class="card" id="login-card">
    <div class="card-head">
      <span class="step-badge">2</span>
      <span class="card-title">Sign in to Confluent Cloud</span>
    </div>
    <div class="form-grid">
      <div class="field-pair">
        <label>Email</label>
        <input type="text" id="email" autocomplete="email" placeholder="you@example.com">
      </div>
      <div class="field-pair">
        <label>Password</label>
        <input type="password" id="password" autocomplete="current-password"
               onkeydown="if(event.key==='Enter')doLogin()">
      </div>
    </div>
    <div class="btn-row">
      <button id="login-btn" onclick="doLogin()">Sign in</button>
    </div>
    <div class="status" id="login-status"></div>
  </div>

  <!-- ── Card 3: Choose environments ──────────────────────────────────────── -->
  <div class="card hidden" id="env-card">
    <div class="card-head">
      <span class="step-badge">3</span>
      <span class="card-title">Select Confluent Cloud environments</span>
    </div>
    <div class="card-help">
      Tick every environment you want OpenLineage to track — selection is at the
      environment level, so the bridge will pick up <strong>all</strong> resources in
      each chosen env (Connect, Flink, ksqlDB, Tableflow, producers, consumers).
    </div>
    <div class="card-help" style="margin-top:.5rem; padding:.5rem .65rem; background:#1a1f2e; border-left:3px solid #d29922; border-radius:3px;">
      <strong>⚠ Cloud API key permissions:</strong> the key in <code>config.yaml</code>
      (<code>CONFLUENT_CLOUD_API_KEY</code>) must have <strong>MetricsViewer</strong>
      (org or env scope) and <strong>CloudClusterAdmin</strong> (or env-level
      Connect access). Without these, producer/consumer/throughput lineage and
      managed Connect lineage return <code>401</code> and silently drop out of
      the graph. Grant the roles in the Confluent Cloud UI under
      <em>Accounts &amp; access → Service accounts / Users</em>.
    </div>
    <div class="env-list" id="env-list"></div>
    <div class="btn-row">
      <button id="env-save-btn" onclick="saveEnvs()" disabled>Save selection</button>
      <button id="env-refresh-btn" onclick="loadEnvironments()" class="secondary">Refresh</button>
    </div>
    <div class="status" id="env-status"></div>

    <div class="divider"></div>
    <div class="card-help" id="bridge-intro" style="margin-bottom:.5rem">
      No environments selected — pick at least one above.
    </div>
    <div class="btn-row">
      <button id="bridge-start-btn" onclick="bridgeStart()">Start OpenLineage</button>
      <button id="bridge-stop-btn"  onclick="bridgeStop()" class="danger" disabled>Stop OpenLineage</button>
    </div>
    <div class="status" id="bridge-status"></div>
    <pre id="bridge-log" class="log hidden"></pre>
  </div>

  <!-- ── Done banner (slides in after envs saved) ────────────────────────── -->
  <div class="done-banner hidden" id="done-card">
    <span>✓</span>
    <span><strong id="done-env"></strong>
          environment(s) saved to <code>config.yaml</code> ·
          click <em>Start OpenLineage</em> above to begin lineage emission.</span>
  </div>

  <!-- ── Card 4: Load test ────────────────────────────────────────────────── -->
  <div class="card" id="loadtest-card">
    <div class="card-head">
      <span class="step-badge">4</span>
      <span class="card-title">Generate demo pipelines in Confluent Cloud</span>
    </div>
    <div class="card-help">
      Creates new test pipelines on Confluent Cloud — they'll show up in the
      lineage sync like any other workload.
    </div>
    <div class="form-grid">
      <div class="field-pair" style="grid-column: 1 / -1;">
        <label>Target environment</label>
        <select id="lt-env" style="width:100%; background:#0d1117; border:1px solid #30363d;
                                   border-radius:4px; color:#c9d1d9; padding:.45rem .6rem;
                                   font-size:.85rem; font-family:'SFMono-Regular',Consolas,monospace;">
        </select>
      </div>
      <div class="field-pair">
        <label>Number of pipelines</label>
        <input id="lt-num-pipelines" type="number" min="1" max="50" value="5"
               style="width:100%; background:#0d1117; border:1px solid #30363d;
                      border-radius:4px; color:#c9d1d9; padding:.45rem .6rem;
                      font-size:.85rem; font-family:'SFMono-Regular',Consolas,monospace;">
      </div>
      <div class="field-pair">
        <label>Max nodes per pipeline</label>
        <input id="lt-max-nodes" type="number" min="2" max="20" value="4"
               style="width:100%; background:#0d1117; border:1px solid #30363d;
                      border-radius:4px; color:#c9d1d9; padding:.45rem .6rem;
                      font-size:.85rem; font-family:'SFMono-Regular',Consolas,monospace;">
      </div>
    </div>
    <div class="card-help" style="margin-top:.4rem; font-size:.8rem;">
      Each pipeline gets a random length in [2, max nodes]. A node = a job
      (Datagen connector / Flink statement / consumer group). One Flink
      statement uses one CFU — keep <em>num × max</em> within your compute
      pool's CFU budget (default 10).
    </div>
    <div class="btn-row">
      <button id="lt-start-btn"    onclick="ltStart()">Provision demo pipelines</button>
      <button id="lt-stop-btn"     onclick="ltStop()"     class="danger" disabled>Stop</button>
      <button id="lt-teardown-btn" onclick="ltTeardown()" class="secondary">Delete all demo pipelines</button>
    </div>
    <div class="status" id="lt-status"></div>
    <pre id="lt-log" class="log hidden"></pre>
  </div>

</main>

<script>
function setHeaderUser(user) {
  const el = document.getElementById('g-user');
  if (user) {
    el.textContent = 'Confluent Cloud: ' + user;
    el.className   = 'badge ok';
  } else {
    el.textContent = 'Confluent Cloud: signed out';
    el.className   = 'badge dim';
  }
}

function setHeaderMarquez(api, ui) {
  const el = document.getElementById('g-marquez');
  if (api && ui) {
    el.textContent = 'Marquez ✓ ready';
    el.className   = 'badge ok';
  } else if (api) {
    el.textContent = 'Marquez ⚠ UI not running';
    el.className   = 'badge dim';
  } else {
    el.textContent = 'Marquez ✗ stopped';
    el.className   = 'badge bad';
  }
}

async function refreshStatus() {
  const r = await fetch('/status');
  const d = await r.json();
  setHeaderUser(d.user);
  if (d.user) {
    document.getElementById('email').value = d.user;
    document.getElementById('login-status').innerHTML =
      '<span class="ok">✓ signed in as ' + d.user + '</span>';
    document.getElementById('env-card').classList.remove('hidden');
    loadEnvironments();
    populateLoadTestEnvs();
  }
  if (d.env_count > 0) {
    document.getElementById('done-env').textContent = d.env_count;
    document.getElementById('done-card').classList.remove('hidden');
  }
}

async function doLogin() {
  const email = document.getElementById('email').value.trim();
  const pwd   = document.getElementById('password').value;
  const btn   = document.getElementById('login-btn');
  const stat  = document.getElementById('login-status');
  if (!email || !pwd) {
    stat.innerHTML = '<span class="err">enter email + password</span>';
    return;
  }
  btn.disabled = true;
  stat.innerHTML = '<span class="dim">signing in…</span>';
  try {
    const r = await fetch('/cc-login', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({email, password: pwd}),
    });
    const d = await r.json();
    if (!r.ok) {
      stat.innerHTML = '<span class="err">✗ ' + (d.error || 'login failed') + '</span>';
      btn.disabled = false;
      return;
    }
    stat.innerHTML = '<span class="ok">✓ signed in as ' + d.user + '</span>';
    setHeaderUser(d.user);
    document.getElementById('password').value = '';
    document.getElementById('env-card').classList.remove('hidden');
    loadEnvironments();
    populateLoadTestEnvs();
  } catch (e) {
    stat.innerHTML = '<span class="err">✗ ' + e.message + '</span>';
    btn.disabled = false;
  }
}

async function loadEnvironments() {
  const list = document.getElementById('env-list');
  const saveBtn = document.getElementById('env-save-btn');
  saveBtn.disabled = true;
  list.innerHTML = '<div class="dim">loading…</div>';

  // Fetch envs + already-configured envs in parallel
  let envsResp, configuredResp;
  try {
    [envsResp, configuredResp] = await Promise.all([
      fetch('/environments').then(r => r.json().then(d => ({ok: r.ok, d}))),
      fetch('/envs/configured').then(r => r.json().then(d => ({ok: r.ok, d}))),
    ]);
  } catch (e) {
    list.innerHTML = '<div class="err">' + e.message + '</div>';
    return;
  }
  if (!envsResp.ok) {
    list.innerHTML = '<div class="err">' + (envsResp.d.error || 'failed to list') + '</div>';
    return;
  }
  const envs = envsResp.d.environments || [];
  if (envs.length === 0) {
    list.innerHTML = '<div class="dim">no environments visible</div>';
    return;
  }

  // Map env_id → {pending: bool} so we can distinguish "selected" (pending
  // provisioning) from "provisioned" (already has Kafka/SR keys minted).
  const configured = new Map(
    (configuredResp.d.environments || []).map(e => [e.env_id, {pending: !!e.pending}])
  );

  list.innerHTML = '';
  for (const e of envs) {
    const cfg       = configured.get(e.id);
    const isChecked = !!cfg;
    const isPending = cfg ? cfg.pending : false;
    const label     = isChecked ? (isPending ? '✓ selected' : '✓ provisioned') : '';
    const cls       = isChecked ? (isPending ? 'selected' : 'saved') : '';
    const row = document.createElement('label');
    row.className = 'env-row' + (isChecked ? ' selected' : '');
    row.innerHTML =
      '<input type="checkbox" data-env-id="' + e.id + '" data-env-name="' +
        escapeHtml(e.name) + '"' + (isChecked ? ' checked' : '') + '>' +
      '<span class="env-name">' + escapeHtml(e.name) + '</span>' +
      '<span class="env-id">' + e.id + '</span>' +
      '<span class="env-status ' + cls + '" id="env-stat-' + e.id + '">' +
        label + '</span>';
    const cb = row.querySelector('input');
    cb.addEventListener('change', () => {
      row.classList.toggle('selected', cb.checked);
      saveBtn.disabled = false;     // any change (check or uncheck) is saveable
    });
    list.appendChild(row);
  }
  saveBtn.disabled = false;        // allow saving an empty selection (clears it)
  updateBridgeIntro(envs, configured);
}

function updateBridgeIntro(envs, configured) {
  const intro = document.getElementById('bridge-intro');
  if (!intro) return;
  const nameById = new Map(envs.map(e => [e.id, e.name]));
  const selected = Array.from(configured.keys()).map(id => ({
    id, name: nameById.get(id) || ''
  }));
  if (selected.length === 0) {
    intro.innerHTML = 'No environments selected — pick at least one above.';
    return;
  }
  const list = selected.map(e =>
    '<strong>' + escapeHtml(e.name || e.id) + '</strong> <span class="dim">(' + e.id + ')</span>'
  ).join(', ');
  const verb = selected.length === 1 ? 'is' : 'are';
  intro.innerHTML = list + ' ' + verb +
    ' selected. Click below to start the OpenLineage flow.';
}

async function saveEnvs() {
  const checked = Array.from(document.querySelectorAll('#env-list input:checked'));
  const stat = document.getElementById('env-status');
  const saveBtn = document.getElementById('env-save-btn');
  saveBtn.disabled = true;
  stat.innerHTML = '<span class="dim">saving selection…</span>';

  const payload = {
    envs: checked.map(cb => ({env_id: cb.dataset.envId, env_name: cb.dataset.envName})),
  };
  const r = await fetch('/envs/select', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload),
  });
  const d = await r.json();
  if (!r.ok) {
    stat.innerHTML = '<span class="err">✗ ' + (d.error || 'save failed') + '</span>';
    saveBtn.disabled = false;
    return;
  }

  // Re-render the env list so labels reflect the new pending/provisioned state.
  await loadEnvironments();

  const n = checked.length;
  stat.innerHTML = n > 0
    ? '<span class="ok">✓ ' + n + ' env(s) selected — Kafka + SR keys are minted on Start OpenLineage</span>'
    : '<span class="dim">selection cleared</span>';
  document.getElementById('done-env').textContent = n;
  if (n > 0) document.getElementById('done-card').classList.remove('hidden');
  // Refresh load-test card env dropdown
  populateLoadTestEnvs();
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"]/g,
    c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
}

// ── Marquez control ─────────────────────────────────────────────────────────
async function mqRefresh() {
  const stat = document.getElementById('mq-status');
  stat.innerHTML = '<span class="dim">checking…</span>';
  const r = await fetch('/marquez/status');
  const d = await r.json();
  document.getElementById('mq-ui-dir').textContent = d.patched_ui_dir;
  setHeaderMarquez(d.api_running, d.patched_ui_running);
  const apiBit = d.api_running
    ? '<span class="ok">✓ Backend ready</span>'
    : '<span class="err">✗ Backend not running</span>';
  const uiBit = d.patched_ui_running
    ? '<span class="ok">✓ UI ready</span>'
    : (d.patched_ui_dir_present
        ? '<span class="dim">UI not running</span>'
        : '<span class="err">UI source not found</span>');
  stat.innerHTML = apiBit + ' &nbsp;·&nbsp; ' + uiBit;
}

async function mqAction(action) {
  const stat = document.getElementById('mq-status');
  const btnU = document.getElementById('mq-up-btn');
  const btnD = document.getElementById('mq-down-btn');
  const btnW = document.getElementById('mq-wipe-btn');
  btnU.disabled = btnD.disabled = btnW.disabled = true;
  stat.innerHTML = '<span class="dim">running make marquez-' + action + ' …</span>';
  try {
    const r = await fetch('/marquez/' + action, {method:'POST'});
    const d = await r.json();
    if (!r.ok) {
      stat.innerHTML = '<span class="err">✗ ' + (d.error || 'failed') + '</span>';
    } else {
      // make + (npm run dev|stop) already done by the time POST returns;
      // give postgres / api / webpack a moment then refresh status
      stat.innerHTML = '<span class="dim">' + action + ' done; refreshing status…</span>';
      await new Promise(r => setTimeout(r, 1500));
      await mqRefresh();
    }
  } catch (e) {
    stat.innerHTML = '<span class="err">✗ ' + e.message + '</span>';
  } finally {
    btnU.disabled = btnD.disabled = btnW.disabled = false;
  }
}

async function mqWipe() {
  if (!confirm('Delete ALL Marquez data?\\n\\n' +
               '  • Drops the postgres volume\\n' +
               '  • Every namespace, dataset, job, and run is gone\\n' +
               '  • Marquez restarts empty — perfect for a clean run\\n\\n' +
               'The bridge MUST be stopped first (the server will refuse otherwise).\\n\\n' +
               'This cannot be undone.')) return;
  await mqAction('wipe');
}

// ── Bridge control (ol-confluent run) ───────────────────────────────────────
let bridgeStream = null;

async function bridgeStart() {
  const stat = document.getElementById('bridge-status');
  const log  = document.getElementById('bridge-log');
  document.getElementById('bridge-start-btn').disabled = true;
  stat.innerHTML = '<span class="dim">starting bridge…</span>';
  log.classList.remove('hidden');
  log.textContent = '';
  try {
    const r = await fetch('/bridge/start', {method: 'POST'});
    const d = await r.json();
    if (!r.ok) {
      stat.innerHTML = '<span class="err">✗ ' + (d.error || 'start failed') + '</span>';
      document.getElementById('bridge-start-btn').disabled = false;
      return;
    }
    stat.innerHTML = '<span class="ok">✓ bridge ' + d.msg + '</span>';
    document.getElementById('bridge-stop-btn').disabled = false;
    bridgeOpenStream();
  } catch (e) {
    stat.innerHTML = '<span class="err">✗ ' + e.message + '</span>';
    document.getElementById('bridge-start-btn').disabled = false;
  }
}

async function bridgeStop() {
  const stat = document.getElementById('bridge-status');
  stat.innerHTML = '<span class="dim">stopping…</span>';
  const r = await fetch('/bridge/stop', {method: 'POST'});
  const d = await r.json();
  stat.innerHTML = '<span class="' + (r.ok ? 'ok' : 'err') + '">' +
                   (r.ok ? '✓ ' : '✗ ') + (d.msg || d.error) + '</span>';
}

function bridgeOpenStream() {
  if (bridgeStream) bridgeStream.close();
  const log = document.getElementById('bridge-log');
  bridgeStream = new EventSource('/bridge/stream');
  bridgeStream.addEventListener('line', e => {
    log.textContent += JSON.parse(e.data) + '\\n';
    log.scrollTop = log.scrollHeight;
  });
  bridgeStream.addEventListener('done', e => {
    const d = JSON.parse(e.data);
    document.getElementById('bridge-status').innerHTML =
      '<span class="' + (d.rc === 0 ? 'ok' : 'err') + '">' +
      (d.rc === 0 ? '✓' : '✗') + ' bridge exited (rc=' + d.rc + ')</span>';
    document.getElementById('bridge-start-btn').disabled = false;
    document.getElementById('bridge-stop-btn').disabled  = true;
    bridgeStream.close(); bridgeStream = null;
  });
  bridgeStream.onerror = () => {
    if (bridgeStream) { bridgeStream.close(); bridgeStream = null; }
    document.getElementById('bridge-start-btn').disabled = false;
  };
}

// ── Load test control ───────────────────────────────────────────────────────
let ltStream = null;

async function populateLoadTestEnvs() {
  const sel = document.getElementById('lt-env');
  if (!sel) return;
  try {
    // List ALL Confluent Cloud envs visible to the account — provisioning can
    // bootstrap an env from scratch (creates cluster + SR + Flink pool), so we
    // do not restrict to envs already saved in config.yaml.
    const r = await fetch('/environments');
    const d = await r.json();
    if (!r.ok) throw new Error(d.error || 'failed');
    const envs = d.environments || [];
    const prev = sel.value;
    sel.innerHTML = '';
    if (envs.length === 0) {
      const o = document.createElement('option');
      o.value = ''; o.textContent = '(no environments visible — sign in to Confluent Cloud first)';
      o.disabled = true; sel.appendChild(o);
      document.getElementById('lt-start-btn').disabled = true;
      document.getElementById('lt-teardown-btn').disabled = true;
      return;
    }
    for (const e of envs) {
      const o = document.createElement('option');
      o.value = e.id;
      o.textContent = e.name || e.id;
      sel.appendChild(o);
    }
    if (prev && envs.some(e => e.id === prev)) sel.value = prev;
    document.getElementById('lt-start-btn').disabled = false;
    document.getElementById('lt-teardown-btn').disabled = false;
  } catch (e) {
    sel.innerHTML = '';
    const o = document.createElement('option');
    o.value = ''; o.textContent = 'failed to load env list: ' + e.message;
    sel.appendChild(o);
  }
}

async function ltStart() {
  const env  = document.getElementById('lt-env').value;
  const num  = parseInt(document.getElementById('lt-num-pipelines').value, 10) || 5;
  const max  = parseInt(document.getElementById('lt-max-nodes').value, 10) || 4;
  const stat = document.getElementById('lt-status');
  if (!env) {
    stat.innerHTML = '<span class="err">pick a target environment first</span>';
    return;
  }
  document.getElementById('lt-start-btn').disabled = true;
  stat.innerHTML = '<span class="dim">provisioning ' + num + ' pipeline(s), max ' + max +
                   ' nodes each…</span>';
  document.getElementById('lt-log').classList.remove('hidden');
  document.getElementById('lt-log').textContent = '';
  try {
    const r = await fetch('/loadtest/start', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({env_id: env, num_pipelines: num, max_nodes: max}),
    });
    const d = await r.json();
    if (!r.ok) {
      stat.innerHTML = '<span class="err">✗ ' + (d.error || 'start failed') + '</span>';
      document.getElementById('lt-start-btn').disabled = false;
      return;
    }
    stat.innerHTML = '<span class="ok">✓ ' + d.msg + '</span>';
    document.getElementById('lt-stop-btn').disabled = false;
    ltOpenStream();
  } catch (e) {
    stat.innerHTML = '<span class="err">✗ ' + e.message + '</span>';
    document.getElementById('lt-start-btn').disabled = false;
  }
}

function ltOpenStream() {
  if (ltStream) ltStream.close();
  const log = document.getElementById('lt-log');
  ltStream = new EventSource('/loadtest/stream');
  ltStream.addEventListener('line', e => {
    log.textContent += JSON.parse(e.data) + '\\n';
    log.scrollTop = log.scrollHeight;
  });
  ltStream.addEventListener('done', e => {
    const d = JSON.parse(e.data);
    document.getElementById('lt-status').innerHTML =
      '<span class="' + (d.rc === 0 ? 'ok' : 'err') + '">'
      + (d.rc === 0 ? '✓' : '✗') + ' load test finished (rc=' + d.rc + ')</span>';
    document.getElementById('lt-start-btn').disabled = false;
    document.getElementById('lt-stop-btn').disabled  = true;
    ltStream.close(); ltStream = null;
  });
  ltStream.onerror = () => {
    if (ltStream) { ltStream.close(); ltStream = null; }
    document.getElementById('lt-start-btn').disabled = false;
  };
}

async function ltStop() {
  const stat = document.getElementById('lt-status');
  stat.innerHTML = '<span class="dim">stopping…</span>';
  const r = await fetch('/loadtest/stop', {method:'POST'});
  const d = await r.json();
  stat.innerHTML = '<span class="' + (r.ok ? 'ok' : 'err') + '">'
                 + (r.ok ? '✓ ' : '✗ ') + (d.msg || d.error) + '</span>';
}

async function ltTeardown() {
  const sel  = document.getElementById('lt-env');
  const env  = sel.value;
  const envName = sel.options[sel.selectedIndex]?.text || env;
  const envLabel = (envName && envName !== env) ? envName + ' (' + env + ')' : env;
  const stat = document.getElementById('lt-status');
  if (!env) { stat.innerHTML = '<span class="err">pick an environment first</span>'; return; }
  if (!confirm('Tear down ALL resources in env ' + envLabel + ':\\n\\n' +
               '  • Every ol-* connector, Flink statement, and Schema Registry subject\\n' +
               '  • EVERY non-system Kafka topic (incl. dlq-lcc-*, error-lcc-*, success-lcc-*, ' +
               'and any user topic in the env regardless of name)\\n' +
               '  • All consumer worker processes started by the provisioner\\n\\n' +
               'This cannot be undone.')) return;
  const btn = document.getElementById('lt-teardown-btn');
  btn.disabled = true;
  stat.innerHTML = '<span class="dim">deleting all ol-* demo pipelines (state-tracked + stateless sweep)…</span>';
  document.getElementById('lt-log').classList.remove('hidden');
  document.getElementById('lt-log').textContent = '';
  try {
    const r = await fetch('/loadtest/teardown', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({env_id: env}),
    });
    const d = await r.json();
    if (!r.ok) {
      stat.innerHTML = '<span class="err">✗ ' + (d.error || 'teardown failed') + '</span>';
    } else {
      stat.innerHTML = '<span class="ok">✓ ' + d.msg + '</span>';
      ltOpenStream();
    }
  } catch (e) {
    stat.innerHTML = '<span class="err">✗ ' + e.message + '</span>';
  } finally {
    btn.disabled = false;
  }
}

// On page load: refresh Marquez status and (if a load test was already
// running from a prior session) reattach the stream.
mqRefresh();
fetch('/loadtest/status').then(r => r.json()).then(d => {
  if (d.running) {
    document.getElementById('lt-log').classList.remove('hidden');
    document.getElementById('lt-stop-btn').disabled = false;
    document.getElementById('lt-start-btn').disabled = true;
    document.getElementById('lt-status').innerHTML =
      '<span class="ok">✓ provisioning already running (started by prior page)</span>';
    ltOpenStream();
  }
});
fetch('/bridge/status').then(r => r.json()).then(d => {
  if (d.running) {
    document.getElementById('bridge-log').classList.remove('hidden');
    document.getElementById('bridge-start-btn').disabled = true;
    document.getElementById('bridge-stop-btn').disabled  = false;
    document.getElementById('bridge-status').innerHTML =
      '<span class="ok">✓ bridge already running (started by prior page)</span>';
    bridgeOpenStream();
  }
});

refreshStatus();
populateLoadTestEnvs();
</script>
</body></html>
"""


# ──────────────────────────────────────────────────────────────────────────────
# HTTP handler
# ──────────────────────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def _send_json(self, code: int, body: dict) -> None:
        payload = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/":
            page = PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(page)))
            self.end_headers()
            self.wfile.write(page)
        elif path == "/status":
            self._send_json(200, {
                "user":      _current_user(),
                "env_count": len(_configured_env_ids()),
            })
        elif path == "/environments":
            user = _current_user()
            if not user:
                self._send_json(401, {"error": "not signed in"})
                return
            envs = _list_environments()
            self._send_json(200, {"environments": envs})
        elif path == "/envs/configured":
            self._send_json(200, {"environments": _configured_env_ids()})
        elif path == "/marquez/status":
            self._send_json(200, _marquez_status())
        elif path == "/loadtest/status":
            self._send_json(200, _lt_status())
        elif path == "/loadtest/stream":
            self._sse_stream(_lt_state, _lt_lock)
        elif path == "/bridge/status":
            self._send_json(200, _bridge_status())
        elif path == "/bridge/stream":
            self._sse_stream(_bridge_state, _bridge_lock)
        else:
            self.send_error(404)

    def _sse_stream(self, state: dict, lock: threading.Lock) -> None:
        """Tail state["lines"] over SSE; emit `done` event when proc exits."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        try:
            cursor = 0
            while True:
                with lock:
                    lines = list(state["lines"])
                    done  = state["done"]
                    rc    = state["rc"]
                new = lines[cursor:]
                for ln in new:
                    self.wfile.write(
                        f"event: line\ndata: {json.dumps(ln)}\n\n".encode()
                    )
                if new:
                    self.wfile.flush()
                    cursor += len(new)
                if done and cursor >= len(lines):
                    self.wfile.write(
                        f"event: done\ndata: {json.dumps({'rc': rc})}\n\n".encode()
                    )
                    self.wfile.flush()
                    return
                try:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    return
                time.sleep(1.0)
        except (BrokenPipeError, ConnectionResetError):
            return

    def do_POST(self):
        path   = urlparse(self.path).path
        length = int(self.headers.get("Content-Length", 0))
        try:
            data = json.loads(self.rfile.read(length)) if length else {}
        except json.JSONDecodeError:
            self._send_json(400, {"error": "invalid JSON"})
            return

        if path == "/cc-login":
            email = (data.get("email") or "").strip()
            pwd   = data.get("password") or ""
            if not email or not pwd:
                self._send_json(400, {"error": "email + password required"})
                return
            rc, _, err = _run_confluent(
                ["login", "--save"],
                env={"CONFLUENT_CLOUD_EMAIL": email, "CONFLUENT_CLOUD_PASSWORD": pwd},
                timeout=30,
            )
            if rc != 0:
                # CLI prints "Error: ..." messages on stderr
                msg = err.strip().splitlines()[0] if err.strip() else "login failed"
                self._send_json(401, {"error": msg[:200]})
                return
            user = _current_user() or email
            self._send_json(200, {"user": user})

        elif path == "/envs/select":
            envs = data.get("envs") or []
            if not isinstance(envs, list):
                self._send_json(400, {"error": "envs (list) required"})
                return
            user = _current_user()
            if not user:
                self._send_json(401, {"error": "not signed in"})
                return

            # Lazy: persist the selection only. Cluster + key minting happens on
            # Start OpenLineage (see _bridge_start) so the user can pick freely
            # without burning API keys or surfacing CLI errors per save.
            already_provisioned = {e["env_id"] for e in _configured_env_ids() if not e["pending"]}
            cleaned: list[dict] = []
            results: list[dict] = []
            for entry in envs:
                env_id   = (entry.get("env_id")   or "").strip()
                env_name = (entry.get("env_name") or "").strip()
                if not env_id:
                    results.append({"env_id": "", "ok": False, "error": "missing env_id"})
                    continue
                # Don't shadow a fully-provisioned env with a pending entry.
                if env_id not in already_provisioned:
                    cleaned.append({"env_id": env_id, "env_name": env_name})
                results.append({"env_id": env_id, "ok": True})

            try:
                _write_selected_envs(cleaned)
            except Exception as exc:    # noqa: BLE001
                self._send_json(500, {"error": f"failed to write config.yaml: {exc}"})
                return

            self._send_json(200, {"results": results})

        elif path in ("/marquez/up", "/marquez/down", "/marquez/wipe"):
            action = path.rsplit("/", 1)[1]
            # Wiping while the bridge is polling guarantees a flood of
            # connection errors (and possibly partial re-emission against the
            # fresh DB before the user expects it). Refuse and tell the user.
            if action == "wipe" and _bridge_running():
                self._send_json(409, {"error":
                    "Bridge is currently running — Stop OpenLineage first, "
                    "then wipe Marquez."})
                return
            rc, out = _marquez_action(action)
            if rc != 0:
                # Tail of output is usually the relevant error
                tail = "\n".join(out.splitlines()[-12:]) if out else ""
                self._send_json(500, {"error": tail or f"exit {rc}"})
                return
            self._send_json(200, {"ok": True, "output": out[-2000:]})

        elif path == "/loadtest/start":
            env_id = (data.get("env_id") or "").strip()
            if not env_id:
                self._send_json(400, {"error": "env_id required"})
                return
            try:
                num_pipelines = int(data.get("num_pipelines", 10))
                max_nodes     = int(data.get("max_nodes",     4))
            except (TypeError, ValueError):
                self._send_json(400, {"error": "num_pipelines and max_nodes must be integers"})
                return
            ok, msg = _lt_start_provision(
                env_id, mode="provision",
                num_pipelines=num_pipelines, max_nodes=max_nodes,
            )
            if not ok:
                self._send_json(409 if "running" in msg else 400, {"error": msg})
                return
            self._send_json(200, {"ok": True, "msg": msg})

        elif path == "/loadtest/stop":
            ok, msg = _lt_stop()
            self._send_json(200 if ok else 409, {"ok": ok, "msg": msg})

        elif path == "/loadtest/teardown":
            env_id = (data.get("env_id") or "").strip()
            if not env_id:
                self._send_json(400, {"error": "env_id required"})
                return
            ok, msg = _lt_start_provision(env_id, mode="teardown")
            if not ok:
                self._send_json(409 if "running" in msg else 400, {"error": msg})
                return
            self._send_json(200, {"ok": True, "msg": msg})

        elif path == "/bridge/start":
            ok, msg = _bridge_start()
            if not ok:
                self._send_json(409 if "running" in msg else 400, {"error": msg})
                return
            self._send_json(200, {"ok": True, "msg": msg})

        elif path == "/bridge/stop":
            ok, msg = _bridge_stop()
            self._send_json(200 if ok else 409, {"ok": ok, "msg": msg})

        else:
            self.send_error(404)

    def log_message(self, fmt, *args):
        # Quiet stdout; let real errors surface via tracebacks
        pass


def main() -> None:
    print(f"OpenLineage-Confluent setup wizard → http://localhost:{PORT}")
    print("Press Ctrl-C to stop.")
    try:
        ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()

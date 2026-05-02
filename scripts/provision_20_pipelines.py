#!/usr/bin/env python3
"""Provision / teardown 20 OpenLineage demo pipelines in Confluent Cloud.

Topology per new domain (17 with Flink, 2 Datagen-only):
    Datagen → ol-{domain}-raw → [Flink enrich] → ol-{domain}-enriched
                                                        ↓
                                               consumer group ol-{domain}-cg

Existing orders domain (3 Flink statements + 2 connectors) stays untouched.

Usage:
    python scripts/provision_20_pipelines.py --env <env_id> --provision
    python scripts/provision_20_pipelines.py --env <env_id> --teardown
    python scripts/provision_20_pipelines.py --env <env_id> --status

All Confluent Cloud creds (cluster_id, Kafka API key, Schema Registry endpoint
+ key, Flink compute pool) are read from config.yaml's `environments:` list
under the matching env_id. Populate config.yaml via the setup wizard first.

Requirements:
    pip install confluent-kafka   (for consumer groups)
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

# Make the project package importable when running this script directly.
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from openlineage_confluent.config import AppConfig  # noqa: E402

# ─── Per-env config (populated in main() from config.yaml) ───────────────────
ENV_ID:          str = ""
CLUSTER_ID:      str = ""
FLINK_POOL:      str = ""
SR_ENDPOINT:     str = ""
SR_KEY:          str = ""
SR_SECRET:       str = ""
KAFKA_BOOTSTRAP: str = ""
KAFKA_API_KEY:   str = ""
KAFKA_API_SECRET: str = ""

# Per-env state file paths (set in main() so multi-env doesn't clobber)
STATE_FILE: Path = SCRIPT_DIR / "demo_20_state.json"
PIDS_FILE:  Path = SCRIPT_DIR / "demo_20_consumers.pids"
WORKER_SCRIPT = SCRIPT_DIR / "demo_20_consumer_worker.py"


def _load_env_creds(config_path: Path, env_id: str) -> None:
    """Populate the module-level config constants from config.yaml.

    Sets STATE_FILE / PIDS_FILE per env so multiple envs can be provisioned
    without their state files clobbering each other.
    """
    global ENV_ID, CLUSTER_ID, FLINK_POOL
    global SR_ENDPOINT, SR_KEY, SR_SECRET
    global KAFKA_BOOTSTRAP, KAFKA_API_KEY, KAFKA_API_SECRET
    global STATE_FILE, PIDS_FILE

    cfg = AppConfig.from_yaml(config_path)
    env = next((e for e in cfg.confluent.environments if e.env_id == env_id), None)
    if env is None:
        sys.exit(
            f"ERROR: env_id '{env_id}' not found in {config_path}. "
            f"Available: {[e.env_id for e in cfg.confluent.environments]}"
        )

    ENV_ID          = env.env_id
    CLUSTER_ID      = env.cluster_id
    FLINK_POOL      = env.flink_compute_pool or ""
    KAFKA_BOOTSTRAP = env.kafka_bootstrap
    KAFKA_API_KEY   = env.kafka_api_key or ""
    KAFKA_API_SECRET = (
        env.kafka_api_secret.get_secret_value() if env.kafka_api_secret else ""
    )

    if env.schema_registry is not None:
        SR_ENDPOINT = env.schema_registry.endpoint
        SR_KEY      = env.schema_registry.api_key
        SR_SECRET   = env.schema_registry.api_secret.get_secret_value()

    if not (CLUSTER_ID and KAFKA_API_KEY and KAFKA_API_SECRET):
        sys.exit(
            f"ERROR: env '{env_id}' is missing cluster_id or Kafka API key/secret in config.yaml. "
            f"Re-run the setup wizard to populate per-env credentials."
        )
    if not FLINK_POOL:
        print(
            f"WARNING: env '{env_id}' has no flink_compute_pool set; "
            f"Flink statements cannot be created. Set EnvDeployment.flink_compute_pool."
        )

    STATE_FILE = SCRIPT_DIR / f"demo_20_state_{env_id}.json"
    PIDS_FILE  = SCRIPT_DIR / f"demo_20_consumers_{env_id}.pids"

# ─── Domains ─────────────────────────────────────────────────────────────────
# 17 new domains with 1 Flink statement each = 17 CFU → total 20 with existing 3
FLINK_DOMAINS = [
    "payments", "shipments", "inventory", "users", "sessions",
    "clicks", "products", "reviews", "returns", "fraud",
    "risk", "notifications", "emails", "sms", "webhooks",
    "audit", "metrics",
]

# 2 domains without Flink: Datagen + consumer group only (CFU budget exhausted)
NO_FLINK_DOMAINS = ["analytics", "reports"]

ALL_NEW_DOMAINS = FLINK_DOMAINS + NO_FLINK_DOMAINS  # 19 new + existing orders = 20 total

# ─── Enriched AVRO schema ─────────────────────────────────────────────────────
ENRICHED_SCHEMA = json.dumps({
    "type": "record",
    "name": "orders_enriched",
    "namespace": "io.confluent.openlineage",
    "fields": [
        {"name": "ordertime",  "type": "long"},
        {"name": "orderid",    "type": "int"},
        {"name": "itemid",     "type": "string"},
        {"name": "orderunits", "type": "double"},
        {"name": "city",       "type": "string"},
        {"name": "state",      "type": "string"},
        {"name": "zipcode",    "type": "long"},
        {"name": "risk_tier",  "type": "string"},
    ],
})

ENRICH_SQL = """\
INSERT INTO `ol-{domain}-enriched`
SELECT
  ordertime,
  orderid,
  itemid,
  orderunits,
  address.city    AS city,
  address.state   AS state,
  address.zipcode AS zipcode,
  CASE
    WHEN orderunits > 10 THEN 'HIGH'
    WHEN orderunits > 5  THEN 'MEDIUM'
    ELSE                      'LOW'
  END AS risk_tier
FROM `ol-{domain}-raw`;
"""

# ─── Helpers ─────────────────────────────────────────────────────────────────

def run(args: list[str], *, capture: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=capture, text=True, input="y\n")


def ok(result: subprocess.CompletedProcess) -> bool:
    return result.returncode == 0


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"connectors": {}, "flink_statements": [], "topics": []}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


def sr_register(topic: str) -> bool:
    subject = f"{topic}-value"
    url = f"{SR_ENDPOINT}/subjects/{subject}/versions"
    try:
        r = httpx.post(url, json={"schema": ENRICHED_SCHEMA, "schemaType": "AVRO"},
                       auth=(SR_KEY, SR_SECRET), timeout=30)
        if r.status_code in (200, 201):
            print(f"  SR ✓ {subject} (id={r.json().get('id')})")
            return True
        print(f"  SR ✗ {subject}: {r.status_code} {r.text[:120]}")
        return False
    except Exception as e:
        print(f"  SR error {subject}: {e}")
        return False


def sr_delete(topic: str) -> None:
    subject = f"{topic}-value"
    try:
        r = httpx.delete(f"{SR_ENDPOINT}/subjects/{subject}",
                         auth=(SR_KEY, SR_SECRET), timeout=30)
        print(f"  SR deleted {subject}: {r.status_code}")
    except Exception as e:
        print(f"  SR delete error {subject}: {e}")


def create_topic(topic: str, state_topics: list) -> bool:
    if topic in state_topics:
        return True
    r = run(["confluent", "kafka", "topic", "create", topic,
             "--partitions", "6", "--cluster", CLUSTER_ID, "--environment", ENV_ID])
    combined = (r.stdout + r.stderr).lower()
    if ok(r) or "already exists" in combined:
        print(f"  topic ✓ {topic}")
        state_topics.append(topic)
        return True
    print(f"  topic ✗ {topic}: {r.stderr.strip()[:120]}")
    return False


def create_connector(domain: str) -> str | None:
    config = {
        "connector.class": "DatagenSource",
        "name": f"ol-{domain}-datagen",
        "kafka.api.key": KAFKA_API_KEY,
        "kafka.api.secret": KAFKA_API_SECRET,
        "kafka.topic": f"ol-{domain}-raw",
        "output.data.format": "AVRO",
        "quickstart": "ORDERS",
        "max.interval": "1000",
        "tasks.max": "1",
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"name": f"ol-{domain}-datagen", "config": config}, f)
        tmp = f.name
    try:
        r = run(["confluent", "connect", "cluster", "create",
                 "--config-file", tmp,
                 "--cluster", CLUSTER_ID, "--environment", ENV_ID, "-o", "json"])
        os.unlink(tmp)
        if ok(r):
            conn_id = json.loads(r.stdout).get("id", "")
            print(f"  connector ✓ ol-{domain}-datagen ({conn_id})")
            return conn_id
        print(f"  connector ✗ ol-{domain}-datagen: {r.stderr.strip()[:120]}")
        return None
    except Exception as e:
        print(f"  connector exception {domain}: {e}")
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return None


def create_flink_statement(domain: str) -> bool:
    sql = ENRICH_SQL.format(domain=domain)
    name = f"ol-{domain}-enrich"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql)
        tmp = f.name
    try:
        r = run(["confluent", "flink", "statement", "create", name,
                 "--sql", sql,
                 "--compute-pool", FLINK_POOL, "--environment", ENV_ID])
        os.unlink(tmp)
        if ok(r):
            print(f"  flink ✓ {name}")
            return True
        print(f"  flink ✗ {name}: {r.stderr.strip()[:120]}")
        return False
    except Exception as e:
        print(f"  flink exception {domain}: {e}")
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return False


def _write_worker() -> None:
    WORKER_SCRIPT.write_text("""\
#!/usr/bin/env python3
import os, sys, time
topic  = sys.argv[1]
group  = sys.argv[2]
try:
    from confluent_kafka import Consumer, KafkaError
except ImportError:
    sys.exit(0)
cfg = {
    "bootstrap.servers":  os.environ["KAFKA_BOOTSTRAP"],
    "security.protocol":  "SASL_SSL",
    "sasl.mechanism":     "PLAIN",
    "sasl.username":      os.environ["KAFKA_API_KEY"],
    "sasl.password":      os.environ["KAFKA_API_SECRET"],
    "group.id":           group,
    "auto.offset.reset":  "latest",
    "enable.auto.commit": True,
}
c = Consumer(cfg)
c.subscribe([topic])
while True:
    msg = c.poll(timeout=5.0)
    if msg is None:
        continue
    if msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
        time.sleep(1)
""")


def start_consumers(domains_and_topics: list[tuple[str, str]]) -> list[int]:
    try:
        import confluent_kafka  # noqa: F401
    except ImportError:
        print("  WARNING: confluent-kafka not installed — skipping consumer groups.")
        print("  Install with: pip install confluent-kafka")
        return []

    _write_worker()
    pids = []
    env = {**os.environ,
           "KAFKA_BOOTSTRAP": KAFKA_BOOTSTRAP,
           "KAFKA_API_KEY": KAFKA_API_KEY,
           "KAFKA_API_SECRET": KAFKA_API_SECRET}
    for domain, topic in domains_and_topics:
        group = f"ol-{domain}-cg"
        proc = subprocess.Popen(
            [sys.executable, str(WORKER_SCRIPT), topic, group],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        pids.append(proc.pid)
        print(f"  consumer ✓ {group} → {topic} (pid={proc.pid})")
    return pids


# ─── Provision ───────────────────────────────────────────────────────────────

def provision() -> None:
    state = load_state()
    topics: list = state.setdefault("topics", [])
    connectors: dict = state.setdefault("connectors", {})
    statements: list = state.setdefault("flink_statements", [])

    print("\n══ Step 1/6 — Create Kafka topics ══")
    for domain in ALL_NEW_DOMAINS:
        create_topic(f"ol-{domain}-raw", topics)
        if domain in FLINK_DOMAINS:
            create_topic(f"ol-{domain}-enriched", topics)
    save_state(state)

    print("\n══ Step 2/6 — Register enriched schemas in Schema Registry ══")
    for domain in FLINK_DOMAINS:
        sr_register(f"ol-{domain}-enriched")

    print("\n══ Step 3/6 — Create Datagen connectors ══")
    for domain in ALL_NEW_DOMAINS:
        if domain not in connectors:
            conn_id = create_connector(domain)
            if conn_id:
                connectors[domain] = conn_id
    save_state(state)

    print("\n══ Step 4/6 — Waiting 90s for Datagen to register raw AVRO schemas ══")
    for remaining in range(90, 0, -15):
        print(f"  {remaining}s ...", flush=True)
        time.sleep(15)

    print("\n══ Step 5/6 — Create Flink enrichment statements ══")
    for domain in FLINK_DOMAINS:
        stmt = f"ol-{domain}-enrich"
        if stmt not in statements:
            if create_flink_statement(domain):
                statements.append(stmt)
    save_state(state)

    print("\n══ Step 6/6 — Start consumer groups ══")
    pairs = [
        (d, f"ol-{d}-enriched" if d in FLINK_DOMAINS else f"ol-{d}-raw")
        for d in ALL_NEW_DOMAINS
    ]
    pids = start_consumers(pairs)
    if pids:
        PIDS_FILE.write_text("\n".join(str(p) for p in pids))

    connectors_ok = len(connectors)
    flink_ok = len(statements)
    print(f"""
══ Provisioning complete ══
  Domains total  : 20 (19 new + existing orders)
  Connectors     : {connectors_ok}/19
  Flink statements: {flink_ok}/17
  Topics         : {len(topics)}/36
  Consumer pids  : {len(pids)}

Next steps:
  1. Wait 2-3 min for Metrics API to ingest consumer group data
  2. cd ~/openlineage-confluent && ol-confluent run-once --config config.yaml
  3. Open Marquez: http://localhost:3000
""")


# ─── Teardown ────────────────────────────────────────────────────────────────

def teardown() -> None:
    state = load_state()

    print("\n══ Stopping consumer processes ══")
    if PIDS_FILE.exists():
        for line in PIDS_FILE.read_text().splitlines():
            try:
                os.kill(int(line.strip()), 15)
                print(f"  killed pid {line.strip()}")
            except (ProcessLookupError, ValueError, OSError):
                pass
        PIDS_FILE.unlink(missing_ok=True)
    WORKER_SCRIPT.unlink(missing_ok=True)

    print("\n══ Deleting Flink statements ══")
    for stmt in state.get("flink_statements", []):
        r = run(["confluent", "flink", "statement", "delete", stmt,
                 "--environment", ENV_ID, "--force"])
        print(f"  {'✓' if ok(r) else '✗'} {stmt}")

    print("\n══ Deleting connectors ══")
    for domain, conn_id in state.get("connectors", {}).items():
        r = run(["confluent", "connect", "cluster", "delete", conn_id,
                 "--cluster", CLUSTER_ID, "--environment", ENV_ID, "--force"])
        print(f"  {'✓' if ok(r) else '✗'} ol-{domain}-datagen ({conn_id})")

    print("\n══ Deleting topics ══")
    for topic in state.get("topics", []):
        r = run(["confluent", "kafka", "topic", "delete", topic,
                 "--cluster", CLUSTER_ID, "--environment", ENV_ID])
        print(f"  {'✓' if ok(r) else '✗'} {topic}")

    print("\n══ Deleting Schema Registry schemas ══")
    for domain in FLINK_DOMAINS:
        sr_delete(f"ol-{domain}-enriched")

    STATE_FILE.unlink(missing_ok=True)
    print("\n══ Teardown complete ══")


# ─── Status ──────────────────────────────────────────────────────────────────

def status() -> None:
    state = load_state()
    connectors = state.get("connectors", {})
    statements = state.get("flink_statements", [])
    topics = state.get("topics", [])
    pids = PIDS_FILE.read_text().splitlines() if PIDS_FILE.exists() else []

    print(f"""
══ Demo pipeline status ══
  Connectors      : {len(connectors)}/19   {list(connectors.keys())[:5]}{'...' if len(connectors) > 5 else ''}
  Flink statements: {len(statements)}/17
  Topics          : {len(topics)}/36
  Consumer pids   : {len(pids)}
  State file      : {STATE_FILE}
""")


# ─── Entry point ─────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", required=True,
                        help="Confluent Cloud env_id from config.yaml's environments: list")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config.yaml"),
                        help="Path to config.yaml (default: project root config.yaml)")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--provision", action="store_true", help="Create all 19 new pipelines")
    grp.add_argument("--teardown",  action="store_true", help="Delete everything created by --provision")
    grp.add_argument("--status",    action="store_true", help="Show provisioning state")
    args = parser.parse_args()

    _load_env_creds(Path(args.config), args.env)

    if args.provision:
        provision()
    elif args.teardown:
        teardown()
    else:
        status()


if __name__ == "__main__":
    main()

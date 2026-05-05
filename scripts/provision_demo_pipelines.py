#!/usr/bin/env python3
"""Provision / teardown OpenLineage demo pipelines in Confluent Cloud.

Generates `--num-pipelines N` independent pipelines, each with a random length
in [2, --max-nodes M]. A "node" is a job (Datagen connector | Flink statement
| consumer group); topics are the edges between them. Random length and random
end-stage choice (Flink vs consumer-group) yield varied shapes that resemble
a real customer environment in Marquez.

Each pipeline shape:

    Datagen → topic₀ → [Flink → topic₁ → Flink → topic₂ → ...] → consumer-group?
    └node 1┘            └────── flink_stages = N - 1 - (1 if consumer else 0) ──┘

All Flink stages use SELECT * (schema-preserving) so chain length doesn't
require per-stage schema registration — the first Flink output topic gets
an explicit schema; downstream topics inherit via SELECT *.

Usage:
    python scripts/provision_demo_pipelines.py --env <env_id> --provision \\
           --num-pipelines 10 --max-nodes 6
    python scripts/provision_demo_pipelines.py --env <env_id> --teardown
    python scripts/provision_demo_pipelines.py --env <env_id> --status

All Confluent Cloud creds are read from config.yaml's `environments:` list
under the matching env_id. Populate config.yaml via the setup wizard first.

Requirements:
    pip install confluent-kafka   (for consumer groups)
"""

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from openlineage_confluent.config import AppConfig  # noqa: E402

# ─── Per-env config (populated in main() from config.yaml) ───────────────────
ENV_ID:          str = ""
CLUSTER_ID:      str = ""
FLINK_POOL:      str = ""
FLINK_CLOUD:     str = ""    # parsed from kafka_bootstrap, e.g. "aws"
FLINK_REGION:    str = ""    # parsed from kafka_bootstrap, e.g. "us-east-2"
SR_ENDPOINT:     str = ""
SR_KEY:          str = ""
SR_SECRET:       str = ""
KAFKA_BOOTSTRAP: str = ""
KAFKA_API_KEY:   str = ""
KAFKA_API_SECRET: str = ""

# Per-env state file paths (set in main() so multi-env doesn't clobber)
STATE_FILE: Path = SCRIPT_DIR / "demo_pipelines_state.json"
PIDS_FILE:  Path = SCRIPT_DIR / "demo_pipelines_consumers.pids"
WORKER_SCRIPT = SCRIPT_DIR / "demo_pipelines_consumer_worker.py"


def _parse_cloud_region(bootstrap: str) -> tuple[str, str]:
    """Extract (cloud, region) from a Confluent Cloud Kafka bootstrap string.

    Bootstrap format: pkc-XXXXXX.<region>.<cloud>.confluent.cloud:<port>
    e.g.  pkc-921jm.us-east-2.aws.confluent.cloud:9092 → ("aws", "us-east-2")

    Returns ("", "") if the bootstrap doesn't match the expected shape, in
    which case Flink CLI calls will fall back to whatever region the user's
    `confluent` context has selected (or fail with the original error).
    """
    host = bootstrap.split(":", 1)[0]   # strip port
    parts = host.split(".")
    # Expect at least: pkc-XXX, region, cloud, confluent, cloud
    if len(parts) >= 5 and parts[-2] == "confluent" and parts[-1] == "cloud":
        return parts[-3], parts[-4]
    return "", ""


def _load_env_creds(config_path: Path, env_id: str) -> None:
    global ENV_ID, CLUSTER_ID, FLINK_POOL, FLINK_CLOUD, FLINK_REGION
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
    FLINK_CLOUD, FLINK_REGION = _parse_cloud_region(KAFKA_BOOTSTRAP)
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

    # Per-env state files keep parallel envs from clobbering each other. Filename
    # kept stable across script renames so older provisioned envs can still
    # be torn down via this script.
    STATE_FILE = SCRIPT_DIR / f"demo_20_state_{env_id}.json"
    PIDS_FILE  = SCRIPT_DIR / f"demo_20_consumers_{env_id}.pids"


# ─── Pipeline generator ─────────────────────────────────────────────────────

# Business-flavoured prefixes; suffixed with the pipeline index so duplicates
# don't collide on Kafka topic names. Picked to look plausible in Marquez —
# mixing transactional, analytical, and ops domains.
DOMAIN_PREFIXES = (
    "orders",   "payments",  "shipments", "inventory", "users",
    "sessions", "clicks",    "products",  "reviews",   "returns",
    "fraud",    "risk",      "billing",   "audit",     "metrics",
    "events",   "logs",      "telemetry", "alerts",    "notifications",
)

# First Flink stage projects + enriches; downstream stages identity-transform
# so the topic schema is preserved without registering one per stage.
ENRICH_SQL_TEMPLATE = """\
INSERT INTO `{out}`
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
FROM `{in_}`;
"""

# Identity transforms used for stages 2+ — schema-preserving SELECT * with
# rotating WHERE clauses so each stage is at least syntactically distinct.
IDENTITY_SQL_VARIANTS = (
    "INSERT INTO `{out}` SELECT * FROM `{in_}`;",
    "INSERT INTO `{out}` SELECT * FROM `{in_}` WHERE orderunits >= 0;",
    "INSERT INTO `{out}` SELECT * FROM `{in_}` WHERE risk_tier <> 'NONE';",
)

# Stage-name suffixes for visual variety in Marquez (purely cosmetic).
STAGE_NAMES = ("enrich", "filter", "project", "transform", "route", "score")


@dataclass
class Pipeline:
    """One independent pipeline: connector → topics → flink stages → consumer?"""
    domain: str
    flink_stages: int       # 0 = Datagen-only (or Datagen + consumer)
    ends_with_consumer: bool

    # Filled in during generation; teardown reads them back from state.
    topics: list[str] = field(default_factory=list)
    flink_names: list[str] = field(default_factory=list)
    consumer_group: str | None = None

    @property
    def total_nodes(self) -> int:
        """Boxes in the CC stream-lineage diagram: connector + topic +
        (Flink + topic)*N + (consumer if any). 3, 5, 7, … for end-to-end
        connected pipelines; 2, 4, 6, … for pipelines that dangle on the
        last topic (only happens in tests / legacy callers — production
        always sets ends_with_consumer=True)."""
        return 2 + 2 * self.flink_stages + (1 if self.ends_with_consumer else 0)


def generate_pipelines(num_pipelines: int, max_nodes: int, *, seed: int | None = None) -> list[Pipeline]:
    """Build `num_pipelines` Pipeline definitions, all fully connected end-to-end.

    A "node" is a box you'd see in Confluent Cloud's stream lineage diagram —
    either a job (connector / Flink statement) OR a topic OR an end consumer.
    Every pipeline starts with a Datagen connector and ends with a consumer
    group, so even the smallest pipeline has 3 boxes:

        connector → topic → consumer-group         (3 nodes, 0 Flink stages)
        connector → topic → Flink → topic → cg     (5 nodes, 1 Flink stage)
        connector → topic → Flink → topic → Flink → topic → cg  (7, 2 stages)
        …                                                       (3 + 2N nodes)

    So pipelines always have an odd node count. `max_nodes` caps the diagram
    width; we round it down to the largest fitting odd value and then pick
    a random length in [3, max_odd_nodes].

    `seed` makes the generator deterministic — used by tests.
    """
    rng = random.Random(seed)
    max_nodes = max(3, max_nodes)
    max_flink_stages = max(0, (max_nodes - 3) // 2)
    out: list[Pipeline] = []
    for i in range(num_pipelines):
        prefix = DOMAIN_PREFIXES[i % len(DOMAIN_PREFIXES)]
        domain = f"{prefix}{i:02d}"
        flink_stages = rng.randint(0, max_flink_stages)
        ends_with_consumer = True   # always — required for end-to-end connectivity

        pipe = Pipeline(domain=domain, flink_stages=flink_stages,
                        ends_with_consumer=ends_with_consumer)
        # Topic 0 is Datagen output; one more topic per Flink stage.
        pipe.topics = [f"ol-{domain}-t0"] + [
            f"ol-{domain}-t{k+1}" for k in range(flink_stages)
        ]
        pipe.flink_names = [
            f"ol-{domain}-{STAGE_NAMES[k % len(STAGE_NAMES)]}-{k}"
            for k in range(flink_stages)
        ]
        if ends_with_consumer:
            pipe.consumer_group = f"ol-{domain}-cg"
        out.append(pipe)
    return out


def _flink_sql(stage_idx: int, in_topic: str, out_topic: str, rng: random.Random) -> str:
    """First stage = enrich; later stages = identity (rotating SQL variants)."""
    if stage_idx == 0:
        return ENRICH_SQL_TEMPLATE.format(in_=in_topic, out=out_topic)
    template = rng.choice(IDENTITY_SQL_VARIANTS)
    return template.format(in_=in_topic, out=out_topic)


# ─── Schemas pre-registered for each topic in the chain ─────────────────────
# Datagen output topics get RAW_ORDERS_SCHEMA (matches the ORDERS quickstart
# Datagen produces). Flink output topics get ENRICHED_SCHEMA (output of the
# first enrich stage; later stages preserve schema via SELECT *). Pre-registering
# means every topic shows up in CC's stream lineage with a schema attached
# immediately, instead of waiting on Datagen's first produce to register.
RAW_ORDERS_SCHEMA = json.dumps({
    "type": "record",
    "name": "Order",
    "namespace": "ksql",
    "fields": [
        {"name": "ordertime",  "type": "long"},
        {"name": "orderid",    "type": "int"},
        {"name": "itemid",     "type": "string"},
        {"name": "orderunits", "type": "double"},
        {"name": "address",    "type": {
            "type": "record", "name": "address",
            "fields": [
                {"name": "city",    "type": "string"},
                {"name": "state",   "type": "string"},
                {"name": "zipcode", "type": "long"},
            ],
        }},
    ],
})

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


def sr_register(topic: str, schema: str) -> bool:
    """Register `schema` as the AVRO value schema for `topic`. Skips silently
    if SR isn't configured for this env (no endpoint/key in config.yaml)."""
    if not (SR_ENDPOINT and SR_KEY and SR_SECRET):
        return False
    subject = f"{topic}-value"
    url = f"{SR_ENDPOINT}/subjects/{subject}/versions"
    try:
        r = httpx.post(url, json={"schema": schema, "schemaType": "AVRO"},
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


def create_connector(domain: str, output_topic: str) -> str | None:
    name = f"ol-{domain}-datagen"
    config = {
        "connector.class": "DatagenSource",
        "name": name,
        "kafka.api.key": KAFKA_API_KEY,
        "kafka.api.secret": KAFKA_API_SECRET,
        "kafka.topic": output_topic,
        "output.data.format": "AVRO",
        "quickstart": "ORDERS",
        "max.interval": "1000",
        "tasks.max": "1",
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"name": name, "config": config}, f)
        tmp = f.name
    try:
        r = run(["confluent", "connect", "cluster", "create",
                 "--config-file", tmp,
                 "--cluster", CLUSTER_ID, "--environment", ENV_ID, "-o", "json"])
        os.unlink(tmp)
        if ok(r):
            conn_id = json.loads(r.stdout).get("id", "")
            print(f"  connector ✓ {name} ({conn_id})")
            return conn_id
        print(f"  connector ✗ {name}: {r.stderr.strip()[:120]}")
        return None
    except Exception as e:
        print(f"  connector exception {domain}: {e}")
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return None


def create_flink_statement(name: str, sql: str) -> bool:
    try:
        r = run(["confluent", "flink", "statement", "create", name,
                 "--sql", sql,
                 "--compute-pool", FLINK_POOL, "--environment", ENV_ID])
        if ok(r):
            print(f"  flink ✓ {name}")
            return True
        print(f"  flink ✗ {name}: {r.stderr.strip()[:120]}")
        return False
    except Exception as e:
        print(f"  flink exception {name}: {e}")
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


def start_consumers(group_topic_pairs: list[tuple[str, str]]) -> list[int]:
    if not group_topic_pairs:
        return []
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
    for group, topic in group_topic_pairs:
        proc = subprocess.Popen(
            [sys.executable, str(WORKER_SCRIPT), topic, group],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        pids.append(proc.pid)
        print(f"  consumer ✓ {group} → {topic} (pid={proc.pid})")
    return pids


# ─── Provision ───────────────────────────────────────────────────────────────

def provision(num_pipelines: int, max_nodes: int, seed: int | None) -> None:
    pipelines = generate_pipelines(num_pipelines, max_nodes, seed=seed)
    rng = random.Random(seed)

    summary_line = (
        f"Generated {len(pipelines)} pipeline(s) with random length in [2, {max_nodes}] — "
        f"total nodes={sum(p.total_nodes for p in pipelines)} "
        f"(connectors={len(pipelines)}, "
        f"flink={sum(p.flink_stages for p in pipelines)}, "
        f"consumers={sum(1 for p in pipelines if p.ends_with_consumer)})"
    )
    print(f"\n══ {summary_line} ══")

    state = load_state()
    topics: list = state.setdefault("topics", [])
    connectors: dict = state.setdefault("connectors", {})
    statements: list = state.setdefault("flink_statements", [])

    print("\n══ Step 1/6 — Create Kafka topics ══")
    for p in pipelines:
        for t in p.topics:
            create_topic(t, topics)
    save_state(state)

    print("\n══ Step 2/6 — Register schemas for every topic in each chain ══")
    if not (SR_ENDPOINT and SR_KEY and SR_SECRET):
        print("  (skipped — env has no Schema Registry credentials in config.yaml)")
    else:
        for p in pipelines:
            # Datagen output topic — RAW_ORDERS_SCHEMA matches the ORDERS quickstart
            sr_register(p.topics[0], RAW_ORDERS_SCHEMA)
            # First Flink output (if any) gets ENRICHED_SCHEMA; later Flink
            # stages SELECT * so the schema propagates without re-registration,
            # but we still pre-register to make every topic show up with a
            # schema in CC's stream lineage from the moment it's created.
            for k, topic in enumerate(p.topics[1:], start=1):
                sr_register(topic, ENRICHED_SCHEMA)

    print("\n══ Step 3/6 — Create Datagen connectors (one per pipeline) ══")
    for p in pipelines:
        if p.domain not in connectors:
            conn_id = create_connector(p.domain, p.topics[0])
            if conn_id:
                connectors[p.domain] = conn_id
    save_state(state)

    print("\n══ Step 4/6 — Waiting 90s for Datagen to register raw AVRO schemas ══")
    for remaining in range(90, 0, -15):
        print(f"  {remaining}s ...", flush=True)
        time.sleep(15)

    print("\n══ Step 5/6 — Create chained Flink statements ══")
    for p in pipelines:
        for k, name in enumerate(p.flink_names):
            if name in statements:
                continue
            in_topic  = p.topics[k]
            out_topic = p.topics[k + 1]
            sql = _flink_sql(k, in_topic, out_topic, rng)
            if create_flink_statement(name, sql):
                statements.append(name)
    save_state(state)

    print("\n══ Step 6/6 — Start consumer groups ══")
    pairs = [
        (p.consumer_group, p.topics[-1])
        for p in pipelines
        if p.consumer_group is not None
    ]
    pids = start_consumers(pairs)
    if pids:
        PIDS_FILE.write_text("\n".join(str(p) for p in pids))

    print(f"""
══ Provisioning complete ══
  Pipelines       : {len(pipelines)}
  Topics          : {len(topics)}
  Connectors      : {len(connectors)}/{len(pipelines)}
  Flink statements: {len(statements)}/{sum(p.flink_stages for p in pipelines)}
  Consumer pids   : {len(pids)}/{sum(1 for p in pipelines if p.ends_with_consumer)}

The bridge will pick these up on its next poll cycle (≤60s) — no restart needed.
""")


# ─── Teardown ────────────────────────────────────────────────────────────────

# All demo resources (topics, connectors, Flink statements, SR subjects) are
# named with this prefix. The Phase 2 stateless sweep filters by it so only
# demo artefacts are touched — never user-owned resources in the same env.
DEMO_NAME_PREFIX = "ol-"


def _is_demo_name(name: str) -> bool:
    """True if `name` is a resource created by this provisioner."""
    return name.startswith(DEMO_NAME_PREFIX)


def _flink_region_args() -> list[str]:
    """Cloud/region flags required by `confluent flink statement` commands.

    Returns ["--cloud", FLINK_CLOUD, "--region", FLINK_REGION] when both are
    known, else []. Without these flags the CLI errors with "no cloud provider
    and region selected" unless the user has set a context with `confluent
    flink region use`.
    """
    if FLINK_CLOUD and FLINK_REGION:
        return ["--cloud", FLINK_CLOUD, "--region", FLINK_REGION]
    return []


def _list_env_flink_statements() -> list[str]:
    """Return all Flink statement names in the current env (or [] on error)."""
    r = subprocess.run(
        ["confluent", "flink", "statement", "list",
         "--environment", ENV_ID, *_flink_region_args(), "-o", "json"],
        capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0:
        print(f"  ⚠ flink statement list failed: {r.stderr.strip()[:120]}")
        return []
    try:
        return [s.get("name", "") for s in json.loads(r.stdout) if s.get("name")]
    except (json.JSONDecodeError, TypeError):
        return []


def _list_env_connectors() -> list[tuple[str, str]]:
    """Return [(name, id), ...] for all connectors in the cluster."""
    r = subprocess.run(
        ["confluent", "connect", "cluster", "list",
         "--cluster", CLUSTER_ID, "--environment", ENV_ID, "-o", "json"],
        capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0:
        print(f"  ⚠ connect cluster list failed: {r.stderr.strip()[:120]}")
        return []
    try:
        return [
            (c.get("name", ""), c.get("id", ""))
            for c in json.loads(r.stdout) if c.get("name")
        ]
    except (json.JSONDecodeError, TypeError):
        return []


def _list_env_topics() -> list[str]:
    """Return all Kafka topic names in the cluster."""
    r = subprocess.run(
        ["confluent", "kafka", "topic", "list",
         "--cluster", CLUSTER_ID, "--environment", ENV_ID, "-o", "json"],
        capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0:
        print(f"  ⚠ kafka topic list failed: {r.stderr.strip()[:120]}")
        return []
    try:
        return [t.get("name", "") for t in json.loads(r.stdout) if t.get("name")]
    except (json.JSONDecodeError, TypeError):
        return []


def _list_sr_subjects() -> list[str]:
    """Return all Schema Registry subjects (or [] if SR isn't configured)."""
    if not SR_ENDPOINT:
        return []
    try:
        r = httpx.get(f"{SR_ENDPOINT}/subjects",
                      auth=(SR_KEY, SR_SECRET), timeout=30)
        if r.status_code != 200:
            print(f"  ⚠ SR /subjects fetch failed: {r.status_code}")
            return []
        body = r.json()
        return body if isinstance(body, list) else []
    except Exception as e:
        print(f"  ⚠ SR /subjects error: {e}")
        return []


def teardown() -> None:
    """Delete every demo-pipeline resource in this env.

    Two phases:
      1. State-tracked cleanup — precise, uses the per-env state file.
      2. Stateless sweep — lists every resource in the env and deletes
         anything matching the ol-* naming convention. This is what makes
         "start from scratch" work even when the state file is missing,
         out of sync, or was created on another machine.
    """
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

    # ── Phase 1: state-tracked cleanup ──────────────────────────────────────
    print("\n══ Phase 1/2 — State-tracked cleanup ══")

    print("\n  Flink statements:")
    for stmt in state.get("flink_statements", []):
        r = run(["confluent", "flink", "statement", "delete", stmt,
                 "--environment", ENV_ID, *_flink_region_args(), "--force"])
        print(f"    {'✓' if ok(r) else '✗'} {stmt}")

    print("\n  Connectors:")
    for domain, conn_id in state.get("connectors", {}).items():
        r = run(["confluent", "connect", "cluster", "delete", conn_id,
                 "--cluster", CLUSTER_ID, "--environment", ENV_ID, "--force"])
        print(f"    {'✓' if ok(r) else '✗'} ol-{domain}-datagen ({conn_id})")

    print("\n  Topics:")
    for topic in state.get("topics", []):
        r = run(["confluent", "kafka", "topic", "delete", topic,
                 "--cluster", CLUSTER_ID, "--environment", ENV_ID])
        print(f"    {'✓' if ok(r) else '✗'} {topic}")

    # ── Phase 2: stateless sweep (catches orphans) ──────────────────────────
    print("\n══ Phase 2/2 — Stateless sweep (purging any remaining ol-* resources) ══")

    print("\n  Orphaned Flink statements:")
    swept = 0
    for stmt in _list_env_flink_statements():
        if _is_demo_name(stmt):
            r = run(["confluent", "flink", "statement", "delete", stmt,
                     "--environment", ENV_ID, *_flink_region_args(), "--force"])
            print(f"    {'✓' if ok(r) else '✗'} {stmt}")
            swept += 1
    if swept == 0:
        print("    (none)")

    print("\n  Orphaned connectors:")
    swept = 0
    for name, conn_id in _list_env_connectors():
        if _is_demo_name(name):
            r = run(["confluent", "connect", "cluster", "delete", conn_id,
                     "--cluster", CLUSTER_ID, "--environment", ENV_ID, "--force"])
            print(f"    {'✓' if ok(r) else '✗'} {name} ({conn_id})")
            swept += 1
    if swept == 0:
        print("    (none)")

    print("\n  Orphaned topics (ol-*):")
    swept = 0
    for topic in _list_env_topics():
        if _is_demo_name(topic):
            r = run(["confluent", "kafka", "topic", "delete", topic,
                     "--cluster", CLUSTER_ID, "--environment", ENV_ID, "--force"])
            print(f"    {'✓' if ok(r) else '✗'} {topic}")
            swept += 1
    if swept == 0:
        print("    (none)")

    print("\n  Orphaned Schema Registry subjects:")
    swept = 0
    for subject in _list_sr_subjects():
        if _is_demo_name(subject):
            try:
                r = httpx.delete(f"{SR_ENDPOINT}/subjects/{subject}",
                                 auth=(SR_KEY, SR_SECRET), timeout=30)
                print(f"    {'✓' if r.status_code in (200, 404) else '✗'} {subject}")
            except Exception as e:
                print(f"    ✗ {subject}: {e}")
            swept += 1
    if swept == 0:
        print("    (none)")

    # ── Phase 3: nuke remaining non-system topics ───────────────────────────
    # Connect-spawned topics (dlq-lcc-*, error-lcc-*, success-lcc-*) and any
    # other user-created topics outside the ol-* convention are caught here.
    # The CLI's `kafka topic list` already omits system topics (`_*`,
    # `__consumer_offsets`, `__transaction_state`, `_confluent-*`, `_schemas`),
    # so listing + deleting everything visible is safe.
    print("\n══ Phase 3/3 — Nuking ALL remaining non-system topics ══")
    print("\n  Topics:")
    swept = 0
    for topic in _list_env_topics():
        r = run(["confluent", "kafka", "topic", "delete", topic,
                 "--cluster", CLUSTER_ID, "--environment", ENV_ID, "--force"])
        print(f"    {'✓' if ok(r) else '✗'} {topic}")
        swept += 1
    if swept == 0:
        print("    (none)")

    STATE_FILE.unlink(missing_ok=True)
    print("\n══ Teardown complete — env is clean ══")


# ─── Status ──────────────────────────────────────────────────────────────────

def status() -> None:
    state = load_state()
    connectors = state.get("connectors", {})
    statements = state.get("flink_statements", [])
    topics = state.get("topics", [])
    pids = PIDS_FILE.read_text().splitlines() if PIDS_FILE.exists() else []

    print(f"""
══ Demo pipeline status ══
  Connectors      : {len(connectors)}   {list(connectors.keys())[:5]}{'...' if len(connectors) > 5 else ''}
  Flink statements: {len(statements)}
  Topics          : {len(topics)}
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
    parser.add_argument("--num-pipelines", type=int, default=10,
                        help="Number of independent pipelines to generate (default: 10)")
    parser.add_argument("--max-nodes", type=int, default=4,
                        help="Maximum nodes per pipeline (jobs only — Datagen + Flinks + consumer). "
                             "Actual length is randomised in [2, max-nodes]. Default: 4.")
    parser.add_argument("--seed", type=int, default=None,
                        help="Seed for the random pipeline generator (omit for fresh randomness).")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--provision", action="store_true", help="Create the generated pipelines")
    grp.add_argument("--teardown",  action="store_true", help="Delete everything previously provisioned")
    grp.add_argument("--status",    action="store_true", help="Show provisioning state")
    args = parser.parse_args()

    _load_env_creds(Path(args.config), args.env)

    if args.provision:
        provision(args.num_pipelines, args.max_nodes, args.seed)
    elif args.teardown:
        teardown()
    else:
        status()


if __name__ == "__main__":
    main()

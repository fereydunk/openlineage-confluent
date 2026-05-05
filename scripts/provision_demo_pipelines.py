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
SR_REGION:       str = ""    # parsed from SR endpoint host, e.g. "us-east-2"
KAFKA_BOOTSTRAP: str = ""
KAFKA_API_KEY:   str = ""
KAFKA_API_SECRET: str = ""

# Single schema serialization format used everywhere — keep producer payload,
# pre-registered topic schemas, and Flink's deserializer all on the same page.
# Changing this requires updating OrderProducer.java's serializer too.
SCHEMA_TYPE: str = "AVRO"


def _region_label() -> str:
    """Short '<cloud>/<region>' tag used in component-creation log lines so
    every ✓ shows where the resource lives. Falls back to '?' if either is
    unknown (only happens before _load_env_creds runs)."""
    return f"{FLINK_CLOUD or '?'}/{FLINK_REGION or '?'}"

# Per-env state file paths (set in main() so multi-env doesn't clobber)
STATE_FILE: Path = SCRIPT_DIR / "demo_pipelines_state.json"
PIDS_FILE:  Path = SCRIPT_DIR / "demo_pipelines_consumers.pids"
WORKER_SCRIPT = SCRIPT_DIR / "demo_pipelines_consumer_worker.py"


# Shared parser — same fn used by the bridge and the wizard. Lives in the
# project's `confluent.topology` module to avoid 3-way duplication.
from openlineage_confluent.confluent.topology import parse_cloud_region as _parse_cloud_region  # noqa: E402


def _load_env_creds(config_path: Path, env_id: str) -> None:
    global ENV_ID, CLUSTER_ID, FLINK_POOL, FLINK_CLOUD, FLINK_REGION
    global SR_ENDPOINT, SR_KEY, SR_SECRET, SR_REGION
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
        # SR endpoint host has the same shape as Kafka bootstrap:
        # psrc-XXXXXX.<region>.<cloud>.confluent.cloud — reuse the parser.
        sr_host = SR_ENDPOINT.replace("https://", "").replace("http://", "")
        _, SR_REGION = _parse_cloud_region(sr_host + ":443")

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

    # Same-region check across every component. Catches the historical
    # misalignment bug where the wizard created a Flink pool in us-west-2
    # against a us-east-2 Kafka cluster — Flink couldn't read the cluster's
    # topics. Now also catches Schema Registry in a different region from
    # the cluster (would force cross-region SR lookups on every produce).
    print("\n══ Region alignment check ══")
    print(f"  Kafka cluster ({CLUSTER_ID})  : {FLINK_CLOUD or '?'}/{FLINK_REGION or '?'}")
    if SR_ENDPOINT:
        same = SR_REGION == FLINK_REGION and SR_REGION
        marker = "✓" if same else "⚠"
        print(f"  Schema Registry           : {FLINK_CLOUD or '?'}/{SR_REGION or '?'}  {marker}")
        if not same and SR_REGION:
            print("  ⚠ SR region differs from cluster region — every produce/fetch crosses regions.")
            print("    Recommend: enable SR in the cluster's region via the Cloud UI.")
    else:
        print("  Schema Registry           : (not configured) ⚠")
    if FLINK_POOL:
        print(f"  Flink compute pool ({FLINK_POOL}): inspecting…")
        # Pool region is queried lazily by _flink_region_args(); call it now
        # to surface mismatches early.
        pool_args = _flink_region_args()
        if len(pool_args) == 4:
            pool_cloud, pool_region = pool_args[1], pool_args[3]
            same_pool = pool_cloud == FLINK_CLOUD and pool_region == FLINK_REGION
            marker = "✓" if same_pool else "⚠"
            print(f"  Flink compute pool        : {pool_cloud}/{pool_region}  {marker}")
            if not same_pool:
                print("  ⚠ Flink pool region differs from cluster — Flink can't read this cluster's topics.")
                print("    Recommend: delete this pool and re-provision so the wizard")
                print("    creates one in the cluster's region.")
    print(f"  Schema serialization      : {SCHEMA_TYPE}")
    print()

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
#
# Why the explicit (col1, col2, ...) target list on every INSERT: when reading
# or writing a Kafka topic, Flink always exposes an implicit `key BYTES`
# column unless you configure `key.format`. Without listing target columns,
# Flink expects 1 + N values and our SELECT only produces N → the statement
# fails SQL validation with "Different number of columns". Listing the
# value columns explicitly tells Flink "leave the key alone".
#
# Per-pipeline single-schema invariant
# ────────────────────────────────────
# Every Flink stage in a pipeline is SCHEMA-PRESERVING — it copies the
# value columns through unchanged so every topic in the chain has the
# IDENTICAL schema (matches the source: Datagen's ORDERS quickstart, or
# the Java OrderProducer's Avro record built from the same shape).
# Optional WHERE clauses give each stage syntactic variety without
# changing the output schema.
#
# We previously had a separate ENRICH_SQL that flattened address.city into
# city and added a risk_tier column — that produced a different schema
# downstream and broke the user's "one schema per pipeline" expectation.
# Removed.
_ORDERS_COLS = "ordertime, orderid, itemid, orderunits, address"
IDENTITY_SQL_VARIANTS = (
    f"INSERT INTO `{{out}}` ({_ORDERS_COLS}) SELECT {_ORDERS_COLS} FROM `{{in_}}`;",
    f"INSERT INTO `{{out}}` ({_ORDERS_COLS}) SELECT {_ORDERS_COLS} "
    f"FROM `{{in_}}` WHERE orderunits >= 0;",
    f"INSERT INTO `{{out}}` ({_ORDERS_COLS}) SELECT {_ORDERS_COLS} "
    f"FROM `{{in_}}` WHERE orderunits < 1000000;",
)

# Stage-name suffixes for visual variety in Marquez (purely cosmetic).
STAGE_NAMES = ("enrich", "filter", "project", "transform", "route", "score")


@dataclass
class Pipeline:
    """One independent pipeline: producer-or-connector → topics → flink → consumer."""
    domain: str
    flink_stages: int       # 0 = no Flink stages
    ends_with_consumer: bool
    uses_native_producer: bool = False    # True → Java producer at head, False → Datagen connector

    # Filled in during generation; teardown reads them back from state.
    topics: list[str] = field(default_factory=list)
    flink_names: list[str] = field(default_factory=list)
    consumer_group: str | None = None

    @property
    def total_nodes(self) -> int:
        """Boxes in the CC stream-lineage diagram: head (connector OR producer)
        + topic + (Flink + topic)*N + (consumer if any). 3, 5, 7, … for end-to-end
        connected pipelines; 2, 4, 6, … for pipelines that dangle on the last
        topic (only happens in tests / legacy callers — production always sets
        ends_with_consumer=True). Native producer counts as 1 head node, same
        as the Datagen connector."""
        return 2 + 2 * self.flink_stages + (1 if self.ends_with_consumer else 0)


def generate_pipelines(num_pipelines: int, min_nodes: int, max_nodes: int,
                       *, seed: int | None = None) -> list[Pipeline]:
    """Build `num_pipelines` Pipeline definitions, all fully connected end-to-end.

    A "node" is a box you'd see in Confluent Cloud's stream lineage diagram —
    either a job (connector / Flink statement) OR a topic OR an end consumer.
    Every pipeline starts with a Datagen connector and ends with a consumer
    group, so even the smallest pipeline has 3 boxes:

        connector → topic → consumer-group         (3 nodes, 0 Flink stages)
        connector → topic → Flink → topic → cg     (5 nodes, 1 Flink stage)
        connector → topic → Flink → topic → Flink → topic → cg  (7, 2 stages)
        …                                                       (3 + 2N nodes)

    Pipelines always have an odd node count. min_nodes/max_nodes are clamped
    to >=3 and rounded down to the nearest odd value; the per-pipeline length
    is then a uniform pick from {min, min+2, …, max}. Set min == max for
    exact-N pipelines.

    `seed` makes the generator deterministic — used by tests.
    """
    rng = random.Random(seed)
    min_nodes = max(3, min_nodes)
    max_nodes = max(min_nodes, max_nodes)
    min_flink_stages = (min_nodes - 3) // 2
    max_flink_stages = (max_nodes - 3) // 2
    out: list[Pipeline] = []
    for i in range(num_pipelines):
        prefix = DOMAIN_PREFIXES[i % len(DOMAIN_PREFIXES)]
        domain = f"{prefix}{i:02d}"
        flink_stages = rng.randint(min_flink_stages, max_flink_stages)
        ends_with_consumer = True   # always — required for end-to-end connectivity
        # ~50/50 split: half the pipelines use a native Java Kafka producer
        # at the head (exercises the bridge's Metrics API producer source),
        # the other half use a Datagen managed connector (exercises the
        # Connect API source). Variety in CC stream lineage out of the box.
        uses_native_producer = rng.random() < 0.5

        pipe = Pipeline(domain=domain, flink_stages=flink_stages,
                        ends_with_consumer=ends_with_consumer,
                        uses_native_producer=uses_native_producer)
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
    """Pick a schema-preserving identity SQL for this stage.

    Every stage uses the same value-column list (RAW_ORDERS_SCHEMA shape) so
    every topic in the pipeline ends up with the IDENTICAL schema. Optional
    WHERE clauses give stages syntactic variety without changing the schema.
    """
    template = rng.choice(IDENTITY_SQL_VARIANTS)
    return template.format(in_=in_topic, out=out_topic)


# ─── Single canonical schema for every topic in every pipeline ──────────────
# This MUST match exactly what the Datagen connector's ORDERS quickstart
# registers — Datagen registers its own value schema for the head topic on
# first produce, so any pre-registered subject must be byte-compatible or SR
# rejects with "incompatible with an earlier schema". The two critical bits:
#   - record name MUST be "orders" (lowercase). Datagen uses lowercase plural.
#     A previous version used "Order" (capitalized singular) which produced
#     identical fields but a different fully-qualified name → SR rejects.
#   - namespace MUST be "ksql". That's what the upstream
#     kafka-connect-datagen/src/main/resources/orders_schema.avro declares.
# Since every Flink stage is schema-preserving (see IDENTITY_SQL_VARIANTS
# above) every topic in the pipeline ends up with this same schema —
# Datagen-headed and native-producer-headed alike. The Java OrderProducer's
# embedded VALUE_SCHEMA_JSON mirrors this exactly; keep them in sync.
RAW_ORDERS_SCHEMA = json.dumps({
    "type": "record",
    "name": "orders",
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
            print(f"  SR ✓ {subject} ({SCHEMA_TYPE}, id={r.json().get('id')})")
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
        print(f"  topic ✓ {topic} ({_region_label()})")
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
            print(f"  connector ✓ {name} ({_region_label()}, {SCHEMA_TYPE}, {conn_id})")
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
    """Create + wait for a Flink statement to reach RUNNING.

    Two non-obvious flags:
      --database CLUSTER_ID  Tells Flink which Kafka cluster's topics to
        resolve unqualified table names against. Without it, the statement
        is created but immediately FAILS SQL validation with "Table … does
        not exist" because the default database is empty.
      --wait                 Block until the statement is RUNNING (success)
        or FAILED (real error). Without --wait, the create call returns
        rc=0 the moment the statement is queued — even if it later fails —
        and we'd report ✓ for broken statements.
    """
    try:
        r = run(["confluent", "flink", "statement", "create", name,
                 "--sql", sql,
                 "--compute-pool", FLINK_POOL,
                 "--database",     CLUSTER_ID,
                 "--environment",  ENV_ID,
                 "--wait"])
        if ok(r):
            print(f"  flink ✓ {name} ({_region_label()}, pool={FLINK_POOL})")
            return True
        # Trim noisy boilerplate; the useful bit is usually after "Caused by"
        err = (r.stderr or r.stdout or "").strip()
        if "Caused by:" in err:
            err = err.split("Caused by:", 1)[1].strip().split("\n", 1)[0]
        print(f"  flink ✗ {name}: {err[:200]}")
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
    log_dir = SCRIPT_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    for group, topic in group_topic_pairs:
        # Redirect stderr to a per-consumer log so silent crashes leave a trace.
        # Append mode so restarts don't clobber prior output.
        log_path = log_dir / f"consumer-{group}.log"
        log_fh = open(log_path, "ab")
        proc = subprocess.Popen(
            [sys.executable, str(WORKER_SCRIPT), topic, group],
            env=env, stdout=log_fh, stderr=subprocess.STDOUT,
        )
        pids.append(proc.pid)
        print(f"  consumer ✓ {group} → {topic} ({_region_label()}, pid={proc.pid}, log={log_path.name})")
    return pids


# ─── Native Java producer ────────────────────────────────────────────────────

JAVA_DEMO_DIR = PROJECT_ROOT / "scripts" / "java_client_demo"
JAVA_DEMO_JAR = JAVA_DEMO_DIR / "target" / "openlineage-java-demo-1.0-SNAPSHOT.jar"
JAVA_HOME_DEFAULT = "/opt/homebrew/opt/openjdk@21"


def _ensure_java_jar() -> bool:
    """Build the Java demo JAR if missing. Returns True on success."""
    if JAVA_DEMO_JAR.exists():
        return True
    print(f"  Java demo JAR not found at {JAVA_DEMO_JAR}; running `make java-demo-build`…")
    java_home = os.environ.get("JAVA_HOME", JAVA_HOME_DEFAULT)
    r = subprocess.run(
        ["make", "java-demo-build"],
        cwd=str(PROJECT_ROOT),
        env={**os.environ, "JAVA_HOME": java_home},
        capture_output=True, text=True, timeout=300,
    )
    if r.returncode != 0 or not JAVA_DEMO_JAR.exists():
        print(f"  ✗ JAR build failed (rc={r.returncode}): {r.stderr.strip()[:300]}")
        print("  Install JDK 21 + Maven, or run manually: make java-demo-build")
        return False
    print(f"  ✓ Built {JAVA_DEMO_JAR.name}")
    return True


def start_native_producers(producer_specs: list[dict]) -> list[int]:
    """Spawn one Java OrderProducer per spec.

    Each spec: {domain, topic, client_id, job_namespace, job_name}.
    Producers run in infinite mode at MESSAGE_INTERVAL_MS=2000 (1 msg/2s) so
    they don't burn API quota; SIGTERM on teardown emits a COMPLETE event
    via the JVM shutdown hook before exiting.
    """
    if not producer_specs:
        return []
    if not _ensure_java_jar():
        print("  ⚠ skipping native Java producers (no JAR)")
        return []

    java_home = os.environ.get("JAVA_HOME", JAVA_HOME_DEFAULT)
    java_bin = f"{java_home}/bin/java"
    if not Path(java_bin).exists():
        java_bin = "java"   # let PATH resolve it; will fail loudly if missing

    if not (SR_ENDPOINT and SR_KEY and SR_SECRET):
        print("  ⚠ skipping native Java producers — env has no Schema Registry "
              "credentials in config.yaml. Producers need SR to write Avro records "
              "compatible with the pre-registered topic schema (otherwise downstream "
              "Flink can't deserialize). Re-provision via the wizard to mint SR keys.")
        return []

    pids: list[int] = []
    log_dir = SCRIPT_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    for spec in producer_specs:
        env = {
            **os.environ,
            "JAVA_HOME":              java_home,
            "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP,
            "KAFKA_API_KEY":          KAFKA_API_KEY,
            "KAFKA_API_SECRET":       KAFKA_API_SECRET,
            # Schema Registry — required by KafkaAvroSerializer so the producer
            # writes records that match the AVRO schema we pre-registered for
            # this topic. Plain string values would fail Flink deserialization.
            "SR_URL":                 SR_ENDPOINT,
            "SR_API_KEY":             SR_KEY,
            "SR_API_SECRET":          SR_SECRET,
            "OL_TOPIC":               spec["topic"],
            "OL_JOB_NAMESPACE":       spec["job_namespace"],
            "OL_JOB_NAME":            spec["job_name"],
            "KAFKA_CLIENT_ID":        spec["client_id"],
            "MESSAGE_COUNT":          "-1",     # infinite
            "MESSAGE_INTERVAL_MS":    "2000",   # 1 msg / 2s
            # Marquez URL — leave default unless overridden
            "MARQUEZ_URL":            os.environ.get("MARQUEZ_URL", "http://localhost:5000"),
        }
        # Redirect stdout+stderr to a per-producer log so silent JVM crashes
        # leave a trace. Append mode preserves prior runs' output.
        log_path = log_dir / f"producer-{spec['client_id']}.log"
        log_fh = open(log_path, "ab")
        proc = subprocess.Popen(
            [java_bin, "-cp", str(JAVA_DEMO_JAR), "demo.OrderProducer"],
            env=env, stdout=log_fh, stderr=subprocess.STDOUT,
        )
        pids.append(proc.pid)
        print(f"  producer ✓ {spec['client_id']} → {spec['topic']} "
              f"({_region_label()}, {SCHEMA_TYPE}, pid={proc.pid}, log={log_path.name})")
    return pids


# ─── Provision ───────────────────────────────────────────────────────────────

def provision(num_pipelines: int, min_nodes: int, max_nodes: int, seed: int | None) -> None:
    pipelines = generate_pipelines(num_pipelines, min_nodes, max_nodes, seed=seed)
    rng = random.Random(seed)

    range_str = (
        f"exactly {min_nodes}" if min_nodes == max_nodes
        else f"random length in [{min_nodes}, {max_nodes}]"
    )
    n_datagen  = sum(1 for p in pipelines if not p.uses_native_producer)
    n_producer = sum(1 for p in pipelines if p.uses_native_producer)
    summary_line = (
        f"Generated {len(pipelines)} pipeline(s), {range_str} nodes each — "
        f"total nodes={sum(p.total_nodes for p in pipelines)} "
        f"(datagen-headed={n_datagen}, native-producer-headed={n_producer}, "
        f"flink={sum(p.flink_stages for p in pipelines)}, "
        f"consumers={sum(1 for p in pipelines if p.ends_with_consumer)})"
    )
    print(f"\n══ {summary_line} ══")

    state = load_state()
    topics: list = state.setdefault("topics", [])
    connectors: dict = state.setdefault("connectors", {})
    statements: list = state.setdefault("flink_statements", [])
    state.setdefault("producer_pids", [])

    # Build each pipeline strictly left-to-right: every component is created
    # only after its upstream dependencies exist. The chain for a Datagen
    # pipeline of N nodes:
    #
    #   topic₀ + schema₀
    #   → Datagen connector → topic₀
    #   → wait 15s for Datagen to register/produce
    #   → topic₁ + schema₁
    #   → Flink₀ (topic₀ → topic₁)
    #   → topic₂ + schema₂
    #   → Flink₁ (topic₁ → topic₂)
    #   → … (repeat for every Flink stage)
    #   → consumer group on the final topic
    #
    # Native-producer pipelines: same chain, but the "head" is a Java client
    # spawned right after topic₀ + schema₀ are in place.
    # Each pipeline is built as a sequence of "data-flow segments". A topic
    # is never the left side of a segment — every topic is the OUTPUT of
    # something on its left:
    #
    #   Segment 1:  producer/connector  →  head topic
    #   Segment 2:  source topic + flink stage  →  output topic    (×N)
    #   Segment 3:  final topic  →  consumer group
    #
    # Underneath, the topic must physically exist before the producer/Flink
    # writes to it (Datagen connectors don't auto-create) — so the create
    # order inside each segment is "create empty topic, register schema,
    # then create producer/Flink" — but the framing in the log shows the
    # producer as the source-of-data on the left.
    consumer_pairs:  list[tuple[str, str]] = []
    producer_specs:  list[dict]            = []
    for i, p in enumerate(pipelines, 1):
        head_kind = "native-producer-headed" if p.uses_native_producer else "Datagen-headed"
        print(f"\n══ Pipeline {i}/{len(pipelines)}: {p.domain} "
              f"({p.total_nodes} nodes, {head_kind}) ══")
        print("  Building data-flow segments left to right:")

        # ── Segment 1: head producer/connector → head topic ────────────────
        head_label   = ("Java producer" if p.uses_native_producer
                        else "Datagen connector")
        head_name    = (f"ol-{p.domain}-producer" if p.uses_native_producer
                        else f"ol-{p.domain}-datagen")
        print(f"\n  ① [{head_label} {head_name}] → <{p.topics[0]}>")
        # Topic must exist before the producer writes — create it first,
        # then attach the producer.
        create_topic(p.topics[0], topics)
        if SR_ENDPOINT and SR_KEY and SR_SECRET:
            # RAW_ORDERS_SCHEMA matches both the Datagen ORDERS quickstart
            # AND the Java OrderProducer's GenericRecord shape.
            sr_register(p.topics[0], RAW_ORDERS_SCHEMA)
        else:
            print("     (SR not configured — skipping schema registration)")
        if p.uses_native_producer:
            # Defer spawning until ALL topics + Flink statements are up — the
            # Java producer would otherwise write into a topic whose downstream
            # Flink hadn't been created yet.
            producer_specs.append({
                "domain":         p.domain,
                "topic":          p.topics[0],
                "client_id":      head_name,
                "job_namespace":  f"kafka-producer://{CLUSTER_ID}",
                "job_name":       head_name,
            })
            print("     producer (deferred — will spawn after all Flink stages are RUNNING)")
        else:
            if p.domain in connectors:
                print(f"     connector ✓ (already exists, id={connectors[p.domain]})")
            else:
                conn_id = create_connector(p.domain, p.topics[0])
                if conn_id:
                    connectors[p.domain] = conn_id
                    save_state(state)
            # Datagen needs a few seconds to register its writer schema with
            # SR and start producing. Pre-registered schema means no
            # auto-register conflict — 15s is enough for the connector to
            # transition to RUNNING and Flink to find data on the topic.
            print("     ⏳ waiting 15s for Datagen to start producing...")
            time.sleep(15)
        save_state(state)

        # ── Segment 2 ×N: input topic → flink stage → output topic ─────────
        for k, name in enumerate(p.flink_names):
            in_topic  = p.topics[k]
            out_topic = p.topics[k + 1]
            print(f"\n  • <{in_topic}> → [flink {name}] → <{out_topic}>")
            # Output topic must exist before Flink writes to it.
            create_topic(out_topic, topics)
            if SR_ENDPOINT and SR_KEY and SR_SECRET:
                # Same schema as the head topic — every Flink stage is
                # schema-preserving (identity SELECT) so every topic in the
                # pipeline shares the IDENTICAL schema.
                sr_register(out_topic, RAW_ORDERS_SCHEMA)
            if name in statements:
                print("     flink ✓ (already exists)")
            else:
                sql = _flink_sql(k, in_topic, out_topic, rng)
                if create_flink_statement(name, sql):
                    statements.append(name)
            save_state(state)

        # ── Segment 3: final topic → tail consumer ─────────────────────────
        if p.consumer_group:
            print(f"\n  ⓩ <{p.topics[-1]}> → [consumer {p.consumer_group}]")
            consumer_pairs.append((p.consumer_group, p.topics[-1]))
            print("     consumer (deferred — will spawn after all stages are up)")

        print(f"\n  ══ Pipeline {p.domain} build complete ══")

    # ── Spawn deferred runtime processes (after every topic+Flink is up) ──
    if producer_specs:
        print("\n══ Starting deferred native Java producers ══")
        producer_pids = start_native_producers(producer_specs)
        if producer_pids:
            state["producer_pids"] = state.get("producer_pids", []) + producer_pids
            save_state(state)
    else:
        producer_pids = []

    if consumer_pairs:
        print("\n══ Starting deferred consumer groups ══")
        pids = start_consumers(consumer_pairs)
        if pids:
            PIDS_FILE.write_text("\n".join(str(p) for p in pids))
    else:
        pids = []

    # ── Per-pipeline chain summary ─────────────────────────────────────────
    # Show every pipeline as a connected directed chain so the user can
    # visually confirm start→end connectivity matches what they expect.
    # Each box is annotated with region + schema type (where applicable),
    # mirroring CC's own stream lineage view but rendered in plain text.
    print("\n══ Pipeline chains ══")
    for p in pipelines:
        head_label = (f"producer {f'ol-{p.domain}-producer'}" if p.uses_native_producer
                      else f"connector {f'ol-{p.domain}-datagen'}")
        head_anno  = f"{_region_label()}, {SCHEMA_TYPE}"
        chain: list[str] = [f"[{head_label}] ({head_anno})"]
        for k, topic in enumerate(p.topics):
            chain.append(f"<{topic}> ({_region_label()}, {SCHEMA_TYPE})")
            if k < len(p.flink_names):
                chain.append(f"[flink {p.flink_names[k]}] ({_region_label()}, pool={FLINK_POOL})")
        if p.ends_with_consumer and p.consumer_group:
            chain.append(f"[consumer {p.consumer_group}] ({_region_label()})")
        print(f"  Pipeline {p.domain} ({p.total_nodes} nodes):")
        for i, node in enumerate(chain):
            arrow = "    " if i == 0 else "  → "
            print(f"  {arrow}{node}")
        print()

    print(f"""══ Provisioning complete ══
  Pipelines           : {len(pipelines)}
  Region              : {_region_label()}
  Schema type         : {SCHEMA_TYPE}
  Topics              : {len(topics)}
  Datagen connectors  : {len(connectors)}/{n_datagen}
  Native producers    : {len(producer_pids)}/{n_producer}
  Flink statements    : {len(statements)}/{sum(p.flink_stages for p in pipelines)}
  Consumer pids       : {len(pids)}/{sum(1 for p in pipelines if p.ends_with_consumer)}

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


_FLINK_POOL_REGION_CACHE: tuple[str, str] | None = None


# When all pools have been deleted, statements in those pools' regions
# become orphans we can't discover from `compute-pool list`. Hardcode the
# common AWS regions Flink is offered in so the sweep still catches them.
# Other clouds (gcp, azure) less commonly used for demos — extend if needed.
_FALLBACK_FLINK_REGIONS: tuple[tuple[str, str], ...] = (
    ("aws", "us-east-1"),
    ("aws", "us-east-2"),
    ("aws", "us-west-1"),
    ("aws", "us-west-2"),
    ("aws", "eu-west-1"),
)


def _all_flink_regions_to_sweep() -> list[tuple[str, str]]:
    """Cloud/region pairs that may contain orphan Flink statements for this env.

    Includes the configured pool's region, the cluster's region, every
    existing pool's region (catches cross-region orphans from misaligned-pool
    history), AND a hardcoded fallback list of common AWS regions (catches
    statements stranded after the pool that hosted them was deleted).
    Sorted + deduped so the sweep visits each region exactly once.
    """
    regions: set[tuple[str, str]] = set()
    # Pool's region (cached after first describe)
    pool_args = _flink_region_args()
    if len(pool_args) == 4:
        regions.add((pool_args[1], pool_args[3]))
    # Cluster's region (parsed from kafka_bootstrap)
    if FLINK_CLOUD and FLINK_REGION:
        regions.add((FLINK_CLOUD, FLINK_REGION))
    # Every existing pool's region
    r = subprocess.run(
        ["confluent", "flink", "compute-pool", "list",
         "--environment", ENV_ID, "-o", "json"],
        capture_output=True, text=True, timeout=15,
    )
    if r.returncode == 0:
        try:
            for pool in json.loads(r.stdout):
                cloud  = (pool.get("cloud")  or "").lower()
                region = (pool.get("region") or "").lower()
                if cloud and region:
                    regions.add((cloud, region))
        except (json.JSONDecodeError, AttributeError):
            pass
    # Fallback list — covers stranded statements when all pools have been deleted
    regions.update(_FALLBACK_FLINK_REGIONS)
    return sorted(regions)


def _list_env_flink_statements_in_region(cloud: str, region: str) -> list[str]:
    """Return all Flink statement names in the given cloud+region, or [] on error."""
    r = subprocess.run(
        ["confluent", "flink", "statement", "list",
         "--environment", ENV_ID, "--cloud", cloud, "--region", region, "-o", "json"],
        capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0:
        return []
    try:
        return [s.get("name", "") for s in json.loads(r.stdout) if s.get("name")]
    except (json.JSONDecodeError, TypeError):
        return []


def _flink_region_args() -> list[str]:
    """Cloud/region flags required by `confluent flink statement` commands.

    The pool's region wins over the cluster's region — they're usually the
    same, but if a misaligned pool exists (e.g. wizard previously created the
    pool in us-west-2 against a us-east-2 cluster), Flink statements live
    where the POOL is, so list/delete must query the pool's region. Falls back
    to the cluster region (parsed from kafka_bootstrap) if the pool describe
    fails — last-resort better than nothing.
    """
    global _FLINK_POOL_REGION_CACHE
    if _FLINK_POOL_REGION_CACHE is None:
        cloud, region = "", ""
        if FLINK_POOL:
            r = subprocess.run(
                ["confluent", "flink", "compute-pool", "describe", FLINK_POOL,
                 "--environment", ENV_ID, "-o", "json"],
                capture_output=True, text=True, timeout=15,
            )
            if r.returncode == 0:
                try:
                    data = json.loads(r.stdout)
                    cloud  = (data.get("cloud")  or "").lower()
                    region = (data.get("region") or "").lower()
                except (json.JSONDecodeError, AttributeError):
                    pass
        if not (cloud and region):
            cloud, region = FLINK_CLOUD, FLINK_REGION    # fall back to cluster region
        _FLINK_POOL_REGION_CACHE = (cloud, region)
    cloud, region = _FLINK_POOL_REGION_CACHE
    if cloud and region:
        return ["--cloud", cloud, "--region", region]
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

    print("\n══ Stopping local client processes ══")
    if PIDS_FILE.exists():
        for line in PIDS_FILE.read_text().splitlines():
            try:
                os.kill(int(line.strip()), 15)
                print(f"  killed consumer pid {line.strip()}")
            except (ProcessLookupError, ValueError, OSError):
                pass
        PIDS_FILE.unlink(missing_ok=True)
    WORKER_SCRIPT.unlink(missing_ok=True)
    # Native Java producers spawned by the provisioner (state["producer_pids"])
    # SIGTERM lets the JVM shutdown hook flush a COMPLETE OL event before exit.
    for pid in state.get("producer_pids", []):
        try:
            os.kill(int(pid), 15)
            print(f"  killed Java producer pid {pid}")
        except (ProcessLookupError, ValueError, OSError):
            pass
    # Catch-all sweep — pkill any orphan workers whose pids we lost track of
    # (state file deleted, machine rebooted between provision and teardown,
    # or workers spawned by an older script version that didn't track pids).
    # Match on full command line so we only hit OUR processes, not unrelated
    # Java/Python apps. macOS + Linux both support `pkill -f`.
    for pattern, label in (
        (str(JAVA_DEMO_JAR),       "orphan Java producer"),
        (str(WORKER_SCRIPT),       "orphan consumer worker (path-matched)"),
        ("demo_pipelines_consumer_worker", "orphan consumer worker (name-matched)"),
        ("demo.OrderProducer",     "orphan Java producer (class-matched)"),
    ):
        r = subprocess.run(["pkill", "-f", pattern], capture_output=True, text=True)
        # pkill exit 0 = killed something, 1 = no match (both fine), 127 = pkill missing
        if r.returncode == 0:
            print(f"  pkilled {label} (pattern={pattern!r})")
        elif r.returncode == 127:
            print("  ⚠ pkill not in PATH — skipping orphan-process sweep")
            break

    # ── Phase 1: state-tracked cleanup ──────────────────────────────────────
    print("\n══ Phase 1/2 — State-tracked cleanup ══")

    print("\n  Flink statements:")
    sweep_regions = _all_flink_regions_to_sweep()
    for stmt in state.get("flink_statements", []):
        # Try every known region — Flink statements live in their pool's region,
        # which may not match the cluster's. Stop on first success.
        deleted = False
        for cloud, region in sweep_regions:
            r = run(["confluent", "flink", "statement", "delete", stmt,
                     "--environment", ENV_ID, "--cloud", cloud, "--region", region, "--force"])
            if ok(r):
                deleted = True
                break
        print(f"    {'✓' if deleted else '✗'} {stmt}")

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

    print("\n  Orphaned Flink statements (across all regions with pools):")
    swept = 0
    for cloud, region in sweep_regions:
        for stmt in _list_env_flink_statements_in_region(cloud, region):
            if _is_demo_name(stmt):
                r = run(["confluent", "flink", "statement", "delete", stmt,
                         "--environment", ENV_ID, "--cloud", cloud, "--region", region, "--force"])
                print(f"    {'✓' if ok(r) else '✗'} {stmt} ({cloud}/{region})")
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
    parser.add_argument("--min-nodes", type=int, default=3,
                        help="Minimum nodes per pipeline (boxes in CC stream lineage: "
                             "connector + topic + Flinks/topics + consumer). Min 3. Default: 3.")
    parser.add_argument("--max-nodes", type=int, default=5,
                        help="Maximum nodes per pipeline. Set min == max for exact-N pipelines. "
                             "Even values round down to the nearest odd. Default: 5.")
    parser.add_argument("--seed", type=int, default=None,
                        help="Seed for the random pipeline generator (omit for fresh randomness).")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--provision", action="store_true", help="Create the generated pipelines")
    grp.add_argument("--teardown",  action="store_true", help="Delete everything previously provisioned")
    grp.add_argument("--status",    action="store_true", help="Show provisioning state")
    args = parser.parse_args()

    _load_env_creds(Path(args.config), args.env)

    if args.provision:
        provision(args.num_pipelines, args.min_nodes, args.max_nodes, args.seed)
    elif args.teardown:
        teardown()
    else:
        status()


if __name__ == "__main__":
    main()

# openlineage-confluent

Bridges **all** Confluent Cloud components into the **[OpenLineage](https://openlineage.io)** standard, making your full Kafka topology visible in observability tools like [Marquez](https://marquezproject.ai), DataHub, and OpenMetadata.

```
Confluent Cloud                              OpenLineage backend
──────────────────────────────               ────────────────────
Managed Connect API    ──┐                   ┌─ Marquez UI  (http://localhost:3000)
Flink CLI              ──┤                   │
Metrics API (consumers)──┤                   │
Metrics API (producers)──┼──▶ ol-confluent ──▶─┤ DataHub
ksqlDB REST API        ──┤                   │
Self-managed Connect   ──┤                   └─ OpenMetadata / ...
Tableflow CLI          ──┘
Schema Registry  ──(enrichment)──┘
Kafka REST API   ──(enrichment)──┘
             (poll every N seconds; hot-reloads config.yaml each cycle)
```

## What it covers

Every poll cycle the bridge fetches from **seven** lineage sources **concurrently** (per env, then merged across envs):

| Source | API | Components covered |
|---|---|---|
| **Managed Connect** | `api.confluent.cloud/connect/v1/…` | Confluent Cloud source and sink connectors |
| **Flink SQL** | `confluent flink statement list -o json` | Cloud Flink persistent statements |
| **Metrics API (consumers)** | `consumer_lag_offsets` grouped by `consumer_group_id` | Any client committing offsets: application consumers, Kafka Streams, self-managed Connect workers |
| **Metrics API (producers)** | `received_bytes` grouped by `client_id` | Per-producer client identity + topics produced — the only broker-side producer signal |
| **ksqlDB REST** | `POST {cluster}/ksql` — `SHOW QUERIES EXTENDED` | ksqlDB persistent queries (CSAS / CTAS) |
| **Self-managed Connect** | `GET {endpoint}/connectors?expand=info,status` | On-prem or customer-hosted Connect clusters |
| **Tableflow** | `confluent tableflow topic list -o json` | Active Tableflow syncs → Iceberg/Glue (closes the lake-house lineage loop) |

Two **optional enrichment sources** add metadata facets to every topic Dataset:

| Source | API | Facet added |
|---|---|---|
| **Schema Registry** | `/subjects/{topic}-key|value/versions/latest` | `SchemaDatasetFacet` — field-level schema (AVRO, JSON Schema, Protobuf) |
| **Kafka REST API** | `/kafka/v3/clusters/{id}/topics` | `KafkaTopicDatasetFacet` — partition count, replication factor, isInternal |

Throughput metrics also fetch from the Metrics API, adding `KafkaTopicThroughputDatasetFacet` (bytes in/out, records in/out) to each topic.

Then each cycle:

1. **Hot-reloads `config.yaml`** — re-reads the file at the top of every cycle and rebuilds the per-env clients only if the env list (or any env's credentials) actually changed. Wizard-added envs are picked up within ≤60s with no Stop/Start.
2. **Parses Flink SQL** — regex parser extracts `FROM`/`JOIN` inputs and `INSERT INTO`/`CREATE TABLE AS` outputs.
3. **Resolves ksqlDB stream names to topics** — `SHOW STREAMS/TABLES EXTENDED` maps stream names to their underlying Kafka topics before building edges.
4. **Filters internal consumer groups + producer client IDs** — `_*` / `confluent_cli_consumer_*` groups and `connector-producer-` / `connector-` / `_confluent-flink_` producers are excluded.
5. **Enriches topic Datasets** — fetches schemas from Schema Registry and partition metadata from Kafka REST (if configured).
6. **Builds a lineage graph** — one `(source → job → target)` edge per data-flow relationship.
7. **Quarantines failed sources** — when a fetch returns 401/5xx/timeout, the corresponding OL job-namespace prefix lands in `LineageGraph.failed_namespaces`. The emitter suppresses removal-detection (no false ABORT events) for those namespaces; recovery is automatic on the next successful poll.
8. **Diffs against the previous cycle** — each job's lineage is fingerprinted (SHA-256); unchanged jobs are skipped.
9. **Detects removals** — jobs absent from the current cycle get a `RunState.ABORT` event (unless their namespace is in `failed_namespaces`).
10. **Emits RunEvents in parallel** — configurable `ThreadPoolExecutor`, one HTTP connection per thread.
11. **Persists state across restarts** — SQLite (WAL mode) stores known jobs and fingerprints.

## Namespace conventions

| Confluent component | OL Job namespace | OL Job name | OL Dataset namespace |
|---|---|---|---|
| Managed connector | `kafka-connect://<env_id>` | `<connector_name>` | `kafka://<bootstrap>` |
| Self-managed connector | `kafka-connect://<cluster_label>` | `<connector_name>` | `kafka://<bootstrap>` |
| Flink statement | `flink://<env_id>` | `<statement_name>` | `kafka://<bootstrap>` |
| Consumer group | `kafka-consumer-group://<cluster_id>` | `<group_id>` | `kafka://<bootstrap>` |
| Kafka producer | `kafka-producer://<cluster_id>` | `<client_id>` | `kafka://<bootstrap>` |
| ksqlDB query | `ksqldb://<ksql_cluster_id>` | `<query_id>` | `kafka://<bootstrap>` |
| Tableflow | `tableflow://<env_id>` | `<topic_name>` | `glue://<region>` |

## Live demo topology (DEVTEST cluster)

The repository includes a **20-domain demo** provisioning script (see [Scripts](#scripts)). The core orders pipeline:

```
[ol-datagen-orders-source]    DatagenSource managed connector
        │
        ▼
  ol-raw-orders                Kafka topic
        │
        ├──▶ [ol-enrich-orders]          Flink: flatten + add risk_tier
        │           │
        │     ol-orders-enriched         Kafka topic
        │     (SchemaDatasetFacet,        ← field-level schema from SR
        │      KafkaTopicDatasetFacet,    ← partitions / replication from Kafka REST
        │      KafkaTopicThroughputFacet) ← bytes/records from Metrics API
        │           │
        │    ┌──────┼──────────────────────────────┐
        │    ▼      ▼                               ▼
        │  [ol-high-value-alerts]  [ol-medium-risk-orders]  [ol-http-sink]
        │        (Flink)                 (Flink)           (HttpSink connector)
        │
        └──▶ [order-processor-service]   Consumer group (application)
        └──▶ [CSAS_HIGH_VALUE_STREAM_0]  ksqlDB persistent query
```

## Prerequisites

- Python 3.11+
- [Confluent CLI](https://docs.confluent.io/confluent-cli/current/install.html) logged in (`confluent login`) — required for Flink statement listing
- A Confluent Cloud environment with at least one of: Connect connectors, Flink statements, consumer groups, or ksqlDB clusters
- Docker or [OrbStack](https://orbstack.dev) — for running Marquez locally

## Quick start

### 1. Install

```bash
git clone https://github.com/fereydunk/openlineage-confluent
cd openlineage-confluent
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Configure

The recommended path is to use the wizard — `./startup.sh` launches it at `http://localhost:8892`. Sign in to Confluent Cloud, multi-select environments, click *Save selection*, then *Start OpenLineage*. The wizard mints all per-env API keys and writes `config.yaml` for you.

If you prefer hand-editing:

```bash
cp config.example.yaml config.yaml
# Edit config.yaml with your credentials
```

Minimum required fields:

| Field | Where to find it |
|---|---|
| `confluent.CONFLUENT_CLOUD_API_KEY` | Confluent Cloud → API Keys → Cloud resource management. **The key's owner principal must have MetricsViewer + CloudClusterAdmin (or env-level Connect access)** — API keys carry their owner's perms; without these you'll get 401 storms in the bridge log. |
| `confluent.environments[].env_id` | Confluent Cloud → Environments |
| `confluent.environments[].cluster_id` | Confluent Cloud → Cluster settings |
| `confluent.environments[].kafka_bootstrap` | Confluent Cloud → Cluster settings → Bootstrap servers |
| `openlineage.OPENLINEAGE_URL` | Marquez backend URL (default `http://localhost:5000`) |

### 3. Start Marquez

```bash
make marquez-up
# Marquez UI  → http://localhost:3000
# Marquez API → http://localhost:5000
```

### 4. Run the bridge

```bash
make validate   # Fetch and print lineage — no events emitted
make run-once   # One fetch → map → emit cycle
make run        # Continuous polling (Ctrl-C to stop)
```

## Consumer group detection demo

To verify that application consumer groups appear in the lineage graph:

```bash
# Install confluent-kafka
pip install confluent-kafka

# Provide a Kafka cluster-scoped API key
# Create under: Confluent Cloud → Cluster → API Keys → Kafka cluster scoped
export KAFKA_API_KEY=your-kafka-key
export KAFKA_API_SECRET=your-kafka-secret

# Produce and consume 20 messages using a named consumer group
python scripts/producer_consumer_demo.py

# Wait ~2-3 min for Metrics API ingestion, then validate
make validate
# Look for "ol-lineage-demo-consumer" in the lineage edges table
```

## CLI reference

```
ol-confluent [OPTIONS] COMMAND

Commands:
  validate   Fetch lineage graph and print edge table — no events emitted
  run-once   One poll cycle: fetch → map → emit
  run        Continuous polling on the configured interval

Options:
  --config PATH   Path to config.yaml (default: read from env vars)
  --verbose       Enable DEBUG logging
```

## Configuration reference

Top-level `confluent.*` scalars can be set in `config.yaml` or as environment variables. Per-env credentials live under `confluent.environments[]`. ksqlDB and self-managed Connect lists are YAML-only.

### `confluent` — top-level

| Key | Env var | Default | Description |
|---|---|---|---|
| `CONFLUENT_CLOUD_API_KEY` | same | — | Cloud-level API key (not resource-scoped). **Owner must have MetricsViewer + CloudClusterAdmin** — keys carry their owner's perms. |
| `CONFLUENT_CLOUD_API_SECRET` | same | — | Corresponding secret |
| `CONFLUENT_FLINK_REST_URL` | same | `https://flink.us-west-2.aws.confluent.cloud` | Regional Flink REST endpoint (stored; CLI is used today) |
| `CONFLUENT_METRICS_API_KEY` | same | *(cloud key)* | Optional dedicated Metrics API key (needs MetricsViewer role) |
| `CONFLUENT_METRICS_API_SECRET` | same | *(cloud secret)* | Corresponding secret |
| `CONFLUENT_METRICS_LOOKBACK_MINUTES` | same | `10` | Metrics API lookback window (minutes) |
| `CONFLUENT_TABLEFLOW_GLUE_REGION` | same | — | Glue region for Tableflow → Iceberg lineage (e.g. `us-west-2`) |
| `CONFLUENT_TABLEFLOW_GLUE_DB` | same | — | Glue database for Tableflow output |
| `consumer_group_exclude_prefixes` | — | `[]` | Extra group ID prefixes to exclude from lineage |
| `producer_client_id_exclude_prefixes` | — | `[]` | Extra producer client ID prefixes to exclude |
| `environments` | — | `[]` | **List of EnvDeployment entries** — one per CC env to bridge (see below) |
| `ksql_clusters` | — | `[]` | List of ksqlDB cluster configs (see below) |
| `self_managed_connect_clusters` | — | `[]` | List of self-managed Connect cluster configs (see below) |
| `selected_envs` | — | `[]` | **Wizard-private** — pending env picks between Save and Start; pydantic ignores this field |

### `confluent.environments` — per-env entries

Each entry carries env-scoped credentials. The bridge polls every entry in parallel and merges the results. Hot-reloads at the top of every cycle so wizard adds/removes/key-rotations are picked up within ≤60s.

```yaml
confluent:
  environments:
    - env_id:               "env-xxxxxx"           # Required
      cluster_id:           "lkc-xxxxxx"           # Required
      kafka_bootstrap:      "pkc-xxxxxx.<region>.aws.confluent.cloud:9092"  # Required

      # Optional — Kafka REST topic-metadata enrichment
      # Use a CLUSTER-SCOPED key (not Cloud-level). Cluster → API Keys.
      kafka_rest_endpoint:  "https://pkc-xxxxxx.<region>.aws.confluent.cloud:443"
      kafka_api_key:        "KAFKAAPIKEY"
      kafka_api_secret:     "kafka-api-secret"

      # Optional — used by provision_demo_pipelines.py
      flink_compute_pool:   "lfcp-xxxxxx"

      # Optional — adds SchemaDatasetFacet to every topic Dataset
      # SR has its own key scope. Environment → Schema Registry → API Keys.
      schema_registry:
        endpoint:   "https://psrc-xxxxx.<region>.aws.confluent.cloud"
        api_key:    "SRAPIKEY"
        api_secret: "sr-api-secret"
```

> **Backwards compat**: configs that still use the legacy top-level `CONFLUENT_ENV_ID` / `CONFLUENT_CLUSTER_ID` / `CONFLUENT_KAFKA_REST_*` / `schema_registry` keys are folded into a one-element `environments:` list at load time by `_apply_legacy_shim()` in `config.py`. New configs should use the `environments:` list directly.

### `confluent.ksql_clusters` and `confluent.self_managed_connect_clusters`

Top-level lists (NOT per-env — each entry has its own creds and namespace):

```yaml
confluent:
  ksql_clusters:
    - cluster_id:    "lksqlc-xxxxxx"
      rest_endpoint: "https://pksqlc-xxxxxx.<region>.aws.confluent.cloud"
      api_key:       "KSQLAPIKEY"
      api_secret:    "ksql-api-secret"

  self_managed_connect_clusters:
    - name:          "on-prem-etl"           # used in namespace: kafka-connect://on-prem-etl
      rest_endpoint: "http://connect-host:8083"
      username:      "admin"                 # optional
      password:      "secret"                # optional
```

### `openlineage`

| Key | Env var | Default | Description |
|---|---|---|---|
| `OPENLINEAGE_TRANSPORT` | same | `http` | `http` or `console` |
| `OPENLINEAGE_URL` | same | `http://localhost:5000` | OpenLineage backend URL |
| `OPENLINEAGE_API_KEY` | same | — | Backend API key (optional) |
| `OPENLINEAGE_KAFKA_BOOTSTRAP` | same | — | Bootstrap servers (used as dataset namespace) |
| `OPENLINEAGE_PRODUCER` | same | `openlineage-confluent/0.1.0` | Producer identifier in events |

### `pipeline`

| Key | Env var | Default | Description |
|---|---|---|---|
| `PIPELINE_POLL_INTERVAL` | same | `60` | Seconds to wait AFTER each cycle finishes before starting the next (completion-to-start, no overlap) |
| `PIPELINE_FULL_REFRESH` | same | `false` | Re-emit all events every cycle |
| `PIPELINE_MAX_WORKERS` | same | `8` | Parallel threads for event emission |
| `PIPELINE_STATE_DB` | same | `~/.openlineage-confluent/state.db` | SQLite state database (diff-tracking + removal-detection survive restarts) |

## Removal detection

When any component disappears (connector deleted, Flink statement stopped, consumer group inactive, ksqlDB query dropped), the bridge diffs the current graph against the persisted previous-cycle state and emits a `RunState.ABORT` event for each removed job:

```json
{
  "eventType": "ABORT",
  "job": {"namespace": "flink://env-m2qxq", "name": "ol-high-value-alerts"},
  "run": {"runId": "..."}
}
```

State is persisted in SQLite so removal detection works across process restarts.

**Quarantine (`failed_namespaces`):** when a fetch fails (401, 5xx, network, CLI subprocess crash), the bridge marks the corresponding OL job-namespace prefix as untrustworthy this cycle. The emitter **skips ABORT events** for any known job in those namespaces — a transient auth blip won't synthesize phantom removals that wipe lineage in Marquez. Recovery is automatic on the next successful poll. If the source recovers and legitimately reports zero resources, ABORTs fire as normal.

## Development

```bash
make test        # 229 tests, all offline — no credentials required
make lint        # ruff
make type-check  # mypy
```

## Scripts

| File | Purpose |
|---|---|
| `provision_demo_pipelines.py` | Provisions `--num-pipelines N` demo pipelines of random length in `[2, --max-nodes M]` (Datagen → optional chained Flink statements → optional consumer group) for testing. The wizard's *Provision demo pipelines* button shells out to this. `--teardown` does a two-phase delete: state-tracked first, then a stateless sweep that lists all `ol-*` resources in the env and deletes them. `--status` shows what's currently provisioned. Requires `pip install confluent-kafka`. |
| `producer_consumer_demo.py` | Produces and consumes messages using a named consumer group — use to verify Metrics API consumer lineage detection |
| `java_client_demo/` | Java Maven project showing end-to-end lineage for native Kafka Java clients. `OrderProducer` emits OpenLineage START/COMPLETE events directly to Marquez (the only way to capture producer identity); `OrderConsumer` uses group `ol-java-demo-consumer`, which the bridge detects via the Metrics API automatically. See [Java client demo](#java-client-end-to-end-demo). |
| `connector-datagen-orders.json` | DatagenSource connector config for DEVTEST topology |
| `connector-http-sink.json` | HttpSink connector config |
| `connector-dummy-sink.json` | Minimal DummySink (used for bulk removal detection demo) |
| `flink-enrich-orders.sql` | Flink: flatten address, add risk_tier |
| `flink-high-value-alerts.sql` | Flink: filter HIGH risk orders |
| `flink-medium-risk-orders.sql` | Flink: filter MEDIUM risk orders |
| `schema-orders-enriched.avsc` | AVRO schema for `ol-orders-enriched` |
| `schema-high-value-alerts.avsc` | AVRO schema for `ol-high-value-alerts` |
| `schema-medium-risk-orders.avsc` | AVRO schema for `ol-medium-risk-orders` |

## Java client end-to-end demo

This demo shows how native Kafka Java producers and consumers appear in the lineage graph using two different mechanisms.

### Why two mechanisms?

| Side | How lineage is captured | Reason |
|---|---|---|
| **Producer** | App emits OpenLineage events directly (`openlineage-java` SDK) | The Confluent Metrics API has no `client_id` dimension for producers — there is no broker-side signal to identify who wrote to a topic |
| **Consumer** | Bridge detects automatically via Metrics API `consumer_lag_offsets` | Consumer groups are a first-class Kafka concept with a broker-side signal; no app-side changes needed |

### What you'll see in Marquez

```
[order-producer]          java-app://order-service       (emitted directly by the app)
       │
       ▼
  ol-raw-orders            kafka://<bootstrap>
       │
       ├──▶ [ol-enrich-orders]          Flink (bridge-detected)
       │           │
       │     ol-orders-enriched
       │           ...
       │
       └──▶ [ol-java-demo-consumer]    kafka-consumer-group://<cluster_id>  (bridge-detected)
```

### Prerequisites

- JDK 17+, Maven 3.8+
- Marquez running locally (`make marquez-up`)
- A **cluster-scoped** Kafka API key (Confluent Cloud → Cluster → API Keys → Kafka cluster scoped)

### Run

```bash
# Set credentials
export KAFKA_BOOTSTRAP_SERVERS=pkc-xxx.us-west-2.aws.confluent.cloud:9092
export KAFKA_API_KEY=YOUR_CLUSTER_KEY
export KAFKA_API_SECRET=YOUR_CLUSTER_SECRET

# Terminal 1 — produce 100 orders and emit lineage to Marquez
make java-demo-produce

# Terminal 2 — consume with named group for 90s (bridge detects it)
make java-demo-consume

# After ~2 min, check the graph
make validate
```

Optional overrides (env vars): `MARQUEZ_URL` (default `http://localhost:5000`), `OL_TOPIC` (default `ol-raw-orders`), `MESSAGE_COUNT` (default `100`), `POLL_DURATION_SECS` (default `90`).

## License

Apache 2.0

# openlineage-confluent

Bridges **all** Confluent Cloud components into the **[OpenLineage](https://openlineage.io)** standard, making your full Kafka topology visible in observability tools like [Marquez](https://marquezproject.ai), DataHub, and OpenMetadata.

```
Confluent Cloud                              OpenLineage backend
──────────────────────────────               ────────────────────
Managed Connect API    ──┐                   ┌─ Marquez UI  (http://localhost:3000)
Flink CLI              ──┤                   │
Metrics API (consumers)──┼──▶ ol-confluent ──▶─┤ DataHub
ksqlDB REST API        ──┤                   │
Self-managed Connect   ──┘                   └─ OpenMetadata / ...
Schema Registry  ──(enrichment)──┘
Kafka REST API   ──(enrichment)──┘
             (poll every N seconds)
```

## What it covers

Every poll cycle the bridge fetches from five lineage sources **concurrently**:

| Source | API | Components covered |
|---|---|---|
| **Managed Connect** | `api.confluent.cloud/connect/v1/…` | Confluent Cloud source and sink connectors |
| **Flink SQL** | `confluent flink statement list -o json` | Cloud Flink persistent statements |
| **Metrics API** | `api.telemetry.confluent.cloud/v2/metrics` | Any component committing Kafka offsets: application consumers, Kafka Streams, self-managed Connect workers |
| **ksqlDB REST** | `POST {cluster}/ksql` — `SHOW QUERIES EXTENDED` | ksqlDB persistent queries (CSAS / CTAS) |
| **Self-managed Connect** | `GET {endpoint}/connectors?expand=info,status` | On-prem or customer-hosted Connect clusters |

Two **optional enrichment sources** add metadata facets to every topic Dataset:

| Source | API | Facet added |
|---|---|---|
| **Schema Registry** | `/subjects/{topic}-key|value/versions/latest` | `SchemaDatasetFacet` — field-level schema (AVRO, JSON Schema, Protobuf) |
| **Kafka REST API** | `/kafka/v3/clusters/{id}/topics` | `KafkaTopicDatasetFacet` — partition count, replication factor, isInternal |

Throughput metrics are also fetched from the Metrics API when consumer lineage is enabled, adding `KafkaTopicThroughputDatasetFacet` (bytes in/out, records in/out) to each topic.

Then each cycle:

1. **Parses Flink SQL** — regex parser extracts `FROM`/`JOIN` inputs and `INSERT INTO`/`CREATE TABLE AS` outputs.
2. **Resolves ksqlDB stream names to topics** — `SHOW STREAMS/TABLES EXTENDED` maps stream names to their underlying Kafka topics before building edges.
3. **Filters internal consumer groups** — `_*` and `confluent_cli_consumer_*` groups (ksqlDB internals, Flink internals, CLI consumers) are excluded; application consumer groups remain.
4. **Enriches topic Datasets** — fetches schemas from Schema Registry and partition metadata from Kafka REST (if configured), attaches them as custom facets to every input/output Dataset.
5. **Builds a lineage graph** — one `(source → job → target)` edge per data-flow relationship.
6. **Diffs against the previous cycle** — each job's lineage is fingerprinted (SHA-256); unchanged jobs are skipped.
7. **Detects removals** — jobs absent from the current cycle get a `RunState.ABORT` event so backends mark them inactive.
8. **Emits RunEvents in parallel** — configurable `ThreadPoolExecutor`, one HTTP connection per thread.
9. **Persists state across restarts** — SQLite (WAL mode) stores known jobs and fingerprints.

## Namespace conventions

| Confluent component | OL Job namespace | OL Job name | OL Dataset namespace |
|---|---|---|---|
| Managed connector | `kafka-connect://<env_id>` | `<connector_name>` | `kafka://<bootstrap>` |
| Self-managed connector | `kafka-connect://<cluster_label>` | `<connector_name>` | `kafka://<bootstrap>` |
| Flink statement | `flink://<env_id>` | `<statement_name>` | `kafka://<bootstrap>` |
| Consumer group | `kafka-consumer-group://<cluster_id>` | `<group_id>` | `kafka://<bootstrap>` |
| ksqlDB query | `ksqldb://<ksql_cluster_id>` | `<query_id>` | `kafka://<bootstrap>` |

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

```bash
cp config.example.yaml config.yaml
# Edit config.yaml with your credentials
```

Minimum required fields:

| Field | Where to find it |
|---|---|
| `CONFLUENT_CLOUD_API_KEY` | Confluent Cloud → API Keys → Cloud resource management |
| `CONFLUENT_ENV_ID` | Confluent Cloud → Environments |
| `CONFLUENT_CLUSTER_ID` | Confluent Cloud → Cluster settings |
| `OPENLINEAGE_KAFKA_BOOTSTRAP` | Confluent Cloud → Cluster settings → Bootstrap servers |

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

All scalar values can be set in `config.yaml` or as environment variables.
List values (`ksql_clusters`, `self_managed_connect_clusters`) are YAML-only.

### `confluent`

| Key | Env var | Default | Description |
|---|---|---|---|
| `CONFLUENT_CLOUD_API_KEY` | same | — | Cloud-level API key (not resource-scoped) |
| `CONFLUENT_CLOUD_API_SECRET` | same | — | Corresponding secret |
| `CONFLUENT_ENV_ID` | same | — | Environment ID (`env-…`) |
| `CONFLUENT_CLUSTER_ID` | same | — | Kafka cluster ID (`lkc-…`) |
| `CONFLUENT_FLINK_REST_URL` | same | `https://flink.us-west-2.aws.confluent.cloud` | Regional Flink REST endpoint (stored; CLI is used today) |
| `CONFLUENT_METRICS_API_KEY` | same | *(cloud key)* | Optional dedicated Metrics API key (needs MetricsViewer role) |
| `CONFLUENT_METRICS_API_SECRET` | same | *(cloud secret)* | Corresponding secret |
| `CONFLUENT_METRICS_LOOKBACK_MINUTES` | same | `10` | Metrics API lookback window (minutes) |
| `consumer_group_exclude_prefixes` | — | `[]` | Extra group ID prefixes to exclude from lineage |
| `ksql_clusters` | — | `[]` | List of ksqlDB cluster configs (see below) |
| `self_managed_connect_clusters` | — | `[]` | List of self-managed Connect cluster configs (see below) |

**Kafka REST API (optional) — adds `KafkaTopicDatasetFacet` to each topic:**
```yaml
CONFLUENT_KAFKA_REST_ENDPOINT: "https://lkc-xxxxxx.<region>.aws.confluent.cloud:443"
CONFLUENT_KAFKA_API_KEY:       "KAFKAAPIKEY"
CONFLUENT_KAFKA_API_SECRET:    "kafka-api-secret"
```
> Use a **cluster-scoped** Kafka API key, not a Cloud-level key. Create it under Confluent Cloud → Cluster → API Keys.

**Schema Registry (optional) — adds `SchemaDatasetFacet` to each topic:**
```yaml
schema_registry:
  endpoint:   "https://psrc-xxxxx.<region>.aws.confluent.cloud"
  api_key:    "SRAPIKEY"
  api_secret: "sr-api-secret"
```
> Schema Registry uses its own API key scope. Generate it under Confluent Cloud → Environment → Schema Registry → API Keys.

**ksqlDB cluster entry:**
```yaml
ksql_clusters:
  - cluster_id:    "lksqlc-xxxxxx"
    rest_endpoint: "https://pksqlc-xxxxxx.<region>.aws.confluent.cloud"
    api_key:       "KSQLAPIKEY"
    api_secret:    "ksql-api-secret"
```

**Self-managed Connect entry:**
```yaml
self_managed_connect_clusters:
  - name:          "on-prem-etl"       # used in namespace: kafka-connect://on-prem-etl
    rest_endpoint: "http://connect-host:8083"
    username:      "admin"             # optional
    password:      "secret"           # optional
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
| `PIPELINE_POLL_INTERVAL` | same | `60` | Seconds between poll cycles |
| `PIPELINE_FULL_REFRESH` | same | `false` | Re-emit all events every cycle |
| `PIPELINE_MAX_WORKERS` | same | `8` | Parallel threads for event emission |
| `PIPELINE_STATE_DB` | same | `~/.openlineage-confluent/state.db` | SQLite state database |

## Removal detection

When any component disappears (connector deleted, Flink statement stopped, consumer group inactive, ksqlDB query dropped), the bridge detects this by diffing the current graph against the persisted previous-cycle state.

For each removed job the bridge emits a `RunState.ABORT` event:

```json
{
  "eventType": "ABORT",
  "job": {"namespace": "flink://env-m2qxq", "name": "ol-high-value-alerts"},
  "run": {"runId": "..."}
}
```

State is persisted in SQLite so removal detection works across process restarts.

## Development

```bash
make test        # 146 tests, all offline — no credentials required
make lint        # ruff
make type-check  # mypy
```

## Scripts

| File | Purpose |
|---|---|
| `provision_20_pipelines.py` | Provisions 19 new demo pipelines (17 with Flink enrich + consumer group, 2 Datagen-only) across business domains — brings total to 20 visible in Marquez. Includes `--teardown` and `--status` modes. Requires `pip install confluent-kafka`. |
| `producer_consumer_demo.py` | Produces and consumes messages using a named consumer group — use to verify Metrics API consumer lineage detection |
| `connector-datagen-orders.json` | DatagenSource connector config for DEVTEST topology |
| `connector-http-sink.json` | HttpSink connector config |
| `connector-dummy-sink.json` | Minimal DummySink (used for bulk removal detection demo) |
| `flink-enrich-orders.sql` | Flink: flatten address, add risk_tier |
| `flink-high-value-alerts.sql` | Flink: filter HIGH risk orders |
| `flink-medium-risk-orders.sql` | Flink: filter MEDIUM risk orders |
| `schema-orders-enriched.avsc` | AVRO schema for `ol-orders-enriched` |
| `schema-high-value-alerts.avsc` | AVRO schema for `ol-high-value-alerts` |
| `schema-medium-risk-orders.avsc` | AVRO schema for `ol-medium-risk-orders` |
| `stress_test.py` | 2-hour scale test: 3 envs × 100 pipelines × 20 components, with random mutations and Marquez verification |
| `diagnose_catalog.py` | Investigation script for the Confluent Stream Catalog Atlas API (determined unusable for operational lineage) |
| `diagnose_lineage_api.py` | Follow-up investigation of the `/catalog/v1/lineage/` endpoint |
| `diagnose_real_types.py` | Atlas entity type enumeration script |

## License

Apache 2.0

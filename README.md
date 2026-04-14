# openlineage-confluent

Bridges **Confluent Cloud** stream lineage into the **[OpenLineage](https://openlineage.io)** standard, making your Kafka topology visible in observability tools like [Marquez](https://marquezproject.ai), DataHub, and OpenMetadata.

```
Confluent Cloud                         OpenLineage backend
──────────────────                      ────────────────────
Connect API   ──┐                       ┌─ Marquez UI  (http://localhost:3000)
Flink CLI     ──┼──▶  ol-confluent  ──▶─┤
                │                       └─ DataHub / OpenMetadata / ...
(poll every N seconds)
```

## What it does

Every poll cycle the bridge:

1. **Fetches connectors** from the Confluent Connect REST API — extracts which topics each source/sink connector reads from or writes to.
2. **Fetches Flink statements** via the Confluent CLI — lists all RUNNING statements and extracts their SQL.
3. **Parses SQL** with a regex-based parser to extract `FROM`/`JOIN` inputs and `INSERT INTO`/`CREATE TABLE AS` outputs.
4. **Builds a lineage graph** of `(source_topic → job → target_topic)` edges.
5. **Emits RunEvents** — one per job — to the configured OpenLineage backend, skipping jobs whose lineage hasn't changed since the last cycle.

## Live demo topology (DEVTEST cluster)

The topology created for end-to-end validation:

```
[ol-datagen-orders-source]          DatagenSource connector
        │
        ▼
  ol-raw-orders                     Kafka topic (orders schema)
        │
[ol-enrich-orders]                  Flink: flatten address, add risk_tier
        │
  ol-orders-enriched                Kafka topic (enriched orders)
        │
  ┌─────┼──────────────┐
  ▼     ▼              ▼
[ol-high-value-alerts] [ol-medium-risk-orders] [ol-http-sink]
  │                    │                        │
  ol-high-value-alerts ol-medium-risk-orders    httpbin.org
  (HIGH risk only)     (MEDIUM risk only)       (HttpSink connector)
```

All three branches from `ol-orders-enriched` are visible in Marquez as a fan-out lineage graph.

## Prerequisites

- Python 3.11+
- [Confluent CLI](https://docs.confluent.io/confluent-cli/current/install.html) logged in (`confluent login`)
- A Confluent Cloud environment with Connect connectors and/or Flink statements
- Docker (or [OrbStack](https://orbstack.dev) on macOS) — for running Marquez locally

## Quick start

### 1. Install

```bash
git clone https://github.com/yourname/openlineage-confluent
cd openlineage-confluent
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Configure

```bash
cp config.example.yaml config.yaml
# Edit config.yaml with your Confluent Cloud credentials
```

Key fields:

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
# One-shot: fetch and emit, then exit
make run-once

# Continuous: poll every 60 seconds
make run

# Validate only (no emit):
make validate
```

Open **http://localhost:3000**, search for any topic or job, and navigate the lineage graph.

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

All values can be set in `config.yaml` or as environment variables.

### `[confluent]`

| Key | Env var | Description |
|---|---|---|
| `CONFLUENT_CLOUD_API_KEY` | `CONFLUENT_CLOUD_API_KEY` | Cloud-level API key (not resource-scoped) |
| `CONFLUENT_CLOUD_API_SECRET` | `CONFLUENT_CLOUD_API_SECRET` | Corresponding secret |
| `CONFLUENT_ENV_ID` | `CONFLUENT_ENV_ID` | Confluent environment ID (`env-…`) |
| `CONFLUENT_CLUSTER_ID` | `CONFLUENT_CLUSTER_ID` | Kafka cluster ID (`lkc-…`) |
| `CONFLUENT_FLINK_REST_URL` | `CONFLUENT_FLINK_REST_URL` | Regional Flink endpoint |

### `[openlineage]`

| Key | Env var | Default | Description |
|---|---|---|---|
| `OPENLINEAGE_TRANSPORT` | `OPENLINEAGE_TRANSPORT` | `http` | `http` or `console` |
| `OPENLINEAGE_URL` | `OPENLINEAGE_URL` | `http://localhost:5000` | OpenLineage backend URL |
| `OPENLINEAGE_API_KEY` | `OPENLINEAGE_API_KEY` | — | Backend API key (optional) |
| `OPENLINEAGE_KAFKA_BOOTSTRAP` | `OPENLINEAGE_KAFKA_BOOTSTRAP` | — | Bootstrap servers (used as dataset namespace) |
| `OPENLINEAGE_PRODUCER` | `OPENLINEAGE_PRODUCER` | `openlineage-confluent/0.1.0` | Producer identifier in events |

### `[pipeline]`

| Key | Env var | Default | Description |
|---|---|---|---|
| `PIPELINE_POLL_INTERVAL` | `PIPELINE_POLL_INTERVAL` | `60` | Seconds between poll cycles |
| `PIPELINE_FULL_REFRESH` | `PIPELINE_FULL_REFRESH` | `false` | Re-emit all events every cycle |

## Development

```bash
make test        # run pytest with coverage
make lint        # ruff
make type-check  # mypy
```

Tests are fully offline — no Confluent Cloud credentials required.

## Scripts

The `scripts/` directory contains the Flink SQL and AVRO schemas used to build the demo topology:

| File | Purpose |
|---|---|
| `connector-datagen-orders.json` | DatagenSource connector config |
| `connector-http-sink.json` | HttpSink connector config |
| `schema-orders-enriched.avsc` | AVRO schema for `ol-orders-enriched` |
| `schema-high-value-alerts.avsc` | AVRO schema for `ol-high-value-alerts` |
| `schema-medium-risk-orders.avsc` | AVRO schema for `ol-medium-risk-orders` |
| `flink-enrich-orders.sql` | Flink statement: flatten + risk tier |
| `flink-high-value-alerts.sql` | Flink statement: HIGH risk filter |
| `flink-medium-risk-orders.sql` | Flink statement: MEDIUM risk filter |

## License

Apache 2.0

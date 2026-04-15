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

1. **Fetches connectors and Flink statements concurrently** — Connect REST API and Confluent CLI run in parallel threads to minimise fetch latency.
2. **Parses Flink SQL** with a regex-based parser to extract `FROM`/`JOIN` inputs and `INSERT INTO`/`CREATE TABLE AS` outputs.
3. **Builds a lineage graph** of `(source_topic → job → target_topic)` edges.
4. **Diffs against the previous cycle** — each job's lineage is fingerprinted (SHA-256); unchanged jobs are skipped to avoid flooding the backend.
5. **Detects removals** — jobs present in the previous cycle but absent in the current one receive a `RunState.ABORT` event so backends can mark them as inactive.
6. **Emits RunEvents in parallel** — a configurable `ThreadPoolExecutor` saturates the backend's connection pool; one HTTP connection per worker thread.
7. **Persists state across restarts** — known jobs and fingerprints are stored in a SQLite database (WAL mode) so removal detection survives process restarts.

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
  ┌─────┼──────────────────────┐
  ▼     ▼                      ▼
[ol-high-value-alerts]  [ol-medium-risk-orders]  [ol-http-sink]
  │                      │                        │
  ol-high-value-alerts   ol-medium-risk-orders    httpbin.org
  (HIGH risk only)       (MEDIUM risk only)       (HttpSink connector)
```

`ol-high-value-alerts` was used to demonstrate removal detection: deleting the Flink statement caused the bridge to emit a `RunState.ABORT` event on the next poll cycle, and Marquez updated accordingly.

## Prerequisites

- Python 3.11+
- [Confluent CLI](https://docs.confluent.io/confluent-cli/current/install.html) logged in (`confluent login`)
- A Confluent Cloud environment with Connect connectors and/or Flink statements
- Docker (or [OrbStack](https://orbstack.dev) on macOS) — for running Marquez locally

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
| `CONFLUENT_FLINK_REST_URL` | `CONFLUENT_FLINK_REST_URL` | Regional Flink REST endpoint |

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
| `PIPELINE_FULL_REFRESH` | `PIPELINE_FULL_REFRESH` | `false` | Re-emit all events every cycle (useful after backend restart) |
| `PIPELINE_MAX_WORKERS` | `PIPELINE_MAX_WORKERS` | `8` | Parallel threads for event emission — raise for DataHub, 8 is safe for Marquez |
| `PIPELINE_STATE_DB` | `PIPELINE_STATE_DB` | `~/.openlineage-confluent/state.db` | SQLite database path for persisting known jobs across restarts |

## Removal detection

When a connector or Flink statement disappears from Confluent Cloud (deleted, stopped, or renamed), the bridge detects this by diffing the current graph against the previous cycle's persisted state.

For each missing job the bridge emits a `RunState.ABORT` event:

```json
{
  "eventType": "ABORT",
  "job": {"namespace": "flink://env-m2qxq", "name": "ol-high-value-alerts"},
  "run": {"runId": "..."}
}
```

This tells Marquez (or any OpenLineage-compatible backend) to mark the job as no longer active. The job is then removed from the known-jobs list, so it will not be reported again unless it reappears.

State is persisted in SQLite so removal detection works correctly even after a process restart — a job seen in cycle N and missing in cycle N+1 triggers an ABORT regardless of whether the process restarted between the two cycles.

## Development

```bash
make test        # run pytest (55 tests, all offline — no credentials required)
make lint        # ruff
make type-check  # mypy
```

## Scripts

The `scripts/` directory contains the Flink SQL and AVRO schemas used to build the demo topology:

| File | Purpose |
|---|---|
| `connector-datagen-orders.json` | DatagenSource connector config |
| `connector-http-sink.json` | HttpSink connector config |
| `connector-dummy-sink.json` | Minimal DummySink config (used for bulk removal detection demo) |
| `schema-orders-enriched.avsc` | AVRO schema for `ol-orders-enriched` |
| `schema-high-value-alerts.avsc` | AVRO schema for `ol-high-value-alerts` |
| `schema-medium-risk-orders.avsc` | AVRO schema for `ol-medium-risk-orders` |
| `flink-enrich-orders.sql` | Flink statement: flatten address + add risk_tier |
| `flink-high-value-alerts.sql` | Flink statement: filter HIGH risk orders |
| `flink-medium-risk-orders.sql` | Flink statement: filter MEDIUM risk orders |

## License

Apache 2.0

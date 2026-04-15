# Architecture

## Problem

Confluent Cloud builds operational lineage internally (visible in the Confluent UI under Stream Lineage), but does not expose it through any standard API. The **OpenLineage spec** is the de-facto standard for lineage interchange — understood by Marquez, DataHub, OpenMetadata, and others. This bridge translates between the two.

## What we tried first: Stream Catalog API

Confluent exposes an Atlas-compatible catalog at `api.confluent.cloud/catalog/v1/…`. The `/lineage/{guid}` endpoint looked promising, but after systematic investigation:

- The endpoint returns 404 for all entity GUIDs, including those returned by search
- The `typeName` filter in search is ignored — it always returns the same 3 internal `sampleRecord` entities
- Entity GUIDs are non-stable (they change between API calls)
- The catalog is designed for schema metadata (Avro/Protobuf), not operational lineage

**Conclusion:** The Stream Catalog API does not expose the graph that drives the Confluent Stream Lineage UI.

## Actual approach: build lineage from first principles

Lineage is reconstructed from two authoritative sources that **do** expose it:

### 1. Connect REST API

```
GET api.confluent.cloud/connect/v1/environments/{env}/clusters/{cluster}/connectors
    ?expand=info,status
```

Auth: Cloud-level API key (HTTP Basic).

Returns all managed connectors with their configs. Connector configs contain topic bindings:
- Source connectors: `kafka.topic` or `topic` → produces to
- Sink connectors: `topics` or `kafka.topics` → consumes from

This gives us `(connector_name → topic)` edges with no parsing required.

### 2. Flink SQL via CLI

The Flink REST API (`flink.<region>.aws.confluent.cloud/sql/v1/…`) requires user-level OAuth tokens. Cloud API keys get 401. Rather than requiring users to obtain OAuth tokens, the bridge shells out to:

```bash
confluent flink statement list --environment {env} -o json
```

The Confluent CLI already holds valid user credentials and emits clean JSON with `name`, `statement`, `status`, and `compute_pool` fields.

Only RUNNING statements are processed. The SQL is parsed to extract input and output table names.

### 3. SQL parser

`sql_parser.py` uses regex to extract table references:

- **Outputs**: `INSERT INTO \`table\`` and `CREATE TABLE [IF NOT EXISTS] \`table\``
- **Inputs**: `FROM \`table\`` and `JOIN \`table\`` — filtered to exclude SQL keywords and output tables

This covers the Confluent Flink SQL dialect used in practice. More complex patterns (CTEs, subqueries) are left for future work.

## Component diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│  openlineage-confluent                                                   │
│                                                                          │
│  ┌──────────────────────┐      ┌──────────────────────────────────┐      │
│  │ ConfluentLineage      │      │ ConfluentOpenLineageMapper       │      │
│  │ Client                │      │                                  │      │
│  │                       │      │  LineageEdge[]                   │      │
│  │  list_connectors()  ──┼─┐    │  → group by job                  │      │
│  │  list_flink_stmts() ──┼─┼───▶│  → one RunEvent per job          │      │
│  │  (parallel fetch)     │ │    │  → stable run ID (SHA-256)       │      │
│  │  get_lineage_graph()──┼─┘    └──────────────┬───────────────────┘      │
│  └──────────────────────┘                      │                          │
│                                                │                          │
│  ┌──────────────────────┐      ┌───────────────▼───────────────────┐      │
│  │ LineagePipeline       │      │ LineageEmitter                   │      │
│  │                       │      │                                  │      │
│  │  run_once()          ─┼─────▶│  emit_batch()                   │      │
│  │  run_forever()        │      │  ├─ diff tracking (SHA-256)      │      │
│  │  APScheduler          │      │  ├─ removal detection → ABORT    │      │
│  └──────────────────────┘      │  └─ parallel emission             │      │
│                                │       (ThreadPoolExecutor)        │      │
│  ┌──────────────────────┐      └──────────────┬────────┬───────────┘      │
│  │ StateStore (SQLite)   │◀─────────flush()────┘        │                 │
│  │                       │                              │ HTTP            │
│  │  WAL mode             │                              ▼                 │
│  │  atomic full-replace  │             ┌────────────────────────────┐     │
│  │  crash-safe           │             │  OpenLineage backend       │     │
│  └──────────────────────┘             │  (Marquez, DataHub, ...)   │     │
│                                       │  POST /api/v1/lineage      │     │
└───────────────────────────────────────┴────────────────────────────┘     │
                                                                            │
```

## OpenLineage mapping

### Namespaces

| Confluent entity | OpenLineage type | Namespace | Name |
|---|---|---|---|
| Kafka topic | `Dataset` | `kafka://<bootstrap>` | `<topic-name>` |
| Source/sink connector | `Job` | `kafka-connect://<env-id>` | `<connector-name>` |
| Flink statement | `Job` | `flink://<env-id>` | `<statement-name>` |

### Event structure

Each job produces one `RunEvent` with `eventType=COMPLETE`. The run ID is deterministic:

```python
SHA256(f"{namespace}:{name}:{cycle_key}")[:16]  → UUID4
```

`cycle_key` is the UTC timestamp of the poll cycle (`YYYYMMDDTHHMMSS`). This means:
- The same job in the same cycle always gets the same run ID (idempotent)
- Different cycles get different run IDs (Marquez stores a run history)

### Diff tracking

The emitter computes a fingerprint for each job's lineage:

```python
SHA256({job_key, sorted(inputs), sorted(outputs)})
```

If the fingerprint matches the previous cycle, the event is skipped. This avoids flooding the backend with redundant events during stable periods. The first cycle always forces a full emit.

## Removal detection

When a connector or Flink statement is deleted or stops running, the bridge detects this by comparing the current event batch against `_known_jobs` — the set of jobs seen in the previous cycle.

For every job in `_known_jobs` that is absent from the current batch, the bridge emits a `RunState.ABORT` event preserving the original job namespace and name. The job is then removed from `_known_jobs` and will not be reported again unless it reappears.

```python
removed_keys = set(_known_jobs) - {event_key(e) for e in current_events}
for key in removed_keys:
    namespace, name = _known_jobs[key]
    client.emit(RunEvent(eventType=RunState.ABORT, job=Job(namespace, name), ...))
```

The removed job's namespace is preserved exactly — a `flink://env-x` job gets an ABORT with that same namespace, not `kafka-connect://`.

Removal detection works correctly across process restarts because `_known_jobs` is loaded from the SQLite state database at startup. A job seen in cycle N-1, absent in cycle N, and with a process restart in between will still trigger an ABORT in cycle N.

## Production-scale design

The original implementation was single-threaded and used a flat JSON state file. At 10,000+ topics these become bottlenecks:

| Bottleneck | Solution |
|---|---|
| Sequential connector + Flink fetches | Parallel `ThreadPoolExecutor(max_workers=2)` in `get_lineage_graph()` |
| Single-threaded HTTP emission | Parallel `ThreadPoolExecutor(max_workers=N)` in `emit_batch()` |
| JSON state file — full rewrite per cycle, no concurrent reads | SQLite with WAL mode — single transaction per cycle, concurrent read-safe |
| Lock held during HTTP calls | Double-acquire pattern: lock → read fingerprint → **release** → HTTP → lock → write fingerprint |

### SQLite StateStore

`StateStore` (`src/openlineage_confluent/emitter/state_store.py`) persists known jobs and fingerprints:

- **Eager load**: full table is read into two in-memory dicts on startup — hot-path fingerprint checks are pure dict lookups, zero SQL per event.
- **Atomic flush**: one `DELETE + INSERT` transaction per `emit_batch()` call. No per-row upserts, no partial-write corruption.
- **WAL mode** (`PRAGMA journal_mode=WAL`): readers never block writers; safe for future multi-process access.
- **Corrupt DB recovery**: any `sqlite3.DatabaseError` at startup is caught and treated as an empty store — the process starts fresh rather than crashing.

### Thread-safe parallel emission

The emitter uses a `threading.Lock` to guard `_known_jobs` and `_last_fingerprints`. The lock is deliberately **released before each HTTP call** to avoid blocking other workers during network I/O:

```python
# 1. Acquire lock — check fingerprint, register job
with self._lock:
    if not force and fingerprint == self._last_fingerprints.get(key):
        return False   # skip
    self._known_jobs[key] = (namespace, name)

# 2. Lock released — make HTTP call (may take 10–500ms)
self._client.emit(event)

# 3. Re-acquire lock — write updated fingerprint
with self._lock:
    self._last_fingerprints[key] = fingerprint
```

`emit_batch()` submits all events to a `ThreadPoolExecutor`, collects results via `as_completed()`, then snapshots state under the lock and flushes to SQLite once per cycle.

### Parallel graph fetch

`ConfluentLineageClient.get_lineage_graph()` fetches connectors (HTTP) and Flink statements (CLI subprocess) concurrently:

```python
with ThreadPoolExecutor(max_workers=2) as pool:
    f_conn = pool.submit(self.list_connectors)
    f_stmt = pool.submit(self.list_flink_statements)
connectors = f_conn.result()
statements  = f_stmt.result()
```

These two calls hit completely independent APIs with no shared state, so the fetch time is `max(connect_latency, flink_cli_latency)` rather than the sum.

## Data flow example

Given the demo topology:

```
ol-datagen-orders-source  →  ol-raw-orders
ol-enrich-orders          :  ol-raw-orders → ol-orders-enriched
ol-high-value-alerts      :  ol-orders-enriched → ol-high-value-alerts
ol-medium-risk-orders     :  ol-orders-enriched → ol-medium-risk-orders
ol-http-sink              ←  ol-orders-enriched
```

The bridge emits 5 `RunEvent`s per cycle (one per job):

```json
// ol-enrich-orders
{
  "eventType": "COMPLETE",
  "job": {"namespace": "flink://env-m2qxq", "name": "ol-enrich-orders"},
  "inputs":  [{"namespace": "kafka://pkc-…:9092", "name": "ol-raw-orders"}],
  "outputs": [{"namespace": "kafka://pkc-…:9092", "name": "ol-orders-enriched"}]
}
```

If `ol-high-value-alerts` is then deleted from Confluent Cloud, the next cycle emits:

```json
{
  "eventType": "ABORT",
  "job": {"namespace": "flink://env-m2qxq", "name": "ol-high-value-alerts"},
  "run": {"runId": "..."}
}
```

Marquez marks the job inactive. Subsequent cycles emit 4 events, and `ol-high-value-alerts` does not appear again unless recreated.

## Marquez deployment

Marquez requires PostgreSQL. The `docker-compose.yml` runs:

1. `postgres:14` — PostgreSQL with health check
2. `marquezproject/marquez:latest` — API on port 5000, admin on 5001
3. `marquezproject/marquez-web:latest` — UI on port 3000

Key configuration notes discovered during setup:
- Marquez uses Dropwizard config with variable substitution. The correct env vars are `POSTGRES_HOST`/`POSTGRES_PORT` (not `MARQUEZ_DB_HOST` — that name is not recognised and causes the API to connect to `localhost:5432` inside the container, finding nothing).
- Set `SEARCH_ENABLED=false` unless you also run OpenSearch — otherwise the API refuses to start.
- The web container requires `WEB_PORT=3000` explicitly — it does not have a default and will exit without it.
- The official images are `linux/amd64`. On Apple Silicon, OrbStack handles the emulation transparently via Rosetta. The platform mismatch warning is non-fatal.

## Known limitations

- **Flink SQL parsing**: regex-based, covers `INSERT INTO` and `CREATE TABLE AS SELECT`. Complex patterns (CTEs, lateral joins, subqueries) may not parse correctly.
- **Flink auth**: relies on the Confluent CLI being logged in. If the CLI session expires, `list_flink_statements()` returns an empty list and logs a warning — it does not crash.
- **External sources/sinks**: connector source endpoints (e.g., the datagen internal source) and sink endpoints (e.g., httpbin.org) are omitted from the OpenLineage graph — only Kafka topics become Datasets.
- **ksqlDB**: not yet supported. The Connect API approach would work for ksqlDB topics; SQL parsing would need extension.
- **Schema lineage**: field-level lineage is not yet extracted from AVRO schemas.
- **Flink REST API**: the regional Flink REST endpoint (`flink.<region>.aws.confluent.cloud/sql/v1`) requires user-level OAuth tokens that Cloud API keys cannot obtain. The CLI workaround is functional but ties availability to a logged-in CLI session.

## Future work

- Use Confluent CLI OAuth token to hit Flink REST directly (avoids subprocess dependency)
- Add ksqlDB statement support
- Extract field-level lineage from Schema Registry AVRO schemas — add `SchemaDatasetFacet` to Dataset nodes
- Support DataHub and OpenMetadata as explicit backends with per-backend transport config
- Audit Log consumer as an alternative lineage source (event-driven rather than poll-based) for very large clusters

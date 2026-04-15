# Architecture

## Problem

Confluent Cloud builds operational lineage internally (visible in the Confluent UI under Stream Lineage), but does not expose it through any standard API. The **OpenLineage spec** is the de-facto standard for lineage interchange — understood by Marquez, DataHub, OpenMetadata, and others. This bridge reconstructs that lineage from first principles and emits it in the OpenLineage format.

## What we tried first: Stream Catalog API

Confluent exposes an Atlas-compatible catalog at `api.confluent.cloud/catalog/v1/…`. The `/lineage/{guid}` endpoint looked promising, but after systematic investigation:

- The endpoint returns 404 for all entity GUIDs, including those returned by search
- The `typeName` filter in search is ignored — it always returns the same 3 internal `sampleRecord` entities
- Entity GUIDs are non-stable (they change between API calls)
- The catalog is designed for schema metadata (Avro/Protobuf), not operational lineage

**Conclusion:** The Stream Catalog API does not expose the graph that drives the Confluent Stream Lineage UI. Investigation scripts are preserved in `scripts/diagnose_*.py`.

---

## Actual approach: five lineage sources

Lineage is reconstructed from five authoritative sources, all fetched concurrently:

### 1. Managed Connect REST API

```
GET api.confluent.cloud/connect/v1/environments/{env}/clusters/{cluster}/connectors
    ?expand=info,status
```

Auth: Cloud-level API key (HTTP Basic). Returns all managed connectors with their configs. Connector configs contain topic bindings:

- Source connectors: `kafka.topic` or `topic` → produces to
- Sink connectors: `topics` or `kafka.topics` → consumes from

### 2. Flink SQL via CLI

The Flink REST API (`flink.<region>.aws.confluent.cloud/sql/v1/…`) requires user-level OAuth tokens. Cloud API keys get 401. The bridge shells out to:

```bash
confluent flink statement list --environment {env} -o json
```

The CLI holds valid user credentials and emits clean JSON with `name`, `statement`, `status`, and `compute_pool` fields. Only RUNNING statements are processed. SQL is parsed to extract input/output table names.

### 3. Confluent Metrics API — consumer group lineage

```
POST https://api.telemetry.confluent.cloud/v2/metrics/cloud/query
{
  "aggregations": [{"metric": "io.confluent.kafka.server/consumer_lag_offsets", "agg": "SUM"}],
  "filter": {"field": "resource.kafka.id", "op": "EQ", "value": "<cluster_id>"},
  "granularity": "PT1M",
  "intervals": ["<last-N-minutes>"],
  "group_by": ["metric.consumer_group_id", "metric.topic"]
}
```

Auth: Cloud-level API key with the **MetricsViewer** role (grant at Environment or Org level). A dedicated metrics key can be configured; if omitted, the cloud API key is used.

The `consumer_lag_offsets` metric is the only cloud-plane signal that exposes `consumer_group_id` alongside `topic`. This covers every component that commits Kafka consumer group offsets:

- Application consumers (Java, Python, Go, etc.)
- Kafka Streams applications
- Self-managed Connect workers (`connect-<name>` groups)
- Any other component using the Kafka consumer group protocol

**Important limitation:** There is no `client_id` dimension on any broker-side metric. Producer identity cannot be inferred from the Metrics API alone — the broker aggregates by topic/partition/group, not by producer client ID.

**Internal group filtering:** Groups with the `_` prefix (`_confluent-*`, `_ksql-*`, `_schemas`) and `confluent_cli_consumer_*` prefix are excluded. These are internal Confluent infrastructure groups already represented by their dedicated lineage sources.

**Pagination:** The Metrics API returns up to 1,000 rows per request. The bridge follows `meta.pagination.page_token` until exhausted.

### 4. ksqlDB REST API

Three statements are executed against each configured ksqlDB cluster:

```
POST {rest_endpoint}/ksql  {"ksql": "SHOW STREAMS EXTENDED;"}
POST {rest_endpoint}/ksql  {"ksql": "SHOW TABLES EXTENDED;"}
POST {rest_endpoint}/ksql  {"ksql": "SHOW QUERIES EXTENDED;"}
```

Auth: ksqlDB-scoped API key (create under Confluent Cloud → ksqlDB cluster → API Keys). Content-Type must be `application/vnd.ksql.v1+json`.

The `SHOW QUERIES EXTENDED` response includes `sinkKafkaTopics` (actual Kafka topic names) and `sources` (ksqlDB stream/table names). Stream names are resolved to their underlying Kafka topics via the map built from the first two statements. Self-referential entries (output topic also listed as a source) are removed.

Only RUNNING persistent queries with both resolved source and sink topics are emitted.

### 5. Self-managed Kafka Connect REST API

```
GET {endpoint}/connectors?expand=info,status
```

Auth: optional HTTP Basic. Available since Kafka Connect 2.3 (KIP-449). For older clusters that return 404 on `?expand`, the bridge falls back to individual `GET /connectors/{name}` and `GET /connectors/{name}/status` calls.

Each configured self-managed cluster has a unique `name` label used as the namespace: `kafka-connect://<name>`.

---

## SQL parser

`sql_parser.py` uses regex to extract table references from Flink SQL:

- **Outputs**: `INSERT INTO \`table\`` and `CREATE TABLE [IF NOT EXISTS] \`table\``
- **Inputs**: `FROM \`table\`` and `JOIN \`table\`` — filtered to exclude SQL keywords and output tables

---

## Component diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  ConfluentLineageClient.get_lineage_graph()                                  │
│                                                                              │
│  ┌────────────────────┐   ┌────────────────────┐   ┌────────────────────┐   │
│  │ list_connectors()  │   │list_flink_stmts()  │   │MetricsApiClient    │   │
│  │ (Connect REST API) │   │ (Confluent CLI)    │   │.get_consumer_groups│   │
│  └─────────┬──────────┘   └────────┬───────────┘   └────────┬───────────┘   │
│            │                       │  ← concurrent →        │               │
│  ┌─────────┴──────────┐            │            ┌───────────┴───────────┐   │
│  │KsqlDbClient        │            │            │SelfManagedConnect     │   │
│  │.get_queries()      │            │            │Client.get_connectors()│   │
│  │(per ksqlDB cluster)│            │            │(per sm Connect cluster)│  │
│  └─────────┬──────────┘            │            └───────────┬───────────┘   │
│            └───────────────────────┼────────────────────────┘               │
│                                    ▼                                         │
│                         list[LineageEdge]                                    │
│                         → LineageGraph                                       │
└──────────────────────────────┬─────────────────────────────────────────────-┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  ConfluentOpenLineageMapper.map_all(graph)                                   │
│                                                                              │
│  Group edges by (job_name, job_type, job_namespace_hint)                     │
│  → one RunEvent per unique job                                               │
│                                                                              │
│  Edge type → OL event shape:                                                 │
│    kafka_connect_source    → outputs only (topic Datasets)                   │
│    kafka_connect_sink      → inputs only  (topic Datasets)                   │
│    flink_statement         → inputs + outputs                                │
│    kafka_consumer_group    → inputs only  (consumer_group target ignored)    │
│    ksqldb_query            → inputs + outputs                                │
│                                                                              │
│  Run ID: SHA-256(namespace:name:cycle_key)[:16] → UUID4  (deterministic)    │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  LineageEmitter.emit_batch(events)                                           │
│                                                                              │
│  ├─ Removal detection → ABORT events for disappeared jobs                    │
│  ├─ Diff tracking (SHA-256 fingerprint per job) → skip unchanged             │
│  ├─ Parallel HTTP emission (ThreadPoolExecutor, lock released during I/O)    │
│  └─ Atomic SQLite flush (WAL mode, one transaction per cycle)                │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
                    ┌──────────┴──────────┐
                    ▼                     ▼
             StateStore (SQLite)   OpenLineage backend
             WAL, crash-safe       (Marquez, DataHub, …)
```

---

## OpenLineage mapping

### Namespaces

| Confluent entity | OL type | Namespace | Name |
|---|---|---|---|
| Kafka topic | `Dataset` | `kafka://<bootstrap>` | `<topic-name>` |
| Managed source/sink connector | `Job` | `kafka-connect://<env_id>` | `<connector-name>` |
| Self-managed connector | `Job` | `kafka-connect://<cluster_label>` | `<connector-name>` |
| Flink statement | `Job` | `flink://<env_id>` | `<statement-name>` |
| Consumer group | `Job` | `kafka-consumer-group://<cluster_id>` | `<group_id>` |
| ksqlDB persistent query | `Job` | `ksqldb://<ksql_cluster_id>` | `<query_id>` |

### Event structure

Each job produces one `RunEvent` with `eventType=COMPLETE`. The run ID is deterministic:

```python
SHA256(f"{namespace}:{name}:{cycle_key}")[:16]  → UUID4
```

`cycle_key` is the UTC timestamp of the poll cycle (`YYYYMMDDTHHMMSS`). This means:
- The same job in the same cycle always gets the same run ID (idempotent)
- Different cycles get different run IDs (Marquez stores a run history)

### Edge semantics and OL event shape

The mapper filters on `source_type`/`target_type`. Only `"kafka_topic"` typed endpoints become OpenLineage Dataset nodes:

| Job type | source_type | target_type | OL inputs | OL outputs |
|---|---|---|---|---|
| `kafka_connect_source` | `external` | `kafka_topic` | none | topic(s) |
| `kafka_connect_sink` | `kafka_topic` | `external` | topic(s) | none |
| `flink_statement` | `kafka_topic` | `kafka_topic` | topic(s) | topic(s) |
| `kafka_consumer_group` | `kafka_topic` | `consumer_group` | topic(s) | none |
| `ksqldb_query` | `kafka_topic` | `kafka_topic` | topic(s) | topic(s) |

Consumer groups are represented as Jobs with inputs (the topics they subscribe to) and no outputs — exactly how Marquez visualises a pure consumer.

### Diff tracking

The emitter computes a fingerprint for each job's lineage:

```python
SHA256({job_key, sorted(inputs), sorted(outputs)})
```

If the fingerprint matches the previous cycle, the event is skipped. The first cycle always forces a full emit.

---

## Removal detection

When a component disappears from the lineage graph (connector deleted, Flink statement stopped, consumer group idle beyond lookback window, ksqlDB query dropped), the bridge detects this by comparing the current event batch against `_known_jobs` — the set of jobs seen in the previous cycle.

For every absent job, the bridge emits a `RunState.ABORT` event preserving the original job namespace and name, then removes it from `_known_jobs`.

```python
removed_keys = set(_known_jobs) - {event_key(e) for e in current_events}
for key in removed_keys:
    namespace, name = _known_jobs[key]
    client.emit(RunEvent(eventType=RunState.ABORT, job=Job(namespace, name), ...))
```

Removal detection works correctly across process restarts because `_known_jobs` is loaded from the SQLite state database at startup.

---

## Production-scale design

| Bottleneck | Solution |
|---|---|
| Sequential multi-source fetches | Concurrent `ThreadPoolExecutor` in `get_lineage_graph()` — all 5 sources run in parallel |
| Single-threaded HTTP emission | Parallel `ThreadPoolExecutor(max_workers=N)` in `emit_batch()` |
| JSON state file — full rewrite per cycle | SQLite WAL mode — single atomic transaction per cycle |
| Lock held during HTTP calls | Lock-release pattern: lock → read fingerprint → **release** → HTTP → lock → write fingerprint |

### SQLite StateStore

`StateStore` persists known jobs and fingerprints:

- **Eager load**: full table is read into two in-memory dicts at startup — hot-path fingerprint checks are pure dict lookups, zero SQL per event.
- **Atomic flush**: one `DELETE + INSERT` transaction per `emit_batch()` call. No per-row upserts, no partial-write corruption.
- **WAL mode** (`PRAGMA journal_mode=WAL`): readers never block writers.
- **Corrupt DB recovery**: any `sqlite3.DatabaseError` at startup is caught and treated as an empty store — the process starts fresh rather than crashing.

### Thread-safe parallel emission

The emitter uses a `threading.Lock` to guard `_known_jobs` and `_last_fingerprints`. The lock is **released before each HTTP call** to avoid blocking other workers during network I/O:

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

---

## Data flow example

Given the demo topology:

```
ol-datagen-orders-source  →  ol-raw-orders
ol-enrich-orders          :  ol-raw-orders → ol-orders-enriched
ol-high-value-alerts      :  ol-orders-enriched → ol-high-value-alerts
ol-http-sink              ←  ol-orders-enriched
order-processor-service   ←  ol-orders-enriched  (consumer group)
CSAS_HIGH_VALUE_STREAM_0  :  ol-orders-enriched → HIGH_VALUE_STREAM  (ksqlDB)
```

The bridge emits 6 `RunEvent`s per cycle (one per job):

```json
// Consumer group (inputs only — correct for a pure consumer)
{
  "eventType": "COMPLETE",
  "job": {"namespace": "kafka-consumer-group://lkc-1j6rd3", "name": "order-processor-service"},
  "inputs":  [{"namespace": "kafka://pkc-…:9092", "name": "ol-orders-enriched"}],
  "outputs": null
}

// ksqlDB query
{
  "eventType": "COMPLETE",
  "job": {"namespace": "ksqldb://lksqlc-xxxxx", "name": "CSAS_HIGH_VALUE_STREAM_0"},
  "inputs":  [{"namespace": "kafka://pkc-…:9092", "name": "ol-orders-enriched"}],
  "outputs": [{"namespace": "kafka://pkc-…:9092", "name": "HIGH_VALUE_STREAM"}]
}
```

---

## Marquez deployment

Marquez requires PostgreSQL. The `docker-compose.yml` runs:

1. `postgres:14` — PostgreSQL with health check
2. `marquezproject/marquez:latest` — API on port 5000, admin on 5001
3. `marquezproject/marquez-web:latest` — UI on port 3000

Key configuration notes:
- Use `POSTGRES_HOST`/`POSTGRES_PORT` (not `MARQUEZ_DB_HOST` — unrecognised by Marquez)
- Set `SEARCH_ENABLED=false` unless you also run OpenSearch
- Set `WEB_PORT=3000` explicitly — it has no default and exits without it
- Official images are `linux/amd64`; OrbStack on Apple Silicon handles emulation transparently

---

## Known limitations

- **Producer identity**: No `client_id` dimension exists on any Confluent Metrics API broker metric. Individual producers cannot be identified from the cloud plane. Options: instrument producers directly with the OpenLineage SDK, or infer from Schema Registry (schema registrant ≈ likely writer).
- **Flink SQL parsing**: Regex-based, covers `INSERT INTO` and `CREATE TABLE AS SELECT`. Complex patterns (CTEs, lateral joins, subqueries) may not parse correctly.
- **Flink auth**: Relies on the Confluent CLI being logged in. If the CLI session expires, `list_flink_statements()` returns an empty list and logs a warning — it does not crash.
- **Consumer group lookback**: Groups not seen within `CONFLUENT_METRICS_LOOKBACK_MINUTES` (default 10) are treated as inactive and removed from the graph. Increase the lookback for bursty consumers.
- **ksqlDB source resolution**: If `SHOW STREAMS/TABLES EXTENDED` fails (auth error, timeout), source stream names fall back to their ksqlDB names rather than Kafka topic names.
- **External sources/sinks**: Connector source endpoints (e.g., datagen internal source) and sink endpoints (e.g., httpbin.org) are omitted from the OpenLineage graph — only Kafka topics become Datasets.
- **Field-level lineage**: Schema-level lineage (column → column mappings) is not yet extracted.

## Future work

- Use Confluent CLI OAuth token to hit Flink REST directly (removes subprocess dependency)
- Extract field-level lineage from Schema Registry AVRO schemas — add `SchemaDatasetFacet` to Dataset nodes
- Producer lineage via Schema Registry subject registration (schema registrant ≈ writer)
- Support DataHub and OpenMetadata as explicit backends with per-backend transport config
- Audit Log consumer as an alternative lineage source (event-driven rather than poll-based) for very large clusters

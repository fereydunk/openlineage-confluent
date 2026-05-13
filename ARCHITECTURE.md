# Architecture

## Problem

Confluent Cloud builds operational lineage internally (visible in the Confluent UI under Stream Lineage), but does not expose it through any standard API. The **OpenLineage spec** is the de-facto standard for lineage interchange — understood by Marquez, DataHub, OpenMetadata, and others. This bridge reconstructs that lineage from first principles and emits it in the OpenLineage format.

## What we tried first: Stream Catalog API

Confluent exposes an Atlas-compatible catalog at `api.confluent.cloud/catalog/v1/…`. The `/lineage/{guid}` endpoint looked promising, but after systematic investigation:

- The endpoint returns 404 for all entity GUIDs, including those returned by search
- The `typeName` filter in search is ignored — it always returns the same 3 internal `sampleRecord` entities
- Entity GUIDs are non-stable (they change between API calls)
- The catalog is designed for schema metadata (Avro/Protobuf), not operational lineage

**Conclusion:** The Stream Catalog API does not expose the graph that drives the Confluent Stream Lineage UI.

---

## Actual approach: seven lineage sources

Lineage is reconstructed from seven authoritative sources, all fetched concurrently per env (then merged across envs):

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

**Internal group filtering:** Groups with the `_` prefix (`_confluent-*`, `_ksql-*`, `_schemas`) and `confluent_cli_consumer_*` prefix are excluded. These are internal Confluent infrastructure groups already represented by their dedicated lineage sources.

**Pagination:** The Metrics API returns up to 1,000 rows per request. The bridge follows `meta.pagination.page_token` until exhausted.

### 4. Confluent Metrics API — producer lineage

Same Metrics API endpoint as consumer lineage, but querying `received_bytes` grouped by `client_id` and `topic`:

```
POST https://api.telemetry.confluent.cloud/v2/metrics/cloud/query
{
  "aggregations": [{"metric": "io.confluent.kafka.server/received_bytes", "agg": "SUM"}],
  "filter": {"field": "resource.kafka.id", "op": "EQ", "value": "<cluster_id>"},
  "group_by": ["metric.topic", "metric.client_id"],
  ...
}
```

`metric.client_id` IS a valid groupBy dimension on `received_bytes` and `received_records` (an earlier limitation in this doc; the API gained it). This gives per-producer identity tied to the topics they actually wrote to in the lookback window.

**Internal producer filtering:** client IDs starting with `connector-producer-` (managed Connect internal), `connector-` (self-managed Connect internal), or `_confluent-flink_` (Confluent Cloud Flink internal) are excluded — those flows are already captured by the Connect / Flink sources.

### 5. ksqlDB REST API

Three statements are executed against each configured ksqlDB cluster:

```
POST {rest_endpoint}/ksql  {"ksql": "SHOW STREAMS EXTENDED;"}
POST {rest_endpoint}/ksql  {"ksql": "SHOW TABLES EXTENDED;"}
POST {rest_endpoint}/ksql  {"ksql": "SHOW QUERIES EXTENDED;"}
```

Auth: ksqlDB-scoped API key (create under Confluent Cloud → ksqlDB cluster → API Keys). Content-Type must be `application/vnd.ksql.v1+json`.

The `SHOW QUERIES EXTENDED` response includes `sinkKafkaTopics` (actual Kafka topic names) and `sources` (ksqlDB stream/table names). Stream names are resolved to their underlying Kafka topics via the map built from the first two statements. Self-referential entries (output topic also listed as a source) are removed.

Only RUNNING persistent queries with both resolved source and sink topics are emitted.

### 6. Self-managed Kafka Connect REST API

```
GET {endpoint}/connectors?expand=info,status
```

Auth: optional HTTP Basic. Available since Kafka Connect 2.3 (KIP-449). For older clusters that return 404 on `?expand`, the bridge falls back to individual `GET /connectors/{name}` and `GET /connectors/{name}/status` calls. Per-connector failures inside that fallback set `last_ok = False` so the emitter quarantines the cluster's namespace (no false ABORTs from partial reads).

Each configured self-managed cluster has a unique `name` label used as the namespace: `kafka-connect://<name>`.

### 7. Tableflow via CLI

The Tableflow REST API requires a bearer token Cloud API keys can't mint (same restriction as the Flink REST API). The bridge shells out to:

```bash
confluent tableflow topic list --environment {env} --cluster {cluster} -o json
```

Each active Tableflow topic becomes a `TABLE_SYNC` job with the source Kafka topic as input and an Iceberg/Glue dataset as output:

- **Job**: `tableflow://<env_id>` / `<topic_name>`
- **Input**: `kafka://<bootstrap>` / `<topic_name>`
- **Output**: `glue://<region>` / `<glue_db>.<table_name>` (when `CONFLUENT_TABLEFLOW_GLUE_REGION` + `_GLUE_DB` are configured)

This closes the lake-house lineage loop: the Spark/Trino reader using OpenLineage's Spark integration will write to the same `glue://<region>/<db>.<table>` Dataset namespace, stitching streaming → table → query lineage in Marquez.

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
| Iceberg table (Tableflow) | `Dataset` | `glue://<region>` | `<glue_db>.<table-name>` |
| Managed source/sink connector | `Job` | `kafka-connect://<env_id>` | `<connector-name>` |
| Self-managed connector | `Job` | `kafka-connect://<cluster_label>` | `<connector-name>` |
| Flink statement | `Job` | `flink://<env_id>` | `<statement-name>` |
| Consumer group | `Job` | `kafka-consumer-group://<cluster_id>` | `<group_id>` |
| Kafka producer | `Job` | `kafka-producer://<cluster_id>` | `<client_id>` |
| ksqlDB persistent query | `Job` | `ksqldb://<ksql_cluster_id>` | `<query_id>` |
| Tableflow sync | `Job` | `tableflow://<env_id>` | `<topic-name>` |

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
| `kafka_producer` | `external` | `kafka_topic` | none | topic(s) |
| `ksqldb_query` | `kafka_topic` | `kafka_topic` | topic(s) | topic(s) |
| `tableflow_topic_sync` | `kafka_topic` | `iceberg_table` | topic | iceberg dataset |

Consumer groups are represented as Jobs with inputs (the topics they subscribe to) and no outputs — exactly how Marquez visualises a pure consumer. Producers are the dual: outputs only, no inputs (the producer "writes" to topics).

### Confluent topology facet

Every job and Kafka topic dataset emitted by the bridge carries a custom
`confluent` facet with the topology context:

```json
{
  "facets": {
    "confluent": {
      "envId":     "env-dpog0y",
      "clusterId": "lkc-96vkp7",
      "cloud":     "aws",
      "region":    "us-east-2"
    }
  }
}
```

Defined in `src/openlineage_confluent/mapping/facets.py` (`ConfluentJobFacet` +
`ConfluentDatasetFacet`). Populated from `LineageEdge.{env_id, cluster_id,
cloud, region}` which `_EnvLineageClient.build_graph()` stamps on every
per-env edge. Lets Marquez consumers query "every job in env-X" or "every
dataset in us-east-2" off a typed facet rather than parsing namespace strings.

The facet is **omitted** for globally-scoped jobs (ksqlDB clusters,
self-managed Connect) since they don't belong to any one CC env/cluster — the
cluster label is the only topology identifier and it's already in the
namespace URI.

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

### Quarantine via `failed_namespaces`

A naive removal-detection has a serious failure mode: when a fetch returns 401 (auth blip), 5xx (CC outage), or times out, the affected source returns `[]`, the merged graph "loses" all those resources, and the diff-tracker fires ABORTs for every job that was in the previous graph from that source — wiping lineage in Marquez even though the resources are still there.

The fix: every source client (Connect / Flink / Metrics consumers / Metrics producers / Tableflow / ksqlDB / self-managed Connect) tracks a `last_ok` (or per-method `*_ok`) flag. When false, `_EnvLineageClient.build_graph()` adds the corresponding OL job-namespace prefix to `LineageGraph.failed_namespaces`. The emitter's `emit_batch(events, failed_namespaces=...)` then **suppresses removal-detection** for any known job whose namespace is in that set, AND keeps the job in `_known_jobs` so the next successful poll either re-emits it normally or finally removes it:

```python
removed_keys = []
quarantined  = 0
for k, (ns, _) in self._known_jobs.items():
    if k in current_keys:    continue
    if ns in failed_namespaces:
        quarantined += 1     # leave in _known_jobs; skip ABORT
        continue
    removed_keys.append(k)
```

A transient 401 storm during a Confluent Cloud incident no longer wipes Marquez. Recovery is automatic on the next cycle that returns 200.

## Hot-reload of `config.yaml`

`LineagePipeline.run_forever()` is a simple sleep-loop on `threading.Event` (no APScheduler — dropped in favor of completion-to-start interval semantics). At the top of every cycle, `_maybe_reload_envs()` re-reads the config file from disk and rebuilds the underlying `ConfluentLineageClient` if the env signature changed.

The signature is `frozenset(_env_signature(e) for e in cfg.environments)` where `_env_signature(env)` is a tuple of `env_id`, `cluster_id`, `kafka_bootstrap`, `flink_compute_pool`, `kafka_rest_endpoint`, Kafka API key + secret, and an SR sub-tuple. Any rotation of those triggers a rebuild.

Crucially, the **emitter and StateStore are NOT rebuilt** — diff-tracking continuity survives the swap. YAML errors during reload are caught and logged; the bridge keeps polling on the previous config until the next cycle reads a parseable file. Net effect: clicking "Provision demo pipelines" or "Save selection" in the wizard updates `config.yaml`, and within ≤60s the running bridge picks it up — no Stop / Start needed.

---

## Production-scale design

| Bottleneck | Solution |
|---|---|
| Sequential multi-source fetches | Per-env `ThreadPoolExecutor` in `_EnvLineageClient.build_graph()` — 6 sources run in parallel; per-env graphs then merged in another `ThreadPoolExecutor` across envs |
| Single-threaded HTTP emission | Parallel `ThreadPoolExecutor(max_workers=N)` in `emit_batch()` |
| JSON state file — full rewrite per cycle | SQLite WAL mode — single atomic transaction per cycle |
| Lock held during HTTP calls | Lock-release pattern: lock → read fingerprint → **release** → HTTP → lock → write fingerprint |
| False ABORTs from transient fetch failures | `failed_namespaces` quarantine — known jobs in failed namespaces stay in `_known_jobs` instead of being ABORT'd |
| Config changes require Stop/Start | Hot-reload — `_maybe_reload_envs()` at top of every cycle rebuilds the lineage client only if env signature changed |

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

A captured snapshot of one demo pipeline lives in [`examples/9-node-pipeline/`](examples/9-node-pipeline/) — actual `RunEvent` JSON the bridge POSTed to Marquez, replayable into any OpenLineage backend. Pipeline shape:

```
ol-orders00-datagen  →  ol-orders00-t0                                  (Datagen managed connector)
ol-orders00-enrich-0 :  ol-orders00-t0 → ol-orders00-t1                 (Flink: schema-preserving INSERT ... SELECT)
ol-orders00-filter-1 :  ol-orders00-t1 → ol-orders00-t2                 (Flink: same shape, optional WHERE)
ol-orders00-project-2:  ol-orders00-t2 → ol-orders00-t3                 (Flink: same shape)
ol-orders00-cg       ←  ol-orders00-t3                                  (consumer group, Metrics-API-detected)
```

= 1 connector + 4 topics + 3 Flink statements + 1 consumer group = 9 nodes, single connected chain in Marquez.

The bridge emits 5 `RunEvent`s per cycle (one per job). Excerpts (full JSON in `examples/9-node-pipeline/events.json`):

```json
// Source connector — outputs only
{
  "eventType": "COMPLETE",
  "job": {"namespace": "kafka-connect://env-dpog0y", "name": "ol-orders00-datagen"},
  "inputs":  [],
  "outputs": [{"namespace": "kafka://pkc-921jm.us-east-2.aws.confluent.cloud:9092", "name": "ol-orders00-t0"}]
}

// Flink statement — input + output
{
  "eventType": "COMPLETE",
  "job": {"namespace": "flink://env-dpog0y", "name": "ol-orders00-enrich-0"},
  "inputs":  [{"namespace": "kafka://...", "name": "ol-orders00-t0"}],
  "outputs": [{"namespace": "kafka://...", "name": "ol-orders00-t1"}]
}

// Consumer group — inputs only
{
  "eventType": "COMPLETE",
  "job": {"namespace": "kafka-consumer-group://lkc-96vkp7", "name": "ol-orders00-cg"},
  "inputs":  [{"namespace": "kafka://...", "name": "ol-orders00-t3"}],
  "outputs": []
}
```

If ksqlDB clusters, Tableflow syncs, or native Java producers are added later, additional events appear automatically — no code changes required. See the namespace conventions table earlier in this doc for the shape of those events.

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

- **Flink SQL parsing**: Regex-based, covers `INSERT INTO` and `CREATE TABLE AS SELECT`. Complex patterns (CTEs, lateral joins, subqueries) may not parse correctly.
- **Flink auth**: Relies on the Confluent CLI being logged in. If the CLI session expires, `list_flink_statements()` returns an empty list, sets `last_ok = False`, and the emitter quarantines the env's `flink://` namespace — no false ABORTs, but no new statements get picked up until the CLI is re-authed.
- **Consumer group lookback**: Groups not seen within `CONFLUENT_METRICS_LOOKBACK_MINUTES` (default 10) are treated as inactive and removed from the graph. Increase the lookback for bursty consumers.
- **ksqlDB source resolution**: If `SHOW STREAMS/TABLES EXTENDED` fails (auth error, timeout), `last_ok` flips to `False` and the cluster's namespace is quarantined; source stream names won't be resolved to topics until the next successful poll.
- **External sources/sinks**: Connector source endpoints (e.g., datagen internal source) and sink endpoints (e.g., httpbin.org) are omitted from the OpenLineage graph — only Kafka topics and Iceberg tables become Datasets.
- **Field-level lineage**: Schema-level lineage (column → column mappings) is not yet extracted.
- **Stateless sweep filter is broad**: `provision_demo_pipelines.py --teardown` Phase 2 deletes anything in the env whose name starts with `ol-`. A user-owned `ol-prod-orders` topic in the same env would be caught by the sweep. The wizard's "Delete all demo pipelines" button uses a per-env scope; it can't cross env boundaries.

## Future work

- Use Confluent CLI OAuth token to hit Flink REST directly (removes subprocess dependency)
- Extract field-level lineage from Schema Registry AVRO schemas — add `SchemaDatasetFacet` to Dataset nodes
- Support DataHub and OpenMetadata as explicit backends with per-backend transport config
- Audit Log consumer as an alternative lineage source (event-driven rather than poll-based) for very large clusters
- More distinctive demo prefix (`ol-demo-`) so the stateless sweep can't accidentally catch user-owned `ol-*` resources

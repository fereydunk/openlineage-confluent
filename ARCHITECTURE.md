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
┌────────────────────────────────────────────────────────────────────┐
│  openlineage-confluent                                             │
│                                                                    │
│  ┌─────────────────────┐     ┌──────────────────────────────┐     │
│  │ ConfluentLineage     │     │ ConfluentOpenLineageMapper   │     │
│  │ Client               │     │                              │     │
│  │                      │     │  LineageEdge[]               │     │
│  │  list_connectors()  ─┼─┐   │  → group by job              │     │
│  │  list_flink_stmts() ─┼─┼──▶│  → one RunEvent per job      │     │
│  │  get_lineage_graph()─┼─┘   │  → stable run ID (SHA256)    │     │
│  └─────────────────────┘     └──────────────┬───────────────┘     │
│                                              │                     │
│  ┌─────────────────────┐     ┌──────────────▼───────────────┐     │
│  │ LineagePipeline      │     │ LineageEmitter               │     │
│  │                      │     │                              │     │
│  │  run_once()         ─┼────▶│  emit_batch()               │     │
│  │  run_forever()       │     │  diff tracking (SHA256)      │     │
│  │  APScheduler         │     │  → skip unchanged jobs       │     │
│  └─────────────────────┘     └──────────────┬───────────────┘     │
│                                              │                     │
└──────────────────────────────────────────────┼─────────────────────┘
                                               │
                              ┌────────────────▼──────────────┐
                              │  OpenLineage backend          │
                              │  (Marquez, DataHub, ...)      │
                              │  POST /api/v1/lineage         │
                              └───────────────────────────────┘
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

Marquez stores this and builds a graph where `ol-orders-enriched` fans out to three jobs — exactly what you see in the UI.

## Marquez deployment

Marquez requires PostgreSQL. The `docker-compose.yml` runs:

1. `postgres:14` — PostgreSQL with health check
2. `marquezproject/marquez:latest` — API on port 5000, admin on 5001
3. `marquezproject/marquez-web:latest` — UI on port 3000

Key configuration notes:
- Marquez uses Dropwizard config with variable substitution. The correct env vars are `POSTGRES_HOST`/`POSTGRES_PORT` (not `MARQUEZ_DB_HOST`).
- Set `SEARCH_ENABLED=false` unless you also run OpenSearch.
- The official images are `linux/amd64`. On Apple Silicon, OrbStack handles the emulation transparently via Rosetta.

## Known limitations

- **Flink SQL parsing**: regex-based, covers `INSERT INTO` and `CREATE TABLE AS SELECT`. Complex patterns (CTEs, lateral joins, subqueries) may not parse correctly.
- **Flink auth**: relies on the Confluent CLI being logged in. If the CLI session expires, `list_flink_statements()` returns an empty list and logs a warning.
- **External sources/sinks**: connector source endpoints (e.g., the datagen internal source) and sink endpoints (e.g., httpbin.org) are omitted from the OpenLineage graph — only Kafka topics become Datasets.
- **ksqlDB**: not yet supported. The Connect API approach would work for ksqlDB topics; SQL parsing would need extension.
- **Schema lineage**: field-level lineage is not yet extracted from AVRO schemas.

## Future work

- Use Confluent CLI OAuth token to hit Flink REST directly (avoids subprocess)
- Add ksqlDB statement support
- Extract field-level lineage from Schema Registry AVRO schemas
- Add `SchemaDatasetFacet` to Dataset nodes
- Support DataHub and OpenMetadata as additional backends

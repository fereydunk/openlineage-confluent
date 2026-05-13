# 9-node pipeline — OpenLineage events example

Real OpenLineage `RunEvent` JSON captured from this bridge running against a
Confluent Cloud demo pipeline. The pipeline is a single linear chain:

```
[Datagen connector ol-orders00-datagen]
    → <ol-orders00-t0>
    → [Flink ol-orders00-enrich-0]
    → <ol-orders00-t1>
    → [Flink ol-orders00-filter-1]
    → <ol-orders00-t2>
    → [Flink ol-orders00-project-2]
    → <ol-orders00-t3>
    → [consumer ol-orders00-cg]
```

That's 9 nodes: 1 Datagen connector + 4 Kafka topics + 3 Flink statements +
1 consumer group.

## Files

- `events.json` — array of 5 OpenLineage `RunEvent` JSON objects, one per
  job (Datagen connector, 3 Flink statements, consumer group). These are
  the exact payloads the bridge POSTed to Marquez at `/api/v1/lineage`.

## Anatomy of an event

Every event has the same top-level shape:

```jsonc
{
  "eventType": "COMPLETE",            // START | RUNNING | COMPLETE | ABORT | FAIL
  "eventTime": "2026-05-13T19:12:22Z",
  "producer":  "https://.../openlineage-python-1.46.0",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
  "run":     { "runId": "<uuid>", "facets": { ... } },
  "job":     { "namespace": "...",  "name": "...", "facets": { ... } },
  "inputs":  [ { "namespace": "kafka://...", "name": "topic", "facets": { ... } } ],
  "outputs": [ { "namespace": "kafka://...", "name": "topic", "facets": { ... } } ]
}
```

Connections between jobs come from **dataset identity**: if job A's output
dataset has the same `(namespace, name)` as job B's input dataset, Marquez
draws an edge between them. That's why the chain renders as a single
connected lineage — every Flink stage's output topic is the next stage's
input topic.

## Facets used in this example

Standard OpenLineage facets:
- `jobType` (on each job) — `processingType=STREAMING`, `integration` =
  `KAFKA_CONNECT` / `FLINK` / `KAFKA`
- `schema` (on each Kafka topic dataset) — flattened key/value Avro fields
  (`value.ordertime`, `value.orderid`, etc.)
- `tags` (on each run) — OpenLineage client version

Custom facets defined by this bridge:
- `confluent` (on jobs + topic datasets) — `{envId, envName, clusterId,
  clusterName, cloud, region}`. Lets Marquez consumers filter by env or
  region without parsing namespace strings.
- `kafkaTopic` (on topic datasets) — `partitions`, `replicationFactor`,
  `isInternal`.
- `kafkaThroughput` (on topic datasets) — bytes/records in/out over the
  Metrics API lookback window.

## Replay into a fresh Marquez

```bash
# 1. Start Marquez
make marquez-up                  # or: docker compose up -d

# 2. Replay every event
python3 -c '
import json, urllib.request
events = json.load(open("examples/9-node-pipeline/events.json"))
for ev in events:
    req = urllib.request.Request(
        "http://localhost:5000/api/v1/lineage",
        data=json.dumps(ev).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req)
print(f"Replayed {len(events)} events")
'

# 3. Open the patched UI and view the lineage graph
open http://localhost:1337/lineage/job/flink%3A%2F%2Fenv-dpog0y/ol-orders00-filter-1
# (set Depth = 5 or toggle "Full Graph" to see all 9 nodes)
```

## Spec reference

- OpenLineage core schema: <https://openlineage.io/spec/2-0-2/OpenLineage.json>
- Facet schemas: <https://openlineage.io/spec/facets/>
- Marquez lineage API: <https://marquezproject.ai/docs/latest/openapi.html>

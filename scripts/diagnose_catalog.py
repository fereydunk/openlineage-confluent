"""Diagnostic script — explore what's actually in the Confluent Stream Catalog.

Usage:
    python scripts/diagnose_catalog.py
"""

import json
import httpx
import sys

SR_URL     = "https://psrc-lq3wm.eu-central-1.aws.confluent.cloud"
API_KEY    = "KPEWKIPWCADOUPP3"
API_SECRET = "cfltbk58oNMz3U/T1K34D9iVYWC8Gwzs+rR4l1dorZA0xbPN4qqIGmdXfxSO1G9g"

auth = (API_KEY, API_SECRET)
base = SR_URL + "/catalog/v1"

def get(path, **params):
    r = httpx.get(f"{base}{path}", auth=auth, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

# ── 1. Which entity types exist in the catalog? ──────────────────────────────
print("\n=== Entity type search — trying each candidate type ===")
candidates = [
    "kafka_topic", "kafka_connect_connector", "kafka_connect_sink",
    "kafka_connect_source_connector", "kafka_connect_sink_connector",
    "flink_job", "flink_statement", "flink_table",
    "ksql_stream", "ksql_table", "ksql_query",
    "sr_subject", "sr_schema",
]
found_types: dict[str, list[dict]] = {}
for t in candidates:
    try:
        res = get("/search/basic", typeName=t, limit=5, excludeDeletedEntities=True)
        entities = res.get("entities", [])
        if entities:
            found_types[t] = entities
            print(f"  {t:45s}  → {len(entities)} entities (showing first)")
            for e in entities[:2]:
                attrs = e.get("attributes", {})
                print(f"      guid={e['guid']}  name={attrs.get('name','?')}  qn={attrs.get('qualifiedName','?')[:80]}")
    except Exception as ex:
        print(f"  {t:45s}  → ERROR {ex}")

# ── 2. Try lineage on each found entity ──────────────────────────────────────
print("\n=== Lineage probe for found entities ===")
for type_name, entities in found_types.items():
    for entity in entities[:1]:
        guid = entity["guid"]
        attrs = entity.get("attributes", {})
        name = attrs.get("name", "?")
        try:
            lineage = get(f"/lineage/{guid}", depth=5, direction="BOTH")
            node_count = len(lineage.get("guidEntityMap", {}))
            rel_count  = len(lineage.get("relations", []))
            print(f"  {type_name}/{name}  → nodes={node_count}  relations={rel_count}")
            if rel_count:
                for rel in lineage.get("relations", [])[:3]:
                    fm = lineage["guidEntityMap"].get(rel["fromEntityId"], {})
                    to = lineage["guidEntityMap"].get(rel["toEntityId"], {})
                    print(f"      {fm.get('typeName','?')}/{fm.get('attributes',{}).get('name','?')}  →  {to.get('typeName','?')}/{to.get('attributes',{}).get('name','?')}")
        except httpx.HTTPStatusError as ex:
            print(f"  {type_name}/{name}  → {ex.response.status_code}")

# ── 3. Look for our OL topics specifically ───────────────────────────────────
print("\n=== Searching for 'ol-' topics ===")
try:
    res = get("/search/basic", typeName="kafka_topic", query="ol-", limit=20, excludeDeletedEntities=True)
    for e in res.get("entities", []):
        a = e.get("attributes", {})
        print(f"  {a.get('name','?'):40s}  guid={e['guid']}")
except Exception as ex:
    print(f"  ERROR: {ex}")

# ── 4. Try the /lineage endpoint for a specific ol-raw-orders topic ───────────
print("\n=== Direct lineage for 'ol-raw-orders' if found ===")
try:
    res = get("/search/basic", typeName="kafka_topic",
              query="ol-raw-orders", limit=5, excludeDeletedEntities=True)
    for e in res.get("entities", []):
        guid = e["guid"]
        name = e.get("attributes", {}).get("name", "?")
        print(f"  Topic: {name}  guid={guid}")
        try:
            lg = get(f"/lineage/{guid}", depth=10, direction="BOTH")
            print(f"    Lineage nodes: {len(lg.get('guidEntityMap',{}))}")
            print(f"    Relations:     {len(lg.get('relations',[]))}")
            print(f"    Full response: {json.dumps(lg, indent=2)[:2000]}")
        except httpx.HTTPStatusError as ex:
            print(f"    Lineage 404 — trying /entity/guid/{guid}")
            try:
                entity = get(f"/entity/guid/{guid}")
                print(f"    Entity fetch: {json.dumps(entity, indent=2)[:500]}")
            except Exception as ex2:
                print(f"    Entity fetch also failed: {ex2}")
except Exception as ex:
    print(f"  ERROR: {ex}")

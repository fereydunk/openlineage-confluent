"""Phase 3 — find real Confluent catalog type names and test lineage with real GUIDs."""

import json
import httpx

SR_URL     = "https://psrc-lq3wm.eu-central-1.aws.confluent.cloud"
API_KEY    = "KPEWKIPWCADOUPP3"
API_SECRET = "cfltbk58oNMz3U/T1K34D9iVYWC8Gwzs+rR4l1dorZA0xbPN4qqIGmdXfxSO1G9g"
auth = (API_KEY, API_SECRET)
base = SR_URL + "/catalog/v1"

def get(path, **params):
    r = httpx.get(f"{base}{path}", auth=auth, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

# ── 1. Search for ALL our entities using the real query approach ──────────────
print("=== Real entities with correct GUIDs ===")
all_entities = {}
for query in ["ol-datagen", "ol-http-sink", "ol-raw-orders", "ol-orders-enriched",
              "ol-high-value-alerts", "ol-enrich", "ol-high-value"]:
    try:
        res = get("/search/basic", query=query, limit=10)
        for e in res.get("entities", []):
            name = e.get("displayText") or e.get("attributes", {}).get("name", "?")
            guid = e["guid"]
            tname = e.get("typeName", "?")
            qn = e.get("attributes", {}).get("qualifiedName", "?")
            key = f"{tname}:{name}"
            if key not in all_entities:
                all_entities[key] = {"guid": guid, "typeName": tname, "name": name, "qn": qn}
    except Exception as ex:
        pass

for k, v in sorted(all_entities.items()):
    print(f"  {v['typeName']:30s}  {v['name']:40s}  guid={v['guid']}")

# ── 2. Find what type names Confluent uses ────────────────────────────────────
print("\n=== Discovered type names ===")
type_names = set(v["typeName"] for v in all_entities.values())
print("  ", sorted(type_names))

# ── 3. Try lineage with real GUIDs ───────────────────────────────────────────
print("\n=== Lineage probe with real GUIDs ===")
for k, v in all_entities.items():
    guid = v["guid"]
    name = v["name"]
    tname = v["typeName"]
    try:
        lg = get(f"/lineage/{guid}", depth=5, direction="BOTH")
        nodes = len(lg.get("guidEntityMap", {}))
        rels  = len(lg.get("relations", []))
        print(f"  {tname:30s}  {name:40s}  → nodes={nodes}  relations={rels}")
        if rels:
            print(f"    *** HAS LINEAGE! ***")
            print(json.dumps(lg, indent=2)[:2000])
    except httpx.HTTPStatusError as ex:
        print(f"  {tname:30s}  {name:40s}  → {ex.response.status_code}")

# ── 4. Fetch full entity for datagen connector ────────────────────────────────
print("\n=== Full entity for ol-datagen-orders-source ===")
datagen_guid = next((v["guid"] for v in all_entities.values() if v["name"] == "ol-datagen-orders-source"), None)
if datagen_guid:
    try:
        entity = get(f"/entity/guid/{datagen_guid}")
        print(json.dumps(entity, indent=2)[:4000])
    except httpx.HTTPStatusError as ex:
        print(f"  FAILED: {ex.response.status_code}  {ex.response.text[:300]}")
else:
    print("  ol-datagen-orders-source not found in all_entities")
    # Try with the hardcoded GUID from phase 2
    try:
        entity = get(f"/entity/guid/043e828e-22de-4038-b872-de680db476a7")
        print(json.dumps(entity, indent=2)[:4000])
    except httpx.HTTPStatusError as ex:
        print(f"  FAILED with hardcoded GUID: {ex.response.status_code}  {ex.response.text[:300]}")

# ── 5. Check what topics look like ───────────────────────────────────────────
print("\n=== Search for cn_ types ===")
for cn_type in ["cn_topic", "cn_connector", "cn_sr_subject", "cn_flink_statement",
                "cn_flink_job", "cn_flink_table", "cn_ksql_stream"]:
    try:
        res = get("/search/basic", typeName=cn_type, limit=5)
        ents = res.get("entities", [])
        if ents:
            print(f"  {cn_type:35s}  → {len(ents)} entities")
            for e in ents[:2]:
                attrs = e.get("attributes", {})
                print(f"      guid={e['guid']}  name={attrs.get('name','?')}  qn={attrs.get('qualifiedName','?')[:70]}")
        else:
            print(f"  {cn_type:35s}  → (empty)")
    except httpx.HTTPStatusError as ex:
        print(f"  {cn_type:35s}  → {ex.response.status_code}")

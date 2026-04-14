"""Phase 2 diagnostic — explore entity attributes and correct lineage API path."""

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

def get_full(url, **params):
    r = httpx.get(url, auth=auth, params=params, timeout=30)
    return r.status_code, r.text[:2000]

# Known GUIDs from the previous diagnostic
KNOWN = {
    "ol-raw-orders":          "a96f30e6-7da5-41d8-b7c6-74018a72601b",
    "ol-orders-enriched":     "90a814a4-2f82-46dc-b757-86954c182c82",
    "ol-high-value-alerts":   "51801913-3372-450f-be60-e0c508fb2f7e",
    "ol-datagen-orders-source": "6b63610d-ceeb-494a-945a-e5b38f67c9c0",
    "ol-http-sink":           "5482b3e6-261b-4d76-b923-b966018286c9",
    "ol-raw-orders-value":    "0d4a77d9-e3fd-46ed-b091-7c8f74581bb9",
}

print("=== 1. Fetch full entity attributes for each known GUID ===")
for name, guid in KNOWN.items():
    print(f"\n--- {name} ({guid}) ---")
    try:
        entity = get(f"/entity/guid/{guid}")
        e = entity.get("entity", entity)
        print(f"  typeName:      {e.get('typeName','?')}")
        attrs = e.get("attributes", {})
        print(f"  qualifiedName: {attrs.get('qualifiedName','?')}")
        rel_attrs = e.get("relationshipAttributes", {})
        if rel_attrs:
            print(f"  relationshipAttributes keys: {list(rel_attrs.keys())}")
            for k, v in rel_attrs.items():
                print(f"    {k}: {str(v)[:200]}")
        classifications = e.get("classifications", [])
        if classifications:
            print(f"  classifications: {[c.get('typeName') for c in classifications]}")
    except httpx.HTTPStatusError as ex:
        print(f"  Entity fetch FAILED: {ex.response.status_code}")
        print(f"  Response: {ex.response.text[:300]}")

print("\n\n=== 2. Try different lineage endpoint patterns ===")
connector_guid = KNOWN["ol-datagen-orders-source"]
topic_guid     = KNOWN["ol-raw-orders"]

patterns = [
    f"/lineage/{connector_guid}",
    f"/lineage/{connector_guid}?direction=OUTPUT",
    f"/lineage/{topic_guid}",
    f"/entity/guid/{connector_guid}/relationships",
    f"/entity/guid/{connector_guid}/header",
    f"/lineage?guid={connector_guid}",
]

for path in patterns:
    url = base + path if not path.startswith("/lineage?") else base + path
    try:
        code, body = get_full(base + path if "?" not in path else SR_URL + "/catalog/v1" + path)
        print(f"  {path[:70]:70s}  → {code}  {body[:100]}")
    except Exception as ex:
        print(f"  {path[:70]:70s}  → ERROR {ex}")

print("\n\n=== 3. Try Confluent Cloud API lineage endpoints ===")
CLOUD_API = "https://api.confluent.cloud"
ENV = "env-m2qxq"
CLUSTER = "lkc-1j6rd3"

cloud_paths = [
    f"/lineage/v1/environments/{ENV}/connections",
    f"/lineage/v1/environments/{ENV}/nodes",
    f"/stream-lineage/v1/environments/{ENV}",
    f"/dataflow/v1/environments/{ENV}",
    f"/catalog/v1/entity/guid/{connector_guid}",
    f"/stream-governance/v1/environments/{ENV}/lineage",
]
for path in cloud_paths:
    try:
        code, body = get_full(CLOUD_API + path)
        print(f"  {path[:70]:70s}  → {code}  {body[:120]}")
    except Exception as ex:
        print(f"  {path[:70]:70s}  → ERROR {ex}")

print("\n\n=== 4. Look at the raw search response to understand entity structure ===")
res = httpx.get(
    f"{base}/search/basic",
    auth=auth,
    params={"query": "ol-datagen", "limit": 3},
    timeout=30,
)
print(json.dumps(res.json(), indent=2)[:3000])

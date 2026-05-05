"""
Step 2: Wire Confluent Cloud to AWS for Tableflow on the orders pipeline.

  1. Create Confluent provider integration → get Confluent IAM ARN
  2. Update IAM role trust policy with Confluent's ARN (NO ExternalId condition)
  3. Enable Tableflow on orders-enriched topic (BYOS, customer-managed S3)
  4. Create Glue catalog integration → Iceberg metadata synced to Glue

Prerequisites:
  - 01_aws_setup.py must have run successfully
  - source ~/aws-session.env  (must have AWS_IAM_ROLE_ARN, AWS_S3_BUCKET, AWS_REGION)
  - CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET already in config.yaml

Run:
    source ~/aws-session.env && python3 scripts/e2e_pipeline/02_tableflow_setup.py
"""

from __future__ import annotations

import json
import os
import subprocess
import time

import boto3
import requests

CONFLUENT_API = "https://api.confluent.cloud"
ENV_ID        = "env-m2qxq"
CLUSTER_ID    = "lkc-1j6rd3"
TOPIC         = "orders-enriched"
IAM_ROLE      = "ol-lineage-confluent-role"
PI_NAME       = "ol-lineage-pi"
CI_NAME       = "ol-lineage-glue"

CC_KEY    = "MRYYEBG4MF6FM2BP"
CC_SECRET = "cfltcDWyksAheJxwNGy6yh9NLlpka5eZWg/6u0Dx+VrFhAD65Wlb1OzK8h3PCTbA"


def cc_get(path: str, **params) -> dict:
    r = requests.get(f"{CONFLUENT_API}{path}", auth=(CC_KEY, CC_SECRET), params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def cc_post(path: str, body: dict) -> dict:
    r = requests.post(f"{CONFLUENT_API}{path}", auth=(CC_KEY, CC_SECRET), json=body, timeout=20)
    if not r.ok:
        print(f"  CC error {r.status_code}: {r.text[:400]}")
        r.raise_for_status()
    return r.json()


# ── Step 1: Provider integration ─────────────────────────────────────────────

def create_provider_integration(role_arn: str) -> tuple[str, str]:
    """Returns (cspi_id, confluent_iam_arn). Idempotent."""
    resp = cc_get("/pim/v1/integrations", environment=ENV_ID)
    for pi in resp.get("data", []):
        if pi.get("config", {}).get("customer_iam_role_arn") == role_arn:
            cspi_id       = pi["id"]
            confluent_arn = pi["config"]["iam_role_arn"]
            print(f"[PI] Already exists: {cspi_id}")
            return cspi_id, confluent_arn

    resp = cc_post("/pim/v1/integrations", {
        "api_version":  "pim/v1",
        "kind":         "Integration",
        "display_name": PI_NAME,
        "provider":     "aws",
        "environment":  {"id": ENV_ID},
        "config": {
            "kind":                  "AwsIntegrationConfig",
            "customer_iam_role_arn": role_arn,
        },
    })
    cspi_id       = resp["id"]
    confluent_arn = resp["config"]["iam_role_arn"]
    print(f"[PI] Created: {cspi_id}  confluent_arn={confluent_arn}")
    return cspi_id, confluent_arn


# ── Step 2: Update IAM trust policy ──────────────────────────────────────────

def update_trust_policy(confluent_iam_arn: str) -> None:
    # No ExternalId condition — Confluent Tableflow does not pass ExternalId.
    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect":    "Allow",
            "Principal": {"AWS": confluent_iam_arn},
            "Action":    "sts:AssumeRole",
        }],
    }
    iam = boto3.client("iam")
    # Confluent takes 20-60s to provision their role in their AWS account.
    # AWS validates the principal exists before accepting the trust policy.
    max_attempts = 10
    for attempt in range(1, max_attempts + 1):
        try:
            iam.update_assume_role_policy(
                RoleName=IAM_ROLE,
                PolicyDocument=json.dumps(trust),
            )
            print(f"[IAM] Trust policy updated → principal={confluent_iam_arn}")
            break
        except iam.exceptions.MalformedPolicyDocumentException:
            if attempt == max_attempts:
                raise
            wait = 15 * attempt
            print(f"[IAM] Principal not yet valid (attempt {attempt}/{max_attempts}), "
                  f"waiting {wait}s for Confluent to provision their role...")
            time.sleep(wait)
    print("[IAM] Waiting 10s for IAM propagation...")
    time.sleep(10)


# ── Step 3: Enable Tableflow on orders-enriched ───────────────────────────────

def enable_tableflow_topic(cspi_id: str, bucket: str) -> None:
    result = subprocess.run(
        ["confluent", "tableflow", "topic", "list",
         "--environment", ENV_ID, "--cluster", CLUSTER_ID, "-o", "json"],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode == 0:
        try:
            topics = json.loads(result.stdout) or []
        except json.JSONDecodeError:
            topics = []
        for t in topics:
            name = t.get("display_name") or t.get("name") or ""
            if name == TOPIC:
                print(f"[TF] Tableflow already enabled on: {TOPIC}")
                return

    print(f"[TF] Enabling Tableflow on {TOPIC} → s3://{bucket} ...")
    result = subprocess.run(
        [
            "confluent", "tableflow", "topic", "enable", TOPIC,
            "--environment", ENV_ID,
            "--cluster", CLUSTER_ID,
            "--storage-type", "BYOS",
            "--provider-integration", cspi_id,
            "--bucket-name", bucket,
            "--table-formats", "ICEBERG",
            "--retention-ms", "604800000",
        ],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        print(f"[TF] WARN: {result.stderr[:300]}")
    else:
        print(f"[TF] Enabled: {result.stdout.strip()}")


# ── Step 4: Glue catalog integration ─────────────────────────────────────────

def create_glue_catalog_integration(cspi_id: str) -> None:
    result = subprocess.run(
        ["confluent", "tableflow", "catalog-integration", "list",
         "--environment", ENV_ID, "--cluster", CLUSTER_ID, "-o", "json"],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode == 0:
        try:
            cis = json.loads(result.stdout) or []
        except json.JSONDecodeError:
            cis = []
        for ci in cis:
            kind = (ci.get("spec") or {}).get("kind") or ci.get("type", "")
            name = ci.get("display_name") or ci.get("name") or ""
            if "glue" in kind.lower() or name == CI_NAME:
                print(f"[CI] Glue catalog integration already exists: {name}")
                return

    print(f"[CI] Creating Glue catalog integration {CI_NAME}...")
    result = subprocess.run(
        [
            "confluent", "tableflow", "catalog-integration", "create", CI_NAME,
            "--environment", ENV_ID,
            "--cluster", CLUSTER_ID,
            "--type", "aws",
            "--provider-integration", cspi_id,
        ],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        print(f"[CI] WARN: {result.stderr[:300]}")
    else:
        print(f"[CI] Created: {result.stdout.strip()}")


def main() -> None:
    role_arn = os.environ.get("AWS_IAM_ROLE_ARN")
    bucket   = os.environ.get("AWS_S3_BUCKET")
    if not role_arn or not bucket:
        raise SystemExit(
            "ERROR: AWS_IAM_ROLE_ARN and AWS_S3_BUCKET must be set.\n"
            "Run 01_aws_setup.py first, then source ~/aws-session.env with those values."
        )

    print(f"ENV_ID={ENV_ID}  CLUSTER={CLUSTER_ID}  TOPIC={TOPIC}")
    print(f"bucket={bucket}  role={role_arn}\n")

    print("[1/4] Creating Confluent provider integration...")
    cspi_id, confluent_arn = create_provider_integration(role_arn)

    print("\n[2/4] Updating IAM trust policy...")
    update_trust_policy(confluent_arn)

    print("\n[3/4] Enabling Tableflow on orders-enriched...")
    enable_tableflow_topic(cspi_id, bucket)

    print("\n[4/4] Creating Glue catalog integration...")
    create_glue_catalog_integration(cspi_id)

    print(f"""
Tableflow setup complete.

  Tableflow will begin syncing orders-enriched → s3://{bucket}/orders_enriched/
  Glue table will appear in database '{os.environ.get("AWS_GLUE_DATABASE", "ol_lineage")}'
  after the first flush (~2 min).

Next:
  1. Wait 2-3 min for Tableflow flush + Glue table registration
  2. source ~/aws-session.env && python3 scripts/e2e_pipeline/spark_job.py
  3. make run-once     (emit Tableflow lineage to Marquez)
""")


if __name__ == "__main__":
    main()

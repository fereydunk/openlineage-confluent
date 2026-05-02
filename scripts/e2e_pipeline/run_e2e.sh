#!/usr/bin/env bash
# End-to-end lineage demo: Connector → Kafka → Flink → Kafka → Tableflow → Iceberg → Spark
# Emits all lineage to Marquez so the full chain appears in the graph.
#
# Prerequisites:
#   1. source ~/aws-session.env       (AWS STS credentials)
#   2. AWS_ACCOUNT_ID must be set     (output of 01_aws_setup.py)
#   3. Marquez must be running        (make marquez-up)
#   4. Tableflow must be enabled      (run 02_tableflow_setup.py once)
#
# Usage:
#   source ~/aws-session.env && export AWS_ACCOUNT_ID=586051073099
#   bash scripts/e2e_pipeline/run_e2e.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MARQUEZ_URL="${MARQUEZ_URL:-http://localhost:5000}"
GLUE_DB="${GLUE_DATABASE:-ol_lineage}"
REGION="${AWS_REGION:-us-west-2}"

cd "$ROOT"

echo "=== Step 1: Emit Confluent lineage (connector + Flink + Tableflow) to Marquez ==="
.venv/bin/ol-confluent run-once --config config.yaml

echo ""
echo "=== Step 2: Run Spark job (reads Iceberg, writes summary, emits lineage via OL listener) ==="

if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
  echo "ERROR: AWS_ACCOUNT_ID not set. Export it before running this script."
  exit 1
fi

PYSPARK_SUBMIT="$(.venv/bin/python3 -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'bin', 'spark-submit'))")"

SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,\
org.apache.iceberg:iceberg-aws-bundle:1.8.1,\
io.openlineage:openlineage-spark_2.12:1.34.0"

JAVA_HOME=/opt/homebrew/opt/openjdk@21 \
MARQUEZ_URL="$MARQUEZ_URL" \
GLUE_DATABASE="$GLUE_DB" \
  "$PYSPARK_SUBMIT" \
    --packages "$SPARK_PACKAGES" \
    --exclude-packages "org.slf4j:slf4j-api,org.slf4j:slf4j-reload4j" \
    --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider" \
    "$SCRIPT_DIR/spark_job.py"

echo ""
echo "=== Done! ==="
echo ""
echo "Marquez lineage graph:"
echo "  http://localhost:1337/lineage/job/tableflow%3A%2F%2Fenv-m2qxq/orders-enriched"
echo ""
echo "Confluent Cloud stream lineage:"
echo "  https://confluent.cloud/environments/env-m2qxq/clusters/lkc-1j6rd3/stream-lineage"

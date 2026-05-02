"""Spark batch job: Iceberg (orders-enriched) → Iceberg (orders-summary).

Reads the Tableflow-synced Iceberg table 'ol_lineage.orders_enriched' from
AWS Glue, aggregates order counts by status (HIGH/MEDIUM/LOW), and writes
the result to 'ol_lineage.orders_summary'.

The OpenLineage Spark listener (configured via spark.extraListeners) auto-captures:
  input:  glue://us-west-2 / ol_lineage.orders_enriched
  output: glue://us-west-2 / ol_lineage.orders_summary

These dataset identifiers must match what the OL bridge emits for the Tableflow
job so Marquez stitches the full chain:
  Connector → Kafka → Flink → Kafka → Tableflow → Iceberg → Spark

Required env vars:
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  AWS_SESSION_TOKEN   (for STS temporary credentials)
  AWS_REGION          (default: us-west-2)
  AWS_ACCOUNT_ID

Optional env vars:
  MARQUEZ_URL         (default: http://localhost:5000)
  GLUE_DATABASE       (default: ol_lineage)
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp

GLUE_DB     = os.environ.get("GLUE_DATABASE", "ol_lineage")
REGION      = os.environ.get("AWS_REGION", "us-west-2")
ACCOUNT_ID  = os.environ.get("AWS_ACCOUNT_ID", "")
MARQUEZ_URL = os.environ.get("MARQUEZ_URL", "http://localhost:5000")

INPUT_TABLE  = f"{GLUE_DB}.orders_enriched"
OUTPUT_TABLE = f"{GLUE_DB}.orders_summary"

spark = (
    SparkSession.builder
    .appName("orders-summary")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://ol-lineage-{ACCOUNT_ID}/warehouse/")
    .config("spark.sql.defaultCatalog", "glue_catalog")
    # OpenLineage Spark listener — emits lineage events to Marquez automatically
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
    .config("spark.openlineage.transport.type", "http")
    .config("spark.openlineage.transport.url", MARQUEZ_URL)
    .config("spark.openlineage.transport.endpoint", "/api/v1/lineage")
    .config("spark.openlineage.namespace", f"glue://{REGION}")
    .config("spark.openlineage.parentJobNamespace", f"glue://{REGION}")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def main() -> None:
    print(f"Reading from Iceberg table: {INPUT_TABLE}")
    orders = spark.table(INPUT_TABLE)
    print(f"  Row count: {orders.count()}")
    orders.printSchema()

    summary = (
        orders
        .filter(col("status").isNotNull())
        .groupBy("status")
        .agg(
            count("*").alias("order_count"),
        )
        .withColumn("computed_at", current_timestamp())
        .orderBy("status")
    )

    summary.show(truncate=False)

    print(f"\nWriting summary to: {OUTPUT_TABLE}")
    (
        summary.writeTo(OUTPUT_TABLE)
        .tableProperty("format-version", "2")
        .createOrReplace()
    )
    print("Done.")


if __name__ == "__main__":
    main()

"""Spark batch job: orders-enriched (Kafka) → orders_enriched (local Parquet).

Reads the latest batch from the orders-enriched Kafka topic, decodes the
Confluent Avro Wire Format payload using the schema from Schema Registry,
and writes to a local Parquet warehouse partitioned by status.

The OpenLineage Spark listener (attached via spark.extraListeners) auto-captures
lineage — no OL code lives in this file, matching how Confluent builds lineage
for producers/consumers via the Metrics API.

Required env vars:
  KAFKA_BOOTSTRAP_SERVERS   e.g. pkc-xxx.aws.confluent.cloud:9092
  KAFKA_API_KEY             cluster-scoped Kafka API key
  KAFKA_API_SECRET          cluster-scoped Kafka API secret

Optional env vars:
  MARQUEZ_URL               default: http://localhost:5000
  SPARK_WAREHOUSE           default: ~/spark-warehouse
"""

from __future__ import annotations

import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr

BOOTSTRAP   = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
API_KEY     = os.environ["KAFKA_API_KEY"]
API_SECRET  = os.environ["KAFKA_API_SECRET"]
MARQUEZ_URL = os.environ.get("MARQUEZ_URL", "http://localhost:5000")
WAREHOUSE   = os.path.expanduser(os.environ.get("SPARK_WAREHOUSE", "~/spark-warehouse"))
TOPIC       = "orders-enriched"

# Actual Avro schema from Schema Registry (schema ID 100125).
# Flink infers nullable union types for all fields and preserves the nested
# address record from the DatagenSource source schema.
AVRO_SCHEMA = json.dumps({
    "type": "record",
    "name": "orders_enriched_value",
    "namespace": "org.apache.flink.avro.generated.record",
    "fields": [
        {"name": "ordertime",  "type": ["null", "long"],   "default": None},
        {"name": "orderid",    "type": ["null", "int"],    "default": None},
        {"name": "itemid",     "type": ["null", "string"], "default": None},
        {"name": "orderunits", "type": ["null", "double"], "default": None},
        {"name": "address", "type": ["null", {
            "type": "record",
            "name": "orders_enriched_value_address",
            "fields": [
                {"name": "city",    "type": ["null", "string"], "default": None},
                {"name": "state",   "type": ["null", "string"], "default": None},
                {"name": "zipcode", "type": ["null", "long"],   "default": None},
            ],
        }], "default": None},
        {"name": "status", "type": ["null", "string"], "default": None},
    ],
})

spark = (
    SparkSession.builder
    .appName("orders-to-warehouse")
    # ── OpenLineage listener — passively captures all dataset I/O ──────────────
    .config("spark.extraListeners",              "io.openlineage.spark.agent.OpenLineageSparkListener")
    .config("spark.openlineage.transport.type",  "http")
    .config("spark.openlineage.transport.url",   MARQUEZ_URL)
    .config("spark.openlineage.namespace",       "spark://local")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

jaas = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{API_KEY}" password="{API_SECRET}";'
)

raw = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers",    BOOTSTRAP)
    .option("kafka.security.protocol",    "SASL_SSL")
    .option("kafka.sasl.mechanism",       "PLAIN")
    .option("kafka.sasl.jaas.config",     jaas)
    .option("subscribe",                  TOPIC)
    .option("startingOffsets",            "earliest")
    .option("endingOffsets",              "latest")
    .load()
)

# Confluent Wire Format: 1 magic byte (0x00) + 4-byte schema ID = 5-byte header.
# SUBSTRING is 1-indexed in Spark SQL, so byte 6 onwards is the Avro payload.
decoded = (
    raw
    .withColumn("avro_bytes", expr("SUBSTRING(value, 6)"))
    .withColumn("data", from_avro(col("avro_bytes"), AVRO_SCHEMA, {"mode": "PERMISSIVE"}))
    .select(
        col("data.ordertime"),
        col("data.orderid"),
        col("data.itemid"),
        col("data.orderunits"),
        col("data.address.city").alias("city"),
        col("data.address.state").alias("state"),
        col("data.address.zipcode").alias("zipcode"),
        col("data.status"),
        col("partition"),
        col("offset"),
        col("timestamp"),
    )
)

output_path = f"{WAREHOUSE}/orders_enriched"
decoded.write.mode("overwrite").partitionBy("status").parquet(output_path)

count = spark.read.parquet(output_path).count()
print(f"\nWritten {count} records → {output_path}\n")

spark.stop()

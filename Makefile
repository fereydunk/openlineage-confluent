.PHONY: install test lint type-check marquez-up marquez-down marquez-wipe marquez-logs validate run-once run \
        java-demo-build java-demo-produce java-demo-consume \
        spark-run \
        e2e-aws-setup e2e-tableflow-setup e2e-run

JAVA_DEMO_DIR := scripts/java_client_demo
JAVA_DEMO_JAR := $(JAVA_DEMO_DIR)/target/openlineage-java-demo-1.0-SNAPSHOT.jar

# Detect docker binary — prefer OrbStack's docker on macOS, fall back to system docker
DOCKER := $(shell command -v ~/.orbstack/bin/docker 2>/dev/null || command -v docker)
COMPOSE := $(DOCKER) compose

# ─── Setup ────────────────────────────────────────────────────────────────────

install:
	pip install -e ".[dev]"

# ─── Quality ──────────────────────────────────────────────────────────────────

test:
	pytest -v --tb=short --cov=openlineage_confluent --cov-report=term-missing

lint:
	ruff check src tests

type-check:
	mypy src

# ─── Local Marquez backend ────────────────────────────────────────────────────

marquez-up:
	$(COMPOSE) up -d
	@echo ""
	@echo "  Marquez API  →  http://localhost:5000/api/v1/namespaces"
	@echo "  Marquez UI   →  http://localhost:3000"
	@echo ""

marquez-down:
	$(COMPOSE) down

# Drops the postgres volume (wipes every namespace/dataset/job/run) then brings
# the stack back up. Use to start lineage from scratch — irreversible.
marquez-wipe:
	$(COMPOSE) down -v
	$(COMPOSE) up -d

marquez-logs:
	$(COMPOSE) logs -f marquez-api

# ─── Pipeline commands ────────────────────────────────────────────────────────

# Fetch lineage graph and print summary table — no events emitted
validate:
	ol-confluent validate --config config.yaml

# Single poll cycle → emit to configured backend
run-once:
	ol-confluent run-once --config config.yaml

# Continuous polling (Ctrl-C to stop)
run:
	ol-confluent run --config config.yaml

# ─── Java client end-to-end demo ──────────────────────────────────────────────
# Requires: JDK 17+, Maven 3.8+
# Credentials: export KAFKA_BOOTSTRAP_SERVERS=... KAFKA_API_KEY=... KAFKA_API_SECRET=...

java-demo-build:
	cd $(JAVA_DEMO_DIR) && mvn -q package -DskipTests

# Produces 100 orders to ol-raw-orders; emits OL START/COMPLETE to Marquez directly.
java-demo-produce: java-demo-build
	java -cp $(JAVA_DEMO_JAR) demo.OrderProducer

# Consumes from ol-raw-orders with group ol-java-demo-consumer for 90s.
# The bridge detects the consumer group automatically via the Metrics API.
java-demo-consume: java-demo-build
	java -cp $(JAVA_DEMO_JAR) demo.OrderConsumer

# ─── Spark end-to-end demo ────────────────────────────────────────────────────
# Reads orders-enriched (Kafka) → decodes Avro → writes Parquet to ~/spark-warehouse/orders_enriched
# OL Spark listener auto-emits lineage to Marquez — no OL code in the job itself.
# Requires: export KAFKA_BOOTSTRAP_SERVERS=... KAFKA_API_KEY=... KAFKA_API_SECRET=...
#           export MARQUEZ_URL=http://localhost:5000  (default)

SPARK_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,\
org.apache.spark:spark-avro_2.12:3.5.5,\
io.openlineage:openlineage-spark_2.12:1.34.0

# Uses PySpark 3.5.5 from the project venv — system spark-submit (4.x) does not
# support the OL listener's Kafka source visitor or the QueryExecution.sparkSession() API.
PYSPARK_SUBMIT := $(shell .venv/bin/python3 -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'bin', 'spark-submit'))")

spark-run:
	JAVA_HOME=/opt/homebrew/opt/openjdk@21 \
	$(PYSPARK_SUBMIT) \
	  --packages "$(SPARK_PACKAGES)" \
	  --exclude-packages "org.slf4j:slf4j-api,org.slf4j:slf4j-reload4j" \
	  scripts/spark_job/orders_to_warehouse.py

# ─── End-to-end pipeline: Connector → Flink → Tableflow → Iceberg → Spark ────
# One-time setup (run once after refreshing AWS credentials):
#   make e2e-aws-setup      (creates S3, Glue DB, IAM role in us-west-2)
#   make e2e-tableflow-setup (wires Confluent to AWS, enables Tableflow)
#
# Daily run (after sourcing ~/aws-session.env):
#   make e2e-run            (OL bridge run-once + Spark job → lineage in Marquez)
#
# Requires: source ~/aws-session.env && export AWS_ACCOUNT_ID=586051073099

e2e-aws-setup:
	source ~/aws-session.env && python3 scripts/e2e_pipeline/01_aws_setup.py

e2e-tableflow-setup:
	source ~/aws-session.env && python3 scripts/e2e_pipeline/02_tableflow_setup.py

e2e-run:
	source ~/aws-session.env && bash scripts/e2e_pipeline/run_e2e.sh

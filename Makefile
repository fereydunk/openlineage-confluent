.PHONY: install test lint type-check marquez-up marquez-down marquez-logs validate run-once run \
        java-demo-build java-demo-produce java-demo-consume

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

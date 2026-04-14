.PHONY: install test lint type-check marquez-up marquez-down marquez-logs validate run-once run

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

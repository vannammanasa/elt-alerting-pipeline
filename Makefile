# =============================================================================
# Makefile — Developer Convenience Commands
# Project: An Empirical Evaluation of Severity-Based Alerting in ELT Pipelines
# =============================================================================
# Usage:
#   make up           — Start all services
#   make down         — Stop all services
#   make build        — Rebuild the pipeline image
#   make logs         — Tail logs from all containers
#   make baseline     — Run the baseline experiment
#   make smart        — Run the smart alerting experiment
#   make dbt-run      — Execute dbt models inside the pipeline container
#   make dbt-test     — Execute dbt tests inside the pipeline container
#   make psql         — Open a psql shell to the warehouse
#   make clean        — Remove volumes and orphaned containers
# =============================================================================

.PHONY: up down build logs baseline smart dbt-run dbt-test psql clean status

# ── Lifecycle ────────────────────────────────────────────────────────────────

up:
	docker compose up -d
	@echo "✓ Services started. Airflow UI → http://localhost:8080"

down:
	docker compose down

build:
	docker compose build --no-cache pipeline

rebuild: down build up

clean:
	docker compose down -v --remove-orphans
	@echo "✓ All volumes and orphaned containers removed."

status:
	docker compose ps

logs:
	docker compose logs -f --tail=100

# ── Experiment Execution ─────────────────────────────────────────────────────

baseline:
	docker compose run --rm -e EXPERIMENT_MODE=baseline pipeline python -m pipeline.cli run --mode baseline
	@echo "✓ Baseline experiment completed."

smart:
	docker compose run --rm -e EXPERIMENT_MODE=smart pipeline python -m pipeline.cli run --mode smart
	@echo "✓ Smart alerting experiment completed."

# ── dbt ──────────────────────────────────────────────────────────────────────

dbt-run:
	docker compose run --rm pipeline bash -c "cd /app/dbt_project && dbt run"

dbt-test:
	docker compose run --rm pipeline bash -c "cd /app/dbt_project && dbt test"

dbt-debug:
	docker compose run --rm pipeline bash -c "cd /app/dbt_project && dbt debug"

# ── Database Access ──────────────────────────────────────────────────────────

psql:
	docker compose exec postgres psql -U pipeline_user -d elt_warehouse

psql-obs:
	docker compose exec postgres psql -U pipeline_user -d observability

# ── Testing ──────────────────────────────────────────────────────────────────

test:
	docker compose run --rm pipeline python -m pytest tests/ -v --tb=short

# ── Reporting ────────────────────────────────────────────────────────────────

report:
	docker compose run --rm pipeline python -m pipeline.cli report --output /app/reports/
	@echo "✓ Report generated in ./reports/"

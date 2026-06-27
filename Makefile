

.PHONY: help up down logs ps clean install setup fmt lint test \
        seed connector gen bronze load dbt dbt-test dbt-snapshot features api health recon demo

help:
	@echo "infra:"
	@echo "  setup      - install python deps + dbt (run inside your 3.11 venv)"
	@echo "  up         - start all docker services"
	@echo "  down       - stop all docker services"
	@echo "  ps         - show running services"
	@echo "  logs       - tail logs (use SVC=kafka to filter)"
	@echo "  clean      - stop services and remove volumes"
	@echo ""
	@echo "setup (run once after 'up'):"
	@echo "  seed       - populate Postgres reference tables (users, merchants)"
	@echo "  connector  - register the Debezium Postgres CDC connector"
	@echo ""
	@echo "pipeline:"
	@echo "  gen        - run transaction generator (ctrl+c to stop)"
	@echo "  bronze     - run spark bronze ingestion (ctrl+c to stop)"
	@echo "  load       - load bronze parquet into postgres"
	@echo "  dbt        - run dbt snapshot + run + test"
	@echo "  dbt-test   - run dbt tests only"
	@echo "  features   - load features from postgres to redis"
	@echo "  api        - start the feature serving api"
	@echo "  demo       - run a complete end-to-end data pipeline simulation"
	@echo ""
	@echo "checks:"
	@echo "  health     - check api health endpoint"
	@echo "  recon      - check reconciliation status"
	@echo "  test       - run python unit tests"
	@echo "  lint / fmt - ruff lint / format"

# --- infra ---

up:
	@docker compose up -d > /dev/null 2>&1
	@echo "Docker services started."
	@echo "Next: make seed && make connector"

down:
	docker compose down

ps:
	docker compose ps

logs:
ifdef SVC
	docker compose logs -f $(SVC)
else
	docker compose logs -f
endif

clean:
	docker compose down -v
	@echo "all containers and volumes removed."

# --- setup ---

seed:
	.venv/bin/python -m ingestion.transaction_generator.src.seed_reference

connector:
	./infra/debezium/register-connector.sh

# --- pipeline ---

gen:
	.venv/bin/python -m ingestion.transaction_generator.src.run --firehose

bronze:
	.venv/bin/python -m streaming.spark.src.bronze_ingest

load:
	.venv/bin/python load_bronze_to_postgres.py

dbt:
	cd warehouse/dbt/fraud_warehouse && ../../../.venv/bin/dbt snapshot && ../../../.venv/bin/dbt run && ../../../.venv/bin/dbt test

dbt-test:
	cd warehouse/dbt/fraud_warehouse && ../../../.venv/bin/dbt test

dbt-snapshot:
	cd warehouse/dbt/fraud_warehouse && ../../../.venv/bin/dbt snapshot

features:
	.venv/bin/python -m feature_store.src.loader

api:
	@echo "Starting FastAPI on http://localhost:8002"
	@echo "API Docs: http://localhost:8002/docs (No auth required)"
	.venv/bin/uvicorn feature_store.src.api:app --reload --port 8002

demo:
	@bash scripts/run_demo.sh

# --- checks ---

health:
	@curl -s localhost:8002/health | .venv/bin/python -m json.tool

recon:
	@docker compose exec postgres psql -U fraud_admin -d fraud_reference -c \
		"SELECT recon_status, bronze_total, silver_total, unaccounted_records FROM silver_data_quality.recon_bronze_silver;"

install:
	.venv/bin/pip install -e ".[dev]"

setup:
	@echo "Creating Python 3.11 virtual environment..."
	python3.11 -m venv --clear .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -e ".[dev]"
	.venv/bin/pip install "dbt-core==1.8.*" "dbt-postgres==1.8.2"
	@echo ""
	@echo "Setup complete! Please activate your environment by running:"
	@echo "  source .venv/bin/activate"
	@echo ""
	@echo "Next: make up"

fmt:
	.venv/bin/ruff format .

lint:
	.venv/bin/ruff check .

test:
	.venv/bin/pytest -v
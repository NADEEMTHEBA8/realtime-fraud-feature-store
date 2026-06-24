.PHONY: help up down logs ps clean install setup fmt lint test \
        seed connector gen bronze load dbt dbt-test dbt-snapshot features api health recon

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
	@echo ""
	@echo "checks:"
	@echo "  health     - check api health endpoint"
	@echo "  recon      - check reconciliation status"
	@echo "  test       - run python unit tests"
	@echo "  lint / fmt - ruff lint / format"

# --- infra ---

up:
	docker compose up -d
	@echo "Kafka UI:  http://localhost:8080"
	@echo "MinIO UI:  http://localhost:9001"
	@echo "Airflow:   http://localhost:8081"
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
	python -m ingestion.transaction_generator.src.seed_reference

connector:
	./infra/debezium/register-connector.sh

# --- pipeline ---

gen:
	python -m ingestion.transaction_generator.src.run

bronze:
	python -m streaming.spark.src.bronze_ingest

load:
	python load_bronze_to_postgres.py

dbt:
	cd warehouse/dbt/fraud_warehouse && dbt snapshot && dbt run && dbt test

dbt-test:
	cd warehouse/dbt/fraud_warehouse && dbt test

dbt-snapshot:
	cd warehouse/dbt/fraud_warehouse && dbt snapshot

features:
	python -m feature_store.src.loader

api:
	uvicorn feature_store.src.api:app --reload --port 8000

# --- checks ---

health:
	@curl -s localhost:8000/health | python -m json.tool

recon:
	@docker compose exec postgres psql -U fraud_admin -d fraud_reference -c \
		"SELECT recon_status, bronze_total, silver_total, unaccounted_records FROM silver_data_quality.recon_bronze_silver;"

install:
	pip install -e ".[dev]"

setup:
	@python -c "import sys; v=sys.version_info; \
	exit(0) if (v.major,v.minor)==(3,11) else (print('ERROR: need Python 3.11, found %d.%d. Activate your venv: source .venv/bin/activate' % (v.major,v.minor)) or exit(1))"
	pip install -e ".[dev]"
	pip install "dbt-core==1.8.*" "dbt-postgres==1.8.2"
	@echo ""
	@echo "Setup complete. Verify with: dbt --version"
	@echo "Next: make up"

fmt:
	ruff format .

lint:
	ruff check .

test:
	pytest -v
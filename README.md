# Realtime Fraud Feature Store

An event-driven data pipeline designed to ingest transaction data, aggregate historical fraud features, and serve them to a machine learning model in milliseconds.

This repository serves as a portfolio prototype focusing strictly on the data engineering infrastructure required to support real-time fraud detection. It simulates an Indian fintech environment where transactions flow through Kafka and are processed by Spark Structured Streaming into a Delta Lake. 

From there, dbt transforms the raw events into a dimensional model, computing critical fraud signals—such as 24-hour transaction velocity, failure rates, and merchant category diversity—over rolling time windows. Finally, these aggregated feature vectors are pushed into Redis, allowing a FastAPI service to retrieve a user's entire transaction history in under 10ms to make an approve/block decision during a live checkout flow.

## Architecture

```text
transaction generator ──▶ Kafka (transactions.raw)
                            │
               Databricks (Spark Structured Streaming)
                            │
               Delta Lake (Lakehouse)  s3a://bronze/
                            │
                   load_bronze_to_postgres.py
                            │
                   dbt: staging ──▶ enriched ──▶ gold features
                            │
                   feature loader (Postgres ──▶ Redis)
                            │
                          FastAPI  (feature lookups)

Reference data (users, merchants):
   Postgres tables ──▶ Debezium ──▶ Kafka CDC topics (fraud_cdc.*)
   dbt reads the tables directly; snapshots track SCD Type 2 history.

Orchestration:
   Airflow DAG runs dbt snapshot ──▶ run ──▶ test ──▶ feature load
```

## Key Features

- **AWS Deployment Ready**: Runs locally via Docker Compose, but includes Terraform modules (`infra/terraform-aws-freetier`) to provision S3, RDS PostgreSQL, ElastiCache Redis, and EC2.
- **Change Data Capture**: Debezium captures inserts and updates to user and merchant profiles from the Postgres WAL.
- **Data Governance**: Spark Structured Streaming hashes `device_id` and `ip_address` before data lands in the bronze layer. dbt runs tests and reconciliation checks between the bronze and silver layers.
- **Batch Backfilling**: Includes PyArrow scripts to generate large historical datasets directly to local storage, bypassing the Kafka stream.

## Stack

- **Kafka** (KRaft mode) — event transport
- **PySpark** — Structured Streaming bronze ingestion
- **Delta Lake (MinIO)** — Object storage
- **PostgreSQL** — Local warehouse stand-in (dbt-postgres adapter)
- **dbt** — SQL transformations, tests, SCD2 snapshots
- **Debezium** — CDC from Postgres WAL
- **Redis** — Feature serving store
- **FastAPI** — Feature lookup API
- **Airflow** — Batch orchestration
- **Docker Compose** — Local execution
- **Terraform** — AWS infrastructure provisioning

*Note: Postgres is used as a local stand-in for a cloud warehouse. The dbt SQL can be ported to Databricks SQL or Snowflake with minor profile changes.*

## How CDC works here

`public.users` and `public.merchants` live in Postgres. Debezium captures inserts and updates from the WAL and publishes them to `fraud_cdc.public.*` Kafka topics.

Currently, dbt reads the reference tables directly from Postgres to build SCD Type 2 history via snapshots. Wiring a Kafka sink connector back into the warehouse to consume the topics is the natural next step, but is out of scope for this prototype.

## What's in the warehouse

- 8 dbt models: 3 staging views, 1 enriched intermediate table, 2 gold feature tables, 2 data-quality tables.
- 2 snapshots: SCD Type 2 on the users and merchants dimensions.
- Schema and data tests on every model: uniqueness, not-null, accepted-values, range checks, and reconciliation assertions.
- ~25 per-user fraud features across 1h / 24h / 7d windows.

## Project layout

```text
ingestion/transaction_generator/src/   transaction generator + historical backfill
streaming/spark/src/                   Kafka -> MinIO bronze ingestion + PII masking
warehouse/dbt/fraud_warehouse/         dbt models, snapshots, tests, macros
feature_store/src/                     Postgres->Redis loader + FastAPI app
orchestration/airflow/dags/            batch pipeline DAG + dbt profile
infra/postgres/init/                   reference table DDL + CDC publication
infra/debezium/                        Debezium connector config + register script
infra/terraform-aws-freetier/          Terraform AWS architecture for cloud deployment
load_bronze_to_postgres.py             bridges MinIO Parquet into Postgres
```

## Running it

Requires Docker (allow ~6 CPUs / 10GB RAM), Python 3.11, Java 17 (for PySpark), and Make.

```bash
# Set up the Python virtual environment and dependencies
make setup
source .venv/bin/activate

# Recommended: Run the entire end-to-end pipeline automatically
make demo

# --- Or run the individual steps manually ---
# 1. start services and wait for health checks
make up                 

# 2. one-time setup
make seed               # populate public.users / public.merchants
make connector          # register the Debezium CDC connector

# 3. ingestion (run, then ctrl+c after ~30s)
make gen                # generate events into Kafka

# 4. streaming (run, then ctrl+c once the backlog is drained)
make bronze             # Spark: Kafka -> MinIO bronze Parquet

# 5. warehouse
make load               # MinIO Parquet -> Postgres bronze.transactions
make dbt                # dbt snapshot + run + test

# 6. serving
make features           # Postgres gold -> Redis
make api                # FastAPI on :8002
```

To test the API, grab a valid user ID from the database and query the endpoint:

```bash
docker compose exec postgres psql -U fraud_admin -d fraud_reference -c \
  "SELECT user_id FROM silver_gold.gold_user_fraud_features LIMIT 1;"

curl -H "X-API-Key: sk_test_123" localhost:8002/v1/features/user/<user_id> | python -m json.tool
```

The Airflow UI is available at `http://localhost:8081` (credentials are printed at the end of the `make demo` output). `dbt test` acts as a gate in the DAG; if tests fail, the Redis feature load is skipped.

## Design notes

- **Reconciliation**: `recon_bronze_silver` asserts `bronze_total = silver_total + filtered_count`. A non-zero `unaccounted_records` fails a dbt test.
- **SCD Type 2**: Updating a merchant's `risk_tier` in Postgres and re-running `dbt snapshot` creates multiple rows with validity timestamps. This tracks risk tier history over time.
- **NUMERIC for money**: Amounts are cast to `NUMERIC(12,2)`. Float arithmetic is not used for currency.
- **Redis for serving**: Feature lookups require single-digit-millisecond reads. Redis provides this consistently.

## Architectural Trade-offs & Known Limitations

- **Kafka Partitioning**: The `transactions.raw` topic uses a single partition to guarantee global ordering for this prototype. This prevents horizontal scaling of the Spark streaming job.
- **Cold Start**: The API returns `404 Not Found` for new users instead of a default feature vector.
- **Batch Features**: Features are calculated in batch via dbt. A streaming feature processor (e.g., Flink) is required to catch real-time velocity spikes.
- **Data Privacy**: Transaction data currently flows through Kafka and Redis in plain text. PII tokenization is required for production compliance.
- No TLS or production-grade auth.
- Credentials are hardcoded local defaults.
- No automated CI/CD.

---

Nadeem Theba — nadeemtheba8@gmail.com
MSc Data Science, University of Hertfordshire

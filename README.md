# realtime-fraud-feature-store

Local event-driven fraud analytics prototype. Payment events flow through
Kafka, get processed by Spark into a medallion layout, are modeled in dbt,
and the resulting per-user fraud features are served from Redis behind a
FastAPI endpoint.

This is a portfolio prototype, not a production system. The goal was to
build the data infrastructure that *feeds* a fraud model — ingestion,
streaming, modeling, feature serving, orchestration — not the model itself.

## Architecture

```text
transaction generator ──▶ Kafka (transactions.raw)
                            │
                   Spark Structured Streaming
                            │
                   MinIO  s3a://bronze/  (partitioned Parquet)
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

## Enterprise Features

- **Cloud-Agnostic Design**: Built locally with Docker Compose, but includes AWS Terraform modules (`infra/terraform-aws-freetier`) to provision S3, RDS PostgreSQL, ElastiCache Redis, and EC2 with zero-cost free tier components.
- **Debezium CDC**: Zero-downtime database extraction capturing real-time changes to user and merchant profiles directly from the Postgres WAL.
- **Data Governance**: PII is masked at the edge. Spark structured streaming jobs apply SHA-256 hashing to `device_id` and `ip_address` before data lands in the bronze data lake. dbt applies strict data-quality assertions, including full reconciliation tests between bronze and silver layers.
- **High Volume Ingestion**: Supports streaming ingestion at scale via a dedicated Firehose mode (`--firehose` flag). Also includes PyArrow-backed local partitioned Parquet backfilling for generating historical datasets (e.g., millions of records) directly to storage.

## Stack

- **Kafka** (KRaft mode, no Zookeeper) — event transport
- **PySpark** — Structured Streaming bronze ingestion
- **MinIO** — S3-compatible object storage for Parquet
- **PostgreSQL** — warehouse (dbt-postgres adapter)
- **dbt** — transformations, tests, SCD2 snapshots
- **Debezium** — CDC from the Postgres WAL
- **Redis** — feature serving store
- **FastAPI** — feature lookup API
- **Airflow** — batch orchestration (standalone, SequentialExecutor)
- **Docker Compose** — runs the stack (7 long-running services plus a
  one-shot MinIO bucket initializer)
- **Terraform** — Infrastructure as Code for AWS deployment

Postgres stands in for a cloud warehouse. The dbt SQL is standard enough
that swapping the adapter (e.g. to BigQuery) is mostly a profile change,
though it has not been tested against another warehouse.

## How CDC works here

`public.users` and `public.merchants` live in Postgres and are the CDC
source. Debezium captures inserts and updates from the WAL and publishes
them to `fraud_cdc.public.*` Kafka topics — visible in the Kafka UI.

dbt reads those reference tables **directly** from Postgres (same instance,
local dev) rather than consuming the CDC topics; there is no Kafka→warehouse
sink in this prototype. The dbt snapshots provide the SCD Type 2 history.
The CDC pipeline is real and observable; wiring a sink connector back into
the warehouse is the natural next step but is out of scope here.

## What's in the warehouse

- 8 dbt models: 3 staging views, 1 enriched intermediate table,
  2 gold feature tables, 2 data-quality tables
- 2 snapshots — SCD Type 2 on the users and merchants dimensions
- Schema and data tests on every model: uniqueness, not-null,
  accepted-values, plus custom range and reconciliation assertions
- ~25 per-user fraud features (velocity, amount stats, merchant/payment
  diversity, failure and refund rates, city diversity, late-night activity,
  latest-amount z-score) across 1h / 24h / 7d windows

## Project layout

```
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

Requires Docker (allow it ~6 CPUs / 10GB RAM), Python 3.11, and Java 17
for Spark. Spark runs on the host; everything else runs in containers.

```bash
pip install -e ".[dev]"
pip install dbt-postgres==1.8.2

# 1. start services
make up                 # wait for services to report healthy

# 2. one-time setup
make seed               # populate public.users / public.merchants
make connector          # register the Debezium CDC connector

# 3. ingestion (run, then ctrl+c after ~30s)
make gen                # generate events into Kafka
# Or generate events aggressively:
python -m ingestion.transaction_generator.src.run --firehose

# 4. streaming (run, then ctrl+c once the backlog is drained)
make bronze             # Spark: Kafka -> MinIO bronze Parquet

# 5. historical backfill (bypasses Kafka)
python -m ingestion.transaction_generator.src.backfill --rows 1000000

# 6. warehouse
make load               # MinIO Parquet -> Postgres bronze.transactions
make dbt                # dbt snapshot + run + test

# 7. serving
make features           # Postgres gold -> Redis
make api                # FastAPI on :8000
```

Pick a real user id from the warehouse, then query it:

```bash
docker compose exec postgres psql -U fraud_admin -d fraud_reference -c \
  "SELECT user_id FROM silver_gold.gold_user_fraud_features LIMIT 1;"

curl localhost:8000/features/user/<user_id> | python -m json.tool
```

The Airflow DAG (`fraud_feature_pipeline`, http://localhost:8081) runs the
batch half — snapshot, run, test, feature load — on a 4-hour schedule.
`dbt test` is a gate: if it fails, the feature load is skipped and the
previous features stay in Redis until their TTL expires.

## Design notes

- **Reconciliation** — `recon_bronze_silver` asserts
  `bronze_total = silver_total + filtered_count`. A non-zero
  `unaccounted_records` fails a dbt test. The generator emits clean data,
  so the filter path is exercised mainly by the validation rules in
  `stg_transactions`.
- **SCD Type 2** — update a merchant's `risk_tier` (e.g. MEDIUM → HIGH) in
  Postgres, re-run `dbt snapshot`, and the snapshot table shows two rows
  with validity timestamps. This answers "what was the risk tier *at the
  time* of the transaction".
- **NUMERIC for money** — amounts are cast to `NUMERIC(12,2)`; float
  arithmetic is not acceptable for currency.
- **Redis for serving** — feature lookups need single-digit-millisecond
  reads, which Redis gives consistently and a SQL query against the
  warehouse does not.

## Architectural Trade-offs & Known Limitations

- **Kafka Partitioning Bottleneck**: The `transactions.raw` topic is currently configured with a single partition. This was a deliberate choice for this local prototype to guarantee strict global ordering, but it severely limits consumer parallelism and prevents horizontal scaling of the Spark Structured Streaming job.
- No auth or TLS — everything is plaintext, local only
- Credentials are hardcoded local defaults
- No monitoring, alerting, or CI/CD
- Single Kafka partition — fine locally, would bottleneck under load
- No Kafka→warehouse sink for CDC; dbt reads reference tables directly
- Generator produces clean data; the validation/recon paths are not
  stress-tested with deliberately malformed input
- Feature serving has not been load-tested; no latency percentiles measured

---

Nadeem Theba — nadeemtheba8@gmail.com
MSc Data Science, University of Hertfordshire

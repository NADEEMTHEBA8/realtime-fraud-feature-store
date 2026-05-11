# realtime-fraud-feature-store

Fintech transaction pipeline with a fraud feature store. Takes payment events from Kafka, runs them through a medallion architecture in dbt, and serves precomputed fraud features from Redis in under 1ms.

I built this to understand how companies like Razorpay and PhonePe structure their data platforms — specifically the infrastructure that feeds fraud models, not the ML model itself.

## What it does

Payment events go into Kafka → Spark writes them as Parquet to MinIO (bronze layer) → dbt transforms and validates them in Postgres (silver/gold) → a loader pushes computed features to Redis → FastAPI serves them.

Debezium handles CDC from Postgres for merchant and user reference data. Airflow ties the batch side together.

```
Kafka ──▶ Spark ──▶ MinIO (parquet)
                        │
                   load to Postgres
                        │
              dbt (staging → enriched → features)
                        │
                   loader script
                        │
                      Redis ──▶ FastAPI (<1ms)

Separately:
  Debezium watches Postgres ──▶ Kafka CDC topics
  Airflow runs: snapshot → dbt run → dbt test → feature load
```

## Stack

- **Kafka** (KRaft, no Zookeeper) — message broker
- **PySpark** — streaming bronze ingestion
- **MinIO** — S3-compatible object storage for parquet
- **PostgreSQL** — warehouse (using dbt-postgres, SQL is portable to BigQuery)
- **dbt** — transformations, tests, snapshots
- **Debezium** — CDC from Postgres WAL
- **Redis** — feature serving
- **FastAPI** — feature API
- **Airflow** — batch orchestration
- **Docker Compose** — runs all 7 services

I originally planned to use BigQuery but couldn't get a GCP account set up (card issues). Postgres works fine — the SQL is ANSI standard so swapping the dbt adapter is a config change.

## Numbers

- 7 containers running locally
- 8 dbt models (3 staging views, 3 gold tables, 2 data quality)
- 2 dbt snapshots (SCD Type 2 on merchants and users)
- 53 dbt tests passing
- 25+ fraud features per user (velocity, amounts, diversity, failure rates — across 1h/24h/7d windows)
- Feature lookup from Redis: roughly 1ms on my MacBook Air

## Project layout

```
ingestion/transaction_generator/src/   # generates realistic payment events, publishes to kafka
streaming/spark/src/                   # reads kafka, writes parquet to minio
warehouse/dbt/fraud_warehouse/         # all dbt models, snapshots, tests, macros
feature_store/src/                     # loader (postgres→redis) and fastapi app
orchestration/airflow/dags/            # pipeline DAG
infra/docker/postgres/                 # reference table SQL
docs/adr/                             # some notes on why I made certain choices
```

## Running it

You need Docker Desktop (give it 6+ CPUs and 10GB RAM), Python 3.11, and Java 17 (Spark needs it).

```bash
# start everything
docker compose up -d
docker compose ps   # should show 7 healthy containers

# generate some events (~30 sec, then ctrl+c)
python -m ingestion.transaction_generator.src.run

# run spark bronze ingestion (~90 sec, then ctrl+c)
python -m streaming.spark.src.bronze_ingest

# load bronze data into postgres
python load_bronze_to_postgres.py

# run dbt
cd warehouse/dbt/fraud_warehouse
dbt snapshot && dbt run && dbt test

# load features to redis and start api
cd ../../..
python -m feature_store.src.loader
uvicorn feature_store.src.api:app --port 8000

# test it
curl localhost:8000/features/user/user_2765df9165 | python -m json.tool
```

## Things I'm happy with

**Reconciliation** — there's a dbt model that checks `bronze_count = silver_count + filtered_count`. If it doesn't add up, the test fails. Currently 2,617 = 2,617 + 0 (the generator makes clean data, but the filtering logic is there for dirty data).

**SCD Type 2** — I updated a merchant's risk tier from 'medium' to 'high', re-ran `dbt snapshot`, and got two rows with validity timestamps. Fraud investigators need to know what the risk tier was *at the time* of the transaction, not just what it is now.

**Quality gate** — Airflow runs dbt tests before loading features. If tests fail, the loader doesn't run. Old features stay in Redis until TTL expires. I'd rather serve slightly stale features than broken ones.

**The features themselves** — 25+ per user: transaction counts per hour/day/week, amount stats, how many unique merchants they hit, failure rates, refund rates, whether they're transacting from unusual cities, late-night activity, z-score on latest transaction amount. These are the kinds of signals real fraud systems use.

## What's missing (and I know it)

- No auth on the API — wide open right now
- No TLS anywhere — everything is plaintext
- Credentials are hardcoded defaults (fine for local, obviously not for prod)
- No monitoring or alerting
- No CI/CD
- Single Kafka partition (would bottleneck at scale)
- The reconciliation hasn't been stress-tested with intentionally bad data
- The latency number is from a single curl, not a proper benchmark with percentiles

## API

| Endpoint | What it does |
|----------|-------------|
| `GET /health` | Feature freshness + Redis status |
| `GET /features/user/{id}` | All fraud features for one user |
| `GET /features/merchant/{id}` | Latest daily stats for a merchant |
| `POST /features/batch` | Bulk lookup (up to 100 users) |
| `GET /docs` | Swagger UI |

## Why I made certain choices

| Choice | Why |
|--------|-----|
| KRaft Kafka | Didn't want to run Zookeeper too. It's deprecated anyway. |
| Spark on host not Docker | My MacBook has 16GB. Docker was already using ~4GB. Spark in Docker would've killed it. |
| Downloaded JARs manually | `--packages` pulls from Maven at runtime. Broke on me once, never again. |
| Postgres not BigQuery | GCP wouldn't let me create an account. The SQL is the same either way. |
| NUMERIC for money | Float arithmetic is wrong for currency. Not debatable. |
| Redis for serving | Need sub-10ms for fraud scoring. Postgres can't do that consistently. |
| dbt tests before feature load | If the data is bad, don't serve it. Let the old features expire naturally. |

---

Nadeem Theba — nadeemtheba8@gmail.com
MSc Data Science, University of Hertfordshire

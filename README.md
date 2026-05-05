# Real-Time Fraud Feature Store

A production-grade fintech data platform that ingests payment events through Kafka, enriches them with rolling fraud features via dbt's medallion architecture, and serves features through a sub-millisecond Redis-backed API — mimicking the infrastructure behind PhonePe, Razorpay, and CRED.

> **This project builds the data infrastructure that feeds a fraud model — not the ML model itself.**

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          AIRFLOW (Orchestrator)                          │
│    dbt_snapshot  →  dbt_run  →  dbt_test (gate)  →  feature_loader      │
└────────────────────────────┬─────────────────────────────────────────────┘
                             │
    ┌───────────┐      ┌─────▼──────┐      ┌──────────┐      ┌──────────┐
    │   Kafka   │─────▶│   Spark    │─────▶│  MinIO   │      │  Redis   │
    │  (Events) │      │  (Bronze)  │      │(Parquet) │      │(Features)│
    └─────┬─────┘      └────────────┘      └──────────┘      └────▲─────┘
          │                                                       │
    ┌─────▼─────┐      ┌────────────────────────────────┐   ┌────┴─────┐
    │Transaction│      │         PostgreSQL              │   │ FastAPI  │
    │ Generator │      │   bronze → silver → gold        │   │  (API)   │
    └───────────┘      │  (dbt medallion architecture)   │   │  <1ms    │
                       └────────────┬───────────────────┘   └──────────┘
    ┌───────────┐                   │
    │ Debezium  │──▶ CDC: merchants, users (real-time sync)
    │   (CDC)   │
    └───────────┘
```

---

## The 60-Second Pitch

> *"I built a fintech transaction pipeline that ingests payment events through Kafka and CDC, enriches them with rolling fraud features via dbt's medallion architecture, and serves features to a real-time scoring API via Redis in under 1 millisecond. Airflow orchestrates the batch layer with data quality gates — if any of the 53 dbt tests fail, the feature loader is blocked, ensuring the fraud API never serves corrupt data. The hard problem I focused on was reconciliation between the streaming and batch views of the same transaction."*

---

## Key Metrics

| Metric | Value |
|--------|-------|
| Docker services | **7** (Kafka, Spark, Postgres, Redis, MinIO, Debezium, Airflow) |
| dbt models | **8** (3 silver views + 3 gold tables + 2 data quality) |
| dbt snapshots | **2** (SCD Type 2 for merchants + users) |
| Data quality tests | **53** (all passing) |
| Fraud features | **25+** per user across 1h/24h/7d rolling windows |
| Feature serving latency | **<1ms** (measured: 0.998ms) |
| Reconciliation | **BALANCED** — zero unaccounted records |
| Airflow DAG tasks | **4** (snapshot → run → test → load) |

---

## Tech Stack

| Component | Technology | Why This Choice |
|-----------|-----------|----------------|
| Streaming | Kafka (KRaft mode) | Industry standard, per-user ordering via partition keys |
| CDC | Debezium + Kafka Connect | WAL-based, zero source DB impact, sub-second latency |
| Stream Processing | PySpark Structured Streaming | Exactly-once semantics, checkpoint-based recovery |
| Data Lake | MinIO (S3-compatible) | Parquet storage, locally mimics GCS/S3 |
| Warehouse | PostgreSQL | ANSI SQL, dbt-portable to BigQuery/Snowflake |
| Transformations | dbt-core | Medallion architecture, 53 data quality tests |
| Feature Store | Redis | Sub-millisecond serving for real-time scoring |
| Feature API | FastAPI | Async, auto-generated OpenAPI docs |
| Orchestration | Airflow 2.x | DAG-based scheduling with quality gates |
| Containerization | Docker Compose | 7 services, health checks, named volumes |

---

## Key Features

### Medallion Architecture (Bronze → Silver → Gold)

- **Bronze:** Raw JSON events preserved as Parquet on MinIO with Kafka metadata for lineage
- **Silver:** Type-cast, validated, filtered — 3 staging views with schema enforcement
- **Gold:** Enriched transactions (3-way join), 25+ rolling fraud features, daily merchant stats

### 25+ Fraud Features

| Category | Features | What It Catches |
|----------|----------|----------------|
| **Velocity** | txn_count_1h, txn_count_24h, txn_count_7d | Card testing, bot attacks |
| **Amount** | txn_sum, txn_avg, txn_max per window | Account compromise |
| **Diversity** | unique_merchants, unique_cities, payment_methods | Stolen card testing |
| **Failure** | failed_txn_count_24h, failure_rate_24h | Rapid card number testing |
| **Refunds** | refund_count_7d, refund_rate_7d | Refund fraud |
| **Temporal** | late_night_txn_count, z-score deviation | Off-hours activity |

All features computed across **1h / 24h / 7d rolling windows**.

### CDC with Debezium

Merchant risk tier and user KYC level changes stream from PostgreSQL WAL to Kafka in sub-second latency. No polling, no batch dumps, zero source database impact.

### SCD Type 2

Historical dimension tracking via dbt snapshots. When a merchant's risk tier changes from `low` to `critical`, both the before and after states are preserved with validity timestamps:

```
merchant_id    | risk_tier | valid_from          | valid_to
merch_8ec4...  | medium    | 2026-04-29 19:09    | 2026-04-30 12:16  ← closed
merch_8ec4...  | high      | 2026-04-30 12:16    | NULL              ← current
```

### Reconciliation

Every record is accounted for across pipeline layers:

```
Pipeline Status : BALANCED
Bronze          : 2,617
Silver          : 2,617
Filtered        : 0
Unaccounted     : 0
```

### Data Quality Gates

53 dbt tests act as a gate in Airflow — feature loading is blocked if any test fails. Tests include: `unique`, `not_null`, `accepted_values`, `positive_value`, `not_future_timestamp`, `value_in_range`, and reconciliation checks.

### Sub-Millisecond Feature Serving

FastAPI + Redis serves 25+ fraud features per user in **<1ms**. Batch endpoint uses Redis MGET for single-round-trip multi-user lookups. Health endpoint monitors feature freshness with TTL-based staleness detection.

---

## Project Structure

```
realtime-fraud-feature-store/
├── docker-compose.yml                 # 7 services
├── ingestion/
│   └── transaction_generator/src/     # Pydantic schemas, Kafka producer
│       ├── schemas.py                 # TransactionEvent with Decimal validation
│       ├── profiles.py                # 5K users, 500 merchants, realistic distributions
│       ├── generator.py               # Log-normal amounts, hour-of-day weights
│       ├── kafka_producer.py          # acks=all, user_id partition key
│       └── run.py                     # Entry point (10 TPS default)
├── streaming/
│   └── spark/
│       ├── jars/                      # Pre-downloaded Kafka + S3A JARs (gitignored)
│       └── src/
│           ├── config.py              # SparkSession factory (Kafka + MinIO)
│           └── bronze_ingest.py       # Kafka → Parquet with dead-letter routing
├── warehouse/
│   └── dbt/fraud_warehouse/
│       ├── snapshots/                 # SCD Type 2 (snp_merchants, snp_users)
│       ├── macros/                    # 3 custom generic tests
│       └── models/
│           ├── staging/               # stg_transactions, stg_merchants, stg_users
│           ├── intermediate/          # int_transactions_enriched (3-way join)
│           ├── gold/                  # gold_user_fraud_features, gold_daily_merchant_stats
│           └── data_quality/          # recon_bronze_silver, dq_pipeline_health
├── feature_store/
│   └── src/
│       ├── loader.py                  # Postgres → Redis (pipelined writes)
│       └── api.py                     # FastAPI with 4 endpoints
├── orchestration/
│   └── airflow/dags/
│       ├── fraud_pipeline_dag.py      # 4-task DAG with quality gate
│       └── profiles.yml               # dbt connection for Airflow container
├── infra/docker/postgres/             # Reference table init SQL
├── docs/
│   ├── adr/                           # 7 Architecture Decision Records
│   └── day*-recap.md                  # Daily build recaps with interview prep
└── load_bronze_to_postgres.py         # One-time bronze data loader
```

---

## Quick Start

### Prerequisites

- Docker Desktop (6+ CPUs, 10GB RAM)
- Python 3.11 with venv
- Java 17 (for PySpark)

### 1. Start Infrastructure

```bash
git clone https://github.com/NADEEMTHEBA8/realtime-fraud-feature-store.git
cd realtime-fraud-feature-store
python3.11 -m venv .venv && source .venv/bin/activate
pip install pyspark==3.5.1 dbt-postgres==1.8.2 fastapi uvicorn redis psycopg2-binary pandas
docker compose up -d
docker compose ps             # Verify all 7 containers healthy
```

### 2. Generate Transaction Events

```bash
python -m ingestion.transaction_generator.src.run    # Ctrl+C after ~30 seconds
```

### 3. Run Bronze Ingestion (Spark)

```bash
python -m streaming.spark.src.bronze_ingest          # Ctrl+C after ~90 seconds
```

### 4. Load Bronze Data to Postgres

```bash
python load_bronze_to_postgres.py
```

### 5. Run dbt Pipeline

```bash
cd warehouse/dbt/fraud_warehouse
dbt snapshot && dbt run && dbt test    # 2 snapshots, 8 models, 53 tests
```

### 6. Load Features to Redis & Start API

```bash
cd ../../..
python -m feature_store.src.loader
uvicorn feature_store.src.api:app --port 8000
```

### 7. Query Features

```bash
# Health check with feature freshness
curl http://localhost:8000/health | python -m json.tool

# Get fraud features for a user (<1ms)
curl http://localhost:8000/features/user/user_2765df9165 | python -m json.tool

# Batch lookup
curl -X POST http://localhost:8000/features/batch \
  -H "Content-Type: application/json" \
  -d '{"user_ids": ["user_2765df9165", "user_99389bd371"]}' | python -m json.tool

# Swagger UI
open http://localhost:8000/docs
```

---

## API Endpoints

| Endpoint | Method | Description | Latency |
|----------|--------|-------------|---------|
| `/health` | GET | Pipeline health + feature freshness | <1ms |
| `/features/user/{user_id}` | GET | 25+ fraud features for a user | <1ms |
| `/features/merchant/{merchant_id}` | GET | Latest daily stats for a merchant | <1ms |
| `/features/batch` | POST | Batch lookup for up to 100 users | <2ms |
| `/docs` | GET | Interactive Swagger UI | — |

---

## Data Quality Tests

| Category | Count | Examples |
|----------|-------|---------|
| Uniqueness | 5 | transaction_id, user_id, merchant_id |
| Not-null | 26 | All required columns across all models |
| Accepted values | 8 | transaction_type, status, payment_method, currency |
| Positive value (custom) | 6 | amount, txn_count, total_amount |
| Value in range (custom) | 4 | failure_rate (0-1), refund_rate (0-1) |
| No future timestamp (custom) | 1 | event_timestamp |
| Reconciliation | 3 | BALANCED status, zero unaccounted records |
| **Total** | **53** | **All passing** |

---

## Infrastructure Services

| Service | Port | Access |
|---------|------|--------|
| Kafka UI | 8080 | http://localhost:8080 |
| MinIO Console | 9001 | http://localhost:9001 |
| Airflow | 8081 | http://localhost:8081 |
| Feature API | 8000 | http://localhost:8000/docs |
| Kafka Connect (Debezium) | 8083 | REST API |
| PostgreSQL | 5432 | `psql -U fraud_admin -d fraud_reference` |
| Redis | 6379 | `redis-cli` |

---

## Architecture Decision Records

| ADR | Decision | Trade-off |
|-----|----------|-----------|
| [ADR-001](docs/adr/001-kafka-kraft-mode.md) | Kafka in KRaft mode (no Zookeeper) | Simpler ops, fewer containers |
| [ADR-002](docs/adr/002-004-006-combined.md) | PySpark locally, not in Docker | Saves 4GB RAM on 16GB MacBook |
| [ADR-003](docs/adr/002-004-006-combined.md) | Pre-downloaded JARs, not `--packages` | Deterministic builds, offline-capable |
| [ADR-004](docs/adr/002-004-006-combined.md) | Postgres as warehouse (not BigQuery) | Portable SQL, no cloud dependency for dev |
| [ADR-005](docs/adr/005-decimal-not-float.md) | NUMERIC for money, never FLOAT | Precision correctness for fintech |
| [ADR-006](docs/adr/002-004-006-combined.md) | Redis for feature serving | Sub-ms latency for real-time scoring |
| [ADR-007](docs/adr/007-dbt-quality-gate.md) | dbt tests as Airflow quality gate | Corrupt data never reaches serving layer |

---

## The dbt DAG

```
bronze.transactions ──▶ stg_transactions ──┐
                                            ├──▶ int_transactions_enriched ──┬──▶ gold_daily_merchant_stats
public.merchants ──▶ stg_merchants ────────┤                                │
                                            │                                ├──▶ dq_pipeline_health
public.users ──▶ stg_users ────────────────┘                                │
                                                                             │
                        stg_transactions ──────▶ gold_user_fraud_features ──┘
                                                                             │
bronze + silver + enriched + features ─────────▶ recon_bronze_silver ────────┘
```

**8 models • 2 snapshots • 3 sources • 53 tests**

---

## Production Considerations

This is a **portfolio project** demonstrating data engineering patterns. For production deployment, the following would be added:

| Area | What's Missing | Production Solution |
|------|---------------|-------------------|
| **Security** | No API authentication | OAuth2/JWT with API keys, mTLS between services |
| **Encryption** | Plaintext connections | TLS 1.2+ on all connections, encryption at rest |
| **Secrets** | Hardcoded credentials (dev only) | HashiCorp Vault / AWS Secrets Manager |
| **Monitoring** | No observability stack | Prometheus + Grafana for Kafka lag, API latency, feature freshness |
| **Scaling** | Single Kafka partition | Multi-partition topics, Redis Cluster, API replicas + load balancer |
| **CI/CD** | No automated pipeline | GitHub Actions for lint, test, dbt compile, Docker build |
| **Compliance** | Partial PII handling | Full PCI-DSS alignment, data retention policies, audit logging |

---

## Author

**Nadeem Theba**

- GitHub: [NADEEMTHEBA8](https://github.com/NADEEMTHEBA8)
- Email: nadeemtheba8@gmail.com
- Education: MSc Data Science, University of Hertfordshire (UK)

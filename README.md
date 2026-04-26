# Fraud Detection Data Pipeline

A production-grade fintech transaction processing pipeline with a real-time fraud feature store.

## Status
In active development. See `docs/architecture.md` for the design.

## Stack
Kafka, PySpark Structured Streaming, Debezium, BigQuery, dbt, Redis, FastAPI, Airflow, Great Expectations.

## Architecture
See `docs/architecture.md`.

## Local Setup
```bash
make up      # Start the stack
make down    # Tear down
```

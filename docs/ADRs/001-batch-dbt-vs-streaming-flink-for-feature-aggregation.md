# ADR 001: Batch dbt vs. Streaming Flink for Feature Aggregation

## Context
We needed to compute user-centric fraud features (e.g., 7-day velocity, average ticket size) from the raw transaction stream. The choice was between a real-time stream processing framework (Apache Flink) or a batch processing framework (dbt + Postgres).

## Decision
We chose to use **dbt** running in a scheduled batch pipeline (Airflow) to compute features, materializing them into Postgres and serving them via Redis.

## Consequences
- **Positive**: Drastically reduced operational complexity. Feature engineering is democratized via standard SQL, and we avoid the overhead of managing JVM state and watermarks.
- **Negative**: We sacrifice sub-second feature freshness. Features are currently computed on a 4-hour schedule, meaning the model makes predictions based on slightly stale aggregation windows.

# ADR-007: dbt Tests as Airflow Quality Gate

## Status
Accepted

## Context
The pipeline loads fraud features from Postgres gold tables into Redis for real-time serving. If the gold tables contain corrupt data (failed validation, broken joins, schema drift), the fraud scoring model would produce inaccurate risk scores.

## Decision
dbt tests act as a gate in the Airflow DAG: `dbt_run → dbt_test → load_features_to_redis`. If any of the 53 tests fail, the feature loader task is skipped.

## Consequences
**Positive:**
- Corrupt data never reaches the fraud scoring API
- Old (valid) features remain in Redis until the issue is fixed
- The failing test provides diagnostic information (which test, which model)
- Alert fires so the team investigates immediately

**Negative:**
- Features can become stale if the pipeline is broken for hours
- The 24-hour TTL on Redis keys means features eventually expire if not refreshed
- Overly strict tests could block legitimate data (false positives)

**Trade-off accepted:** Stale-but-correct features are strictly better than fresh-but-corrupt features for fraud scoring. A model working with yesterday's features produces slightly degraded but directionally correct scores. A model working with corrupt features produces garbage scores that could block legitimate transactions or approve fraudulent ones.

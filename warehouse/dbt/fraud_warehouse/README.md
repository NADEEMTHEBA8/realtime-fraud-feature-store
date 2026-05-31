# fraud_warehouse (dbt)

Transforms bronze transaction data and CDC-sourced reference data into
silver staging models, an enriched intermediate table, and gold feature
tables.

Layers:
- `staging/`      typed/validated views over bronze + reference sources
- `intermediate/` `int_transactions_enriched` — transactions joined to dims
- `gold/`         user fraud features and daily merchant stats
- `data_quality/` bronze-vs-silver reconciliation and a pipeline health summary
- `snapshots/`    SCD Type 2 history for the users and merchants dimensions

Run locally (Postgres on localhost, profile in this directory):

    dbt snapshot && dbt run && dbt test

/*
    recon_bronze_silver.sql — Reconciliation: Bronze vs Silver

    What this does:
        Compares record counts between the bronze (raw) and silver (cleaned)
        layers. Every record that exists in bronze should either exist in
        silver OR be accounted for as a filtered/invalid record.

    Why reconciliation matters:
        In fintech, every transaction must be accounted for. If bronze has
        10,000 records and silver has 9,500, the 500 missing records need
        an explanation: were they filtered by validation rules? Were they
        lost due to a bug? Regulators and auditors ask this question.

    The formula:
        bronze_count = silver_count + filtered_count
        If this equation doesn't balance, something is wrong.

    Interview talking point:
        "I built a reconciliation model that compares record counts across
        pipeline layers. The hard problem I solved was accounting for every
        record: bronze count must equal silver count plus filtered count.
        If the equation doesn't balance, the pipeline alerts — every
        transaction must be accounted for in fintech."
*/

{{ config(
    materialized='table',
    schema='data_quality'
) }}

with bronze_counts as (

    select
        count(*) as bronze_total,
        count(distinct transaction_id) as bronze_distinct_txn_ids,
        count(*) filter (where transaction_id is null) as bronze_null_txn_ids,
        count(*) filter (where amount is null) as bronze_null_amounts,
        min(event_timestamp) as bronze_min_timestamp,
        max(event_timestamp) as bronze_max_timestamp
    from {{ source('bronze', 'transactions') }}

),

silver_counts as (

    select
        count(*) as silver_total,
        count(distinct transaction_id) as silver_distinct_txn_ids,
        min(event_timestamp) as silver_min_timestamp,
        max(event_timestamp) as silver_max_timestamp
    from {{ ref('stg_transactions') }}

),

-- Records in bronze that were filtered out by silver validation rules
filtered_records as (

    select count(*) as filtered_count
    from {{ source('bronze', 'transactions') }} b
    where
        -- These are the validation rules from stg_transactions.sql
        b.transaction_id is null
        or cast(b.amount as numeric) <= 0
        or b.event_timestamp is null
        or b.transaction_type not in ('PURCHASE', 'REFUND', 'TRANSFER', 'WITHDRAWAL')

),

-- Records in bronze that don't appear in silver (potential data loss)
missing_records as (

    select count(*) as missing_count
    from {{ source('bronze', 'transactions') }} b
    left join {{ ref('stg_transactions') }} s
        on b.transaction_id = s.transaction_id::text
    where s.transaction_id is null
        -- Exclude records we intentionally filtered
        and b.transaction_id is not null
        and cast(b.amount as numeric) > 0
        and b.event_timestamp is not null
        and b.transaction_type in ('PURCHASE', 'REFUND', 'TRANSFER', 'WITHDRAWAL')

),

enriched_counts as (

    select
        count(*) as enriched_total,
        count(*) filter (where merchant_name is not null) as enriched_with_merchant,
        count(*) filter (where user_kyc_level is not null) as enriched_with_user,
        count(*) filter (where merchant_name is null) as enriched_missing_merchant,
        count(*) filter (where user_kyc_level is null) as enriched_missing_user
    from {{ ref('int_transactions_enriched') }}

),

feature_counts as (

    select
        count(*) as feature_user_count,
        count(*) filter (where txn_count_total > 0) as users_with_transactions,
        avg(txn_count_total) as avg_txn_per_user
    from {{ ref('gold_user_fraud_features') }}

)

select
    -- Timestamps
    current_timestamp as recon_run_at,

    -- Bronze layer
    b.bronze_total,
    b.bronze_distinct_txn_ids,
    b.bronze_null_txn_ids,
    b.bronze_null_amounts,
    b.bronze_min_timestamp,
    b.bronze_max_timestamp,

    -- Silver layer
    s.silver_total,
    s.silver_distinct_txn_ids,
    s.silver_min_timestamp,
    s.silver_max_timestamp,

    -- Reconciliation
    f.filtered_count,
    m.missing_count,
    b.bronze_total - s.silver_total as bronze_silver_diff,
    b.bronze_total - s.silver_total - f.filtered_count as unaccounted_records,

    -- Balance check: 0 means everything is accounted for
    case
        when b.bronze_total - s.silver_total - f.filtered_count = 0
        then 'BALANCED'
        else 'UNBALANCED'
    end as recon_status,

    -- Enrichment coverage
    e.enriched_total,
    e.enriched_with_merchant,
    e.enriched_with_user,
    e.enriched_missing_merchant,
    e.enriched_missing_user,
    case
        when e.enriched_total > 0
        then round(e.enriched_with_merchant::numeric / e.enriched_total * 100, 2)
        else 0
    end as merchant_coverage_pct,
    case
        when e.enriched_total > 0
        then round(e.enriched_with_user::numeric / e.enriched_total * 100, 2)
        else 0
    end as user_coverage_pct,

    -- Feature store
    fc.feature_user_count,
    fc.users_with_transactions,
    round(fc.avg_txn_per_user::numeric, 2) as avg_txn_per_user

from bronze_counts b
cross join silver_counts s
cross join filtered_records f
cross join missing_records m
cross join enriched_counts e
cross join feature_counts fc

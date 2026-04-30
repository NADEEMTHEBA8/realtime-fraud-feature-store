/*
    dq_pipeline_health.sql — Data Quality: Pipeline Health Summary

    A single-row summary of pipeline health metrics that an ops
    dashboard would consume. Designed to be queried by Grafana,
    Metabase, or any BI tool.

    Metrics include:
        - Record counts at each layer
        - Reconciliation status
        - Data freshness (age of latest record)
        - Enrichment coverage rates
        - Anomaly counts from fraud features
*/

{{ config(
    materialized='table',
    schema='data_quality'
) }}

with recon as (

    select * from {{ ref('recon_bronze_silver') }}

),

fraud_anomalies as (

    select
        -- Users with suspiciously high velocity
        count(*) filter (where txn_count_1h > 10) as high_velocity_users,

        -- Users with high failure rates
        count(*) filter (where failure_rate_24h > 0.2) as high_failure_rate_users,

        -- Users with high refund rates
        count(*) filter (where refund_rate_7d > 0.3) as high_refund_rate_users,

        -- Users transacting from multiple cities
        count(*) filter (where unique_cities_24h > 3) as multi_city_users,

        -- Users with late-night activity
        count(*) filter (where late_night_txn_count_24h > 3) as late_night_active_users,

        -- Total users
        count(*) as total_users

    from {{ ref('gold_user_fraud_features') }}

),

merchant_anomalies as (

    select
        count(distinct merchant_id) filter (where failure_rate > 0.1) as high_failure_merchants,
        count(distinct merchant_id) filter (where refund_rate > 0.15) as high_refund_merchants,
        count(distinct merchant_id) filter (where max_ticket_ratio > 10) as anomalous_ticket_merchants,
        count(distinct merchant_id) as total_merchants
    from {{ ref('gold_daily_merchant_stats') }}

)

select
    current_timestamp as report_generated_at,

    -- Pipeline status
    r.recon_status as pipeline_status,

    -- Volume metrics
    r.bronze_total,
    r.silver_total,
    r.enriched_total,
    r.filtered_count as records_filtered,
    r.unaccounted_records,

    -- Enrichment health
    r.merchant_coverage_pct,
    r.user_coverage_pct,

    -- Feature store health
    r.feature_user_count,

    -- Data freshness
    r.bronze_max_timestamp as latest_bronze_record,
    r.silver_max_timestamp as latest_silver_record,

    -- Fraud signal summary
    fa.high_velocity_users,
    fa.high_failure_rate_users,
    fa.high_refund_rate_users,
    fa.multi_city_users,
    fa.late_night_active_users,
    fa.total_users,

    -- Merchant risk summary
    ma.high_failure_merchants,
    ma.high_refund_merchants,
    ma.anomalous_ticket_merchants,
    ma.total_merchants

from recon r
cross join fraud_anomalies fa
cross join merchant_anomalies ma

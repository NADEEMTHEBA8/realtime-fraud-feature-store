/*
    gold_daily_merchant_stats.sql — Gold Layer: Daily Merchant Aggregates

    One row per merchant per day. Used for:
    - Merchant risk monitoring dashboards
    - Identifying merchants with unusual transaction patterns
    - Input to merchant-level fraud scoring

    Interview talking point:
        "I built daily merchant aggregates that feed the risk monitoring
        dashboard. When a grocery store suddenly processes a ₹500K
        transaction or a merchant's failure rate spikes to 30%, the
        fraud ops team sees it immediately."
*/

{{ config(
    materialized='table',
    schema='gold'
) }}

with enriched as (

    select * from {{ ref('int_transactions_enriched') }}

),

daily_stats as (

    select
        merchant_id,
        merchant_name,
        merchant_category,
        merchant_risk_tier,
        merchant_avg_ticket,
        event_date,

        -- Volume metrics
        count(*)                                          as txn_count,
        count(distinct user_id)                           as unique_users,

        -- Amount metrics
        sum(amount)                                       as total_amount,
        avg(amount)                                       as avg_amount,
        max(amount)                                       as max_amount,
        min(amount)                                       as min_amount,

        -- Status breakdown
        count(*) filter (where status = 'SUCCESS')        as success_count,
        count(*) filter (where status = 'FAILED')         as failed_count,
        count(*) filter (where status = 'PENDING')        as pending_count,

        -- Failure rate
        case
            when count(*) > 0
            then round(
                count(*) filter (where status = 'FAILED')::numeric
                / count(*)::numeric,
                4
            )
            else 0
        end                                               as failure_rate,

        -- Payment method breakdown
        count(*) filter (where payment_method = 'UPI')         as upi_count,
        count(*) filter (where payment_method = 'CARD')        as card_count,
        count(*) filter (where payment_method = 'NETBANKING')  as netbanking_count,
        count(*) filter (where payment_method = 'WALLET')      as wallet_count,

        -- Transaction type breakdown
        count(*) filter (where transaction_type = 'PURCHASE')   as purchase_count,
        count(*) filter (where transaction_type = 'REFUND')     as refund_count,

        -- Refund rate (high = potential refund fraud)
        case
            when count(*) > 0
            then round(
                count(*) filter (where transaction_type = 'REFUND')::numeric
                / count(*)::numeric,
                4
            )
            else 0
        end                                               as refund_rate,

        -- Anomaly signals
        -- Max amount compared to merchant's typical ticket size
        case
            when merchant_avg_ticket is not null and merchant_avg_ticket > 0
            then round(max(amount) / merchant_avg_ticket, 2)
            else null
        end                                               as max_ticket_ratio,

        -- Cross-city transactions (users transacting from different cities)
        count(*) filter (where is_cross_city = true)      as cross_city_count,

        -- High risk combo transactions
        count(*) filter (where is_high_risk_combo = true) as high_risk_combo_count,

        -- Audit
        current_timestamp                                 as _dbt_loaded_at

    from enriched
    group by
        merchant_id,
        merchant_name,
        merchant_category,
        merchant_risk_tier,
        merchant_avg_ticket,
        event_date

)

select * from daily_stats

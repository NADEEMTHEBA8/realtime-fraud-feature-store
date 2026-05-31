/*
    Gold: one row per merchant per day — volume, amounts, status and payment
    mix, failure/refund rates, and a few anomaly counts.
*/

{{ config(materialized='table', schema='gold') }}

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

        count(*)                                          as txn_count,
        count(distinct user_id)                           as unique_users,

        sum(amount)                                       as total_amount,
        avg(amount)                                       as avg_amount,
        max(amount)                                       as max_amount,
        min(amount)                                       as min_amount,

        count(*) filter (where status = 'SUCCESS')        as success_count,
        count(*) filter (where status = 'FAILED')         as failed_count,
        count(*) filter (where status = 'PENDING')        as pending_count,

        case
            when count(*) > 0
            then round(count(*) filter (where status = 'FAILED')::numeric
                       / count(*)::numeric, 4)
            else 0
        end                                               as failure_rate,

        count(*) filter (where payment_method = 'UPI')         as upi_count,
        count(*) filter (where payment_method = 'CARD')        as card_count,
        count(*) filter (where payment_method = 'NETBANKING')  as netbanking_count,
        count(*) filter (where payment_method = 'WALLET')      as wallet_count,

        count(*) filter (where transaction_type = 'PURCHASE')   as purchase_count,
        count(*) filter (where transaction_type = 'REFUND')     as refund_count,

        case
            when count(*) > 0
            then round(count(*) filter (where transaction_type = 'REFUND')::numeric
                       / count(*)::numeric, 4)
            else 0
        end                                               as refund_rate,

        -- Largest transaction relative to the merchant's typical ticket.
        case
            when merchant_avg_ticket is not null and merchant_avg_ticket > 0
            then round(max(amount) / merchant_avg_ticket, 2)
        end                                               as max_ticket_ratio,

        count(*) filter (where is_cross_city = true)      as cross_city_count,
        count(*) filter (where is_high_risk_combo = true) as high_risk_combo_count,

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

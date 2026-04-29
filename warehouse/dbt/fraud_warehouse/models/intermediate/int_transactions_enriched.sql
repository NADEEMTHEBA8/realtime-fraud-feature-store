/*
    int_transactions_enriched.sql — Intermediate: Enriched Transactions

    This model joins the silver-layer transactions with merchant and user
    reference data to create a single wide table ready for gold-layer
    aggregations and fraud feature engineering.

    Why this is an INTERMEDIATE model (not gold):
        Intermediate models do joins and reshaping. Gold models do
        aggregations and business metrics. Keeping them separate means
        you can add new gold models (e.g., a new fraud feature) without
        re-doing the expensive join logic.

    Interview talking point:
        "I built an enriched transactions model that joins payment events
        with CDC-sourced merchant and user dimensions. This gives the
        fraud feature pipeline access to merchant risk tiers and user
        KYC levels at query time — fields that aren't in the raw
        transaction event but are critical for fraud scoring."
*/

{{ config(
    materialized='table',
    schema='gold'
) }}

with transactions as (

    select * from {{ ref('stg_transactions') }}

),

merchants as (

    select * from {{ ref('stg_merchants') }}

),

users as (

    select * from {{ ref('stg_users') }}

),

enriched as (

    select
        -- Transaction fields
        t.transaction_id,
        t.event_timestamp,
        t.ingestion_timestamp,
        t.transaction_type,
        t.status,
        t.payment_method,
        t.amount,
        t.currency,

        -- User fields (from CDC)
        t.user_id,
        u.user_city,
        u.user_country,
        u.user_kyc_level,
        u.user_risk_score,
        u.is_blocked            as user_is_blocked,

        -- Merchant fields (from CDC)
        t.merchant_id,
        m.merchant_name,
        m.merchant_category,
        m.merchant_risk_tier,
        m.avg_ticket_size       as merchant_avg_ticket,
        m.is_active             as merchant_is_active,

        -- Device and location from transaction
        t.device_id,
        t.ip_address,
        t.city                  as transaction_city,
        t.country               as transaction_country,

        -- Derived fields for fraud detection
        -- Is the transaction amount unusually high for this merchant?
        case
            when m.avg_ticket_size is not null and m.avg_ticket_size > 0
            then round(t.amount / m.avg_ticket_size, 2)
            else null
        end                     as amount_to_avg_ticket_ratio,

        -- Is the user transacting from a different city than their registered city?
        case
            when t.city is not null and u.user_city is not null
                 and t.city != u.user_city
            then true
            else false
        end                     as is_cross_city,

        -- Risk flag: high-risk user + high-risk merchant
        case
            when u.user_risk_score > 0.5 and m.merchant_risk_tier in ('high', 'critical')
            then true
            else false
        end                     as is_high_risk_combo,

        -- Partition columns
        t.event_date,
        t.event_hour,

        -- Kafka lineage
        t._kafka_partition,
        t._kafka_offset,

        -- Audit
        current_timestamp       as _dbt_loaded_at

    from transactions t
    left join merchants m on t.merchant_id = m.merchant_id
    left join users u on t.user_id = u.user_id

)

select * from enriched

/*
    Transactions joined to the user and merchant dimensions, with a few
    derived fraud signals. Joins live here so gold models only aggregate.
*/

{{ config(materialized='table', schema='silver') }}

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
        t.transaction_id,
        t.event_timestamp,
        t.ingestion_timestamp,
        t.transaction_type,
        t.status,
        t.payment_method,
        t.amount,
        t.currency,

        t.user_id,
        u.user_city,
        u.user_country,
        u.user_kyc_level,
        u.user_risk_score,
        u.is_blocked            as user_is_blocked,

        t.merchant_id,
        m.merchant_name,
        m.merchant_category,
        m.merchant_risk_tier,
        m.avg_ticket_size       as merchant_avg_ticket,
        m.is_active             as merchant_is_active,

        t.device_id,
        t.ip_address,
        t.city                  as transaction_city,
        t.country               as transaction_country,

        -- Transaction amount relative to the merchant's typical ticket.
        case
            when m.avg_ticket_size is not null and m.avg_ticket_size > 0
            then round(t.amount / m.avg_ticket_size, 2)
        end                     as amount_to_avg_ticket_ratio,

        -- Transaction city differs from the user's registered city.
        case
            when t.city is not null and u.user_city is not null
                 and t.city != u.user_city
            then true
            else false
        end                     as is_cross_city,

        -- High-risk user transacting at a high-risk merchant.
        case
            when u.user_risk_score > 0.5 and m.merchant_risk_tier = 'HIGH'
            then true
            else false
        end                     as is_high_risk_combo,

        t.event_date,
        t.event_hour,
        t._kafka_partition,
        t._kafka_offset,
        current_timestamp       as _dbt_loaded_at

    from transactions t
    left join merchants m on t.merchant_id = m.merchant_id
    left join users u on t.user_id = u.user_id

)

select * from enriched

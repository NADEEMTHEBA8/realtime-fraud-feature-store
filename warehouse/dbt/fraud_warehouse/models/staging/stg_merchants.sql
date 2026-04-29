/*
    stg_merchants.sql — Silver Layer: Merchant Reference Data

    Source: public.merchants (captured via Debezium CDC)

    In production, this table lives in a separate operational database.
    Debezium streams changes to Kafka, and a sink connector or Spark job
    loads them into the warehouse. For local dev, we read directly from
    Postgres since it's the same instance.

    Interview talking point:
        "Merchant reference data is captured via CDC using Debezium.
        When a merchant's risk_tier changes from 'low' to 'critical',
        the change flows through Kafka to the warehouse in sub-second
        latency. My staging model cleans and types this data for
        downstream joins with the transaction fact table."
*/

{{ config(
    materialized='view',
    schema='silver'
) }}

with source as (

    select * from {{ source('reference', 'merchants') }}

),

staged as (

    select
        merchant_id,
        merchant_name,
        category                as merchant_category,
        risk_tier               as merchant_risk_tier,
        avg_ticket_size,
        is_active,
        onboarded_at,
        updated_at,
        current_timestamp       as _dbt_loaded_at

    from source

)

select * from staged

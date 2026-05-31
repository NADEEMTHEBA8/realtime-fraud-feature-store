/*
    Silver staging for the merchant dimension (CDC source: public.merchants).

    dbt reads public.merchants directly from Postgres. Debezium captures the
    same table to Kafka CDC topics (observable in Kafka UI); this prototype
    does not run a sink back into the warehouse.
*/

{{ config(materialized='view') }}

with source as (

    select * from {{ source('reference', 'merchants') }}

),

staged as (

    select
        merchant_id,
        merchant_name,
        category        as merchant_category,
        risk_tier       as merchant_risk_tier,
        avg_ticket_size,
        is_active,
        onboarded_at,
        updated_at,
        current_timestamp as _dbt_loaded_at

    from source

)

select * from staged

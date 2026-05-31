/*
    Silver staging for the user dimension (CDC source: public.users).
*/

{{ config(materialized='view') }}

with source as (

    select * from {{ source('reference', 'users') }}

),

staged as (

    select
        user_id,
        email_hash,
        phone_hash,
        city            as user_city,
        country         as user_country,
        kyc_level       as user_kyc_level,
        risk_score      as user_risk_score,
        is_blocked,
        created_at,
        updated_at,
        current_timestamp as _dbt_loaded_at

    from source

)

select * from staged

/*
    stg_users.sql — Silver Layer: User Reference Data

    Source: public.users (captured via Debezium CDC)

    Interview talking point:
        "User KYC level and risk score are critical for fraud detection.
        A user upgrading from 'basic' to 'full' KYC changes their
        transaction limits and risk profile. CDC ensures the warehouse
        reflects this within seconds, not after a nightly batch."
*/

{{ config(
    materialized='view',
    schema='silver'
) }}

with source as (

    select * from {{ source('reference', 'users') }}

),

staged as (

    select
        user_id,
        email_hash,
        phone_hash,
        city                    as user_city,
        country                 as user_country,
        kyc_level               as user_kyc_level,
        risk_score              as user_risk_score,
        is_blocked,
        created_at,
        updated_at,
        current_timestamp       as _dbt_loaded_at

    from source

)

select * from staged

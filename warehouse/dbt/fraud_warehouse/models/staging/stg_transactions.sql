/*
    Silver staging for transaction events: cast bronze TEXT columns to
    typed columns, drop records that fail basic validation.
*/

{{ config(materialized='view') }}

with source as (

    select * from {{ source('bronze', 'transactions') }}

),

casted as (

    select
        transaction_id,
        user_id,
        merchant_id,

        cast(amount as NUMERIC(12, 2)) as amount,
        currency,

        transaction_type,
        status,
        payment_method,

        cast(event_timestamp as TIMESTAMP) as event_timestamp,
        cast(ingestion_timestamp as TIMESTAMP) as ingestion_timestamp,

        device_id,
        ip_address,
        city,
        country,

        _kafka_topic,
        cast(_kafka_partition as INTEGER) as _kafka_partition,
        cast(_kafka_offset as BIGINT) as _kafka_offset,
        cast(_kafka_timestamp as TIMESTAMP) as _kafka_timestamp,
        cast(_processing_timestamp as TIMESTAMP) as _processing_timestamp,

        cast(event_date as DATE) as event_date,
        cast(event_hour as INTEGER) as event_hour,

        current_timestamp as _dbt_loaded_at

    from source

),

validated as (

    select *
    from casted
    where transaction_id is not null
      and amount > 0
      and event_timestamp is not null
      and transaction_type in ('PURCHASE', 'REFUND', 'TRANSFER', 'WITHDRAWAL')

)

select * from validated

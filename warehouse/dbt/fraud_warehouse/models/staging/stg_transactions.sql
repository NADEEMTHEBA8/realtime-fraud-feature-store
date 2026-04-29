/*
    stg_transactions.sql — Silver Layer Staging Model

    What this model does:
        1. Casts all columns from TEXT to proper types (NUMERIC, TIMESTAMP, INT)
        2. Renames columns to consistent snake_case conventions
        3. Filters out records that fail basic validation
        4. Adds a surrogate key and audit columns

    Why a staging model?
        In dbt, staging models are the FIRST transformation layer. They sit between
        raw sources (bronze) and business logic models (gold). Their job is purely
        mechanical: clean types, rename fields, basic filtering. No business logic.

    Interview talking point:
        "My staging models handle the schema contract between raw ingestion and
        business logic. They cast types, enforce not-null constraints, and add
        audit metadata. This separation means if the source schema changes,
        I only fix one model — not every downstream query."

    Materialization: VIEW
        Staging models are typically views, not tables. Why?
        - No data duplication (saves storage)
        - Always reflects the latest bronze data
        - Fast to build (no data movement)
        In production with millions of rows, you might switch to incremental.
*/

{{ config(
    materialized='view',
    schema='silver'
) }}

with source as (

    select * from {{ source('bronze', 'transactions') }}

),

casted as (

    select
        -- Primary key
        transaction_id,

        -- Entity references
        user_id,
        merchant_id,

        -- Financial data: CAST to NUMERIC, never FLOAT
        -- Why NUMERIC not FLOAT? FLOAT has precision errors:
        -- 0.1 + 0.2 = 0.30000000000000004 in FLOAT
        -- NUMERIC stores exact decimal values — required for money
        cast(amount as NUMERIC(12, 2)) as amount,
        currency,

        -- Transaction classification
        transaction_type,
        status,
        payment_method,

        -- Timestamps: cast from TEXT to proper TIMESTAMP
        cast(event_timestamp as TIMESTAMP) as event_timestamp,
        cast(ingestion_timestamp as TIMESTAMP) as ingestion_timestamp,

        -- Device and location
        device_id,
        ip_address,
        city,
        country,

        -- Kafka metadata (keep for lineage and debugging)
        _kafka_topic,
        cast(_kafka_partition as INTEGER) as _kafka_partition,
        cast(_kafka_offset as BIGINT) as _kafka_offset,
        cast(_kafka_timestamp as TIMESTAMP) as _kafka_timestamp,
        cast(_processing_timestamp as TIMESTAMP) as _processing_timestamp,

        -- Partition columns
        cast(event_date as DATE) as event_date,
        cast(event_hour as INTEGER) as event_hour,

        -- Audit: when did dbt process this row?
        current_timestamp as _dbt_loaded_at

    from source

),

validated as (

    /*
        Basic validation rules for the silver layer:
        - transaction_id must exist (primary key)
        - amount must be positive (no zero or negative transactions)
        - event_timestamp must be parseable (not null after cast)
        - transaction_type must be a known value

        Records failing these checks are excluded from silver.
        In a production system, you'd route these to a quarantine table.
    */

    select *
    from casted
    where
        transaction_id is not null
        and amount > 0
        and event_timestamp is not null
        and transaction_type in ('PURCHASE', 'REFUND', 'TRANSFER', 'WITHDRAWAL')

)

select * from validated

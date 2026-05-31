/*
    SCD Type 2 snapshot of the user dimension. Tracks KYC level, risk score,
    blocked status and city changes over time using the updated_at column.
*/

{% snapshot snp_users %}

{{
    config(
        target_schema='snapshots',
        unique_key='user_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select * from {{ source('reference', 'users') }}

{% endsnapshot %}

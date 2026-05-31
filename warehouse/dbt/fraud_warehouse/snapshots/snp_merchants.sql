/*
    SCD Type 2 snapshot of the merchant dimension. A change to risk_tier,
    category, avg_ticket_size, etc. closes the current row (sets dbt_valid_to)
    and inserts a new one. Uses the timestamp strategy on updated_at.
*/

{% snapshot snp_merchants %}

{{
    config(
        target_schema='snapshots',
        unique_key='merchant_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select * from {{ source('reference', 'merchants') }}

{% endsnapshot %}

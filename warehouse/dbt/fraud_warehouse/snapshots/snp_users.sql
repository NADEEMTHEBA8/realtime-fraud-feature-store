/*
    snp_users.sql — SCD Type 2 Snapshot for User Dimension

    Tracks changes to user KYC level, risk score, blocked status, and city.

    Why snapshot users?
        A user's KYC level upgrade from 'basic' to 'full' changes their
        transaction limits and risk profile. For fraud investigation,
        you need to know: "Was this user KYC-verified at the time of
        the disputed transaction?"
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

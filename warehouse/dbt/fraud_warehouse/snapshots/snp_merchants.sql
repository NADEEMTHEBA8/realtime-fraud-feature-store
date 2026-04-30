/*
    snp_merchants.sql — SCD Type 2 Snapshot for Merchant Dimension

    What this does:
        Tracks every change to the merchants table over time. When a merchant's
        risk_tier, category, avg_ticket_size, or any other field changes, dbt
        creates a new row with the new values and closes out the old row by
        setting dbt_valid_to.

    How it works:
        dbt compares the current state of public.merchants against the previous
        snapshot stored in the snapshots schema. For each row:
        - If unchanged → skip
        - If changed → close old row (set dbt_valid_to = now), insert new row
        - If new → insert with dbt_valid_to = NULL
        - If deleted → close old row (optional, via invalidate_hard_deletes)

    Strategy: timestamp
        We use the 'updated_at' column to detect changes. Every time a row
        is modified, the application sets updated_at = now(). dbt compares
        the stored updated_at against the current one.

        Alternative: 'check' strategy compares all columns. More thorough
        but slower on wide tables.

    Interview talking point:
        "I implemented SCD Type 2 on the merchant dimension using dbt snapshots.
        This lets fraud investigators ask 'what was this merchant's risk tier
        at the time of the transaction?' — not just what it is now. In fintech,
        this historical context is critical for retroactive fraud analysis and
        regulatory audits."
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

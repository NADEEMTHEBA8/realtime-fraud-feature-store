/*
    gold_user_fraud_features.sql — Gold Layer: User-Level Fraud Features

    This model computes rolling aggregate features for each user that a
    fraud scoring model would consume. Features are computed across three
    time windows: 1 hour, 24 hours, and 7 days.

    Why these specific features?
        Fraud patterns are almost always about VELOCITY and DEVIATION:
        - A user who normally makes 2 transactions/day suddenly makes 20 → suspicious
        - A user whose average transaction is ₹500 suddenly sends ₹50,000 → suspicious
        - A user who shops at 2 merchants suddenly hits 15 different ones → card stolen

    How this works in production:
        In real-time fraud scoring, these features would be computed by Spark
        Structured Streaming or Flink with sliding windows, and served from Redis.
        This batch version computes the same features on a schedule (via Airflow)
        for offline model training and backtesting.

    Interview talking point:
        "I built rolling fraud features across 1h/24h/7d windows covering
        transaction velocity, amount statistics, merchant diversity, and
        payment method entropy. These are the same feature categories used
        by production fraud systems at companies like Razorpay and PhonePe.
        The batch version feeds model training; the real-time version
        (via Redis) feeds the scoring API."
*/

{{ config(
    materialized='table',
    schema='gold'
) }}

with transactions as (

    select * from {{ ref('stg_transactions') }}

),

/*
    Reference point: use each user's latest transaction timestamp
    as the "current time" for window calculations.
    In production, you'd use the scoring request timestamp.
*/
user_latest as (

    select
        user_id,
        max(event_timestamp) as latest_txn_at
    from transactions
    group by user_id

),

/*
    Tag each transaction with its age relative to the user's latest
    transaction. This lets us use simple WHERE filters for windows.
*/
transactions_with_age as (

    select
        t.*,
        ul.latest_txn_at,
        extract(epoch from (ul.latest_txn_at - t.event_timestamp)) / 3600.0 as hours_ago
    from transactions t
    inner join user_latest ul on t.user_id = ul.user_id

),

user_features as (

    select
        user_id,
        latest_txn_at,

        -- ============================================================
        -- TRANSACTION VELOCITY (count of transactions in window)
        -- High velocity = potential card testing or automated fraud
        -- ============================================================
        count(*) filter (where hours_ago <= 1)    as txn_count_1h,
        count(*) filter (where hours_ago <= 24)   as txn_count_24h,
        count(*) filter (where hours_ago <= 168)  as txn_count_7d,
        count(*)                                  as txn_count_total,

        -- ============================================================
        -- AMOUNT STATISTICS (spending patterns per window)
        -- Sudden spikes in amount relative to historical average = suspicious
        -- ============================================================
        -- 1-hour window
        coalesce(sum(amount) filter (where hours_ago <= 1), 0)     as txn_sum_1h,
        coalesce(avg(amount) filter (where hours_ago <= 1), 0)     as txn_avg_1h,
        coalesce(max(amount) filter (where hours_ago <= 1), 0)     as txn_max_1h,

        -- 24-hour window
        coalesce(sum(amount) filter (where hours_ago <= 24), 0)    as txn_sum_24h,
        coalesce(avg(amount) filter (where hours_ago <= 24), 0)    as txn_avg_24h,
        coalesce(max(amount) filter (where hours_ago <= 24), 0)    as txn_max_24h,

        -- 7-day window
        coalesce(sum(amount) filter (where hours_ago <= 168), 0)   as txn_sum_7d,
        coalesce(avg(amount) filter (where hours_ago <= 168), 0)   as txn_avg_7d,
        coalesce(max(amount) filter (where hours_ago <= 168), 0)   as txn_max_7d,

        -- All-time
        sum(amount)                                                as txn_sum_total,
        avg(amount)                                                as txn_avg_total,
        max(amount)                                                as txn_max_total,
        min(amount)                                                as txn_min_total,

        -- ============================================================
        -- MERCHANT DIVERSITY (unique merchants per window)
        -- Sudden spike in unique merchants = potential stolen card
        -- being tested across many merchants
        -- ============================================================
        count(distinct merchant_id) filter (where hours_ago <= 1)   as unique_merchants_1h,
        count(distinct merchant_id) filter (where hours_ago <= 24)  as unique_merchants_24h,
        count(distinct merchant_id) filter (where hours_ago <= 168) as unique_merchants_7d,

        -- ============================================================
        -- PAYMENT METHOD PATTERNS
        -- Fraudsters often switch payment methods to find one that works
        -- ============================================================
        count(distinct payment_method) filter (where hours_ago <= 24) as unique_payment_methods_24h,

        -- Dominant payment method (most used overall)
        mode() within group (order by payment_method)                as preferred_payment_method,

        -- ============================================================
        -- FAILURE RATE
        -- High failure rate = potential card testing (trying stolen cards)
        -- ============================================================
        count(*) filter (where status = 'FAILED' and hours_ago <= 24)  as failed_txn_count_24h,
        case
            when count(*) filter (where hours_ago <= 24) > 0
            then round(
                count(*) filter (where status = 'FAILED' and hours_ago <= 24)::numeric
                / count(*) filter (where hours_ago <= 24)::numeric,
                4
            )
            else 0
        end                                                            as failure_rate_24h,

        -- ============================================================
        -- REFUND PATTERNS
        -- Excessive refunds = potential refund fraud or friendly fraud
        -- ============================================================
        count(*) filter (where transaction_type = 'REFUND' and hours_ago <= 168) as refund_count_7d,
        case
            when count(*) filter (where hours_ago <= 168) > 0
            then round(
                count(*) filter (where transaction_type = 'REFUND' and hours_ago <= 168)::numeric
                / count(*) filter (where hours_ago <= 168)::numeric,
                4
            )
            else 0
        end                                                            as refund_rate_7d,

        -- ============================================================
        -- CITY DIVERSITY
        -- Transactions from multiple cities in a short window = suspicious
        -- (unless user is traveling)
        -- ============================================================
        count(distinct city) filter (where hours_ago <= 24)   as unique_cities_24h,

        -- ============================================================
        -- TIME PATTERNS
        -- Transactions at unusual hours (2am-5am) = higher risk
        -- ============================================================
        count(*) filter (where hours_ago <= 24 and event_hour between 2 and 5) as late_night_txn_count_24h,

        -- ============================================================
        -- AMOUNT DEVIATION
        -- How far is the latest transaction from the user's average?
        -- High deviation = anomalous spending
        -- ============================================================
        case
            when stddev(amount) > 0
            then round(
                (max(amount) filter (where hours_ago = 0) - avg(amount))
                / stddev(amount),
                4
            )
            else 0
        end                                                    as latest_amount_zscore

    from transactions_with_age
    group by user_id, latest_txn_at

)

select
    *,
    current_timestamp as _feature_computed_at
from user_features

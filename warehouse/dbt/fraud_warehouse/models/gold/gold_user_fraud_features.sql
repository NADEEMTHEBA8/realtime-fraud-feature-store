/*
    Gold: per-user fraud features over 1h / 24h / 7d windows.

    Windows are measured backwards from each user's most recent transaction
    (their "current time"), so a simple WHERE on hours_ago implements them.
    A real-time scorer would window against the scoring request time instead.
*/

{{ config(materialized='table', schema='gold') }}

with transactions as (

    select * from {{ ref('stg_transactions') }}

),

user_latest as (

    select
        user_id,
        max(event_timestamp) as latest_txn_at
    from transactions
    group by user_id

),

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

        -- Transaction velocity (count per window).
        count(*) filter (where hours_ago <= 1)    as txn_count_1h,
        count(*) filter (where hours_ago <= 24)   as txn_count_24h,
        count(*) filter (where hours_ago <= 168)  as txn_count_7d,
        count(*)                                  as txn_count_total,

        -- Amount statistics per window.
        coalesce(sum(amount) filter (where hours_ago <= 1), 0)     as txn_sum_1h,
        coalesce(avg(amount) filter (where hours_ago <= 1), 0)     as txn_avg_1h,
        coalesce(max(amount) filter (where hours_ago <= 1), 0)     as txn_max_1h,

        coalesce(sum(amount) filter (where hours_ago <= 24), 0)    as txn_sum_24h,
        coalesce(avg(amount) filter (where hours_ago <= 24), 0)    as txn_avg_24h,
        coalesce(max(amount) filter (where hours_ago <= 24), 0)    as txn_max_24h,

        coalesce(sum(amount) filter (where hours_ago <= 168), 0)   as txn_sum_7d,
        coalesce(avg(amount) filter (where hours_ago <= 168), 0)   as txn_avg_7d,
        coalesce(max(amount) filter (where hours_ago <= 168), 0)   as txn_max_7d,

        sum(amount)                                                as txn_sum_total,
        avg(amount)                                                as txn_avg_total,
        max(amount)                                                as txn_max_total,
        min(amount)                                                as txn_min_total,

        -- Merchant diversity (unique merchants per window).
        count(distinct merchant_id) filter (where hours_ago <= 1)   as unique_merchants_1h,
        count(distinct merchant_id) filter (where hours_ago <= 24)  as unique_merchants_24h,
        count(distinct merchant_id) filter (where hours_ago <= 168) as unique_merchants_7d,

        -- Payment method spread and dominant method.
        count(distinct payment_method) filter (where hours_ago <= 24) as unique_payment_methods_24h,
        mode() within group (order by payment_method)                as preferred_payment_method,

        -- Failure rate (24h) — elevated rates can indicate card testing.
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

        -- Refund rate (7d).
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

        -- City diversity (24h).
        count(distinct city) filter (where hours_ago <= 24)   as unique_cities_24h,

        -- Late-night activity (02:00-05:00 local hour).
        count(*) filter (where hours_ago <= 24 and event_hour between 2 and 5) as late_night_txn_count_24h,

        -- Z-score of the latest transaction amount vs the user's history.
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

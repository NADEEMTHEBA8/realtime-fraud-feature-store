/*
    test_not_future_timestamp.sql — Custom Generic Test

    Asserts that a timestamp column contains no future dates.
    A transaction with event_timestamp in the future indicates
    either clock skew or data corruption.

    Why this matters in fintech:
        Future-dated transactions can bypass time-based fraud rules
        (e.g., "flag transactions outside business hours"). They also
        break rolling window features — a future transaction would
        dominate the "latest" calculation.
*/

{% test not_future_timestamp(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} > current_timestamp + interval '1 hour'

{% endtest %}

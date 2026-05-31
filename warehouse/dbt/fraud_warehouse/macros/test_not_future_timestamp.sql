/*
    Generic test: timestamp column has no future-dated values.
    A 1-hour grace window absorbs minor clock skew between services.
*/

{% test not_future_timestamp(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} > current_timestamp + interval '1 hour'

{% endtest %}

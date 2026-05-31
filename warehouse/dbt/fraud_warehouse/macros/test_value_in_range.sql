/*
    Generic test: numeric column falls within [min_value, max_value].

    Usage:
        - value_in_range:
            min_value: 0
            max_value: 1
*/

{% test value_in_range(model, column_name, min_value, max_value) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} < {{ min_value }}
   or {{ column_name }} > {{ max_value }}

{% endtest %}

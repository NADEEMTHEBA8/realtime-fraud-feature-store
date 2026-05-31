/*
    Generic test: column contains only positive values (> 0).
    Used for monetary amounts.
*/

{% test positive_value(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}

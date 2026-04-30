/*
    test_positive_value.sql — Custom Generic Test

    Asserts that a column contains only positive values (> 0).
    Used for monetary amounts — a transaction with amount <= 0
    should never reach the silver layer.

    Usage in schema.yml:
        columns:
          - name: amount
            data_tests:
              - positive_value

    Interview talking point:
        "I wrote custom generic tests beyond dbt's built-in set.
        The positive_value test catches negative or zero amounts
        that would corrupt revenue calculations and fraud features."
*/

{% test positive_value(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}

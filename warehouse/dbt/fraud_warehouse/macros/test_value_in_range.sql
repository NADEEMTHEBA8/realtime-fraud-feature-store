/*
    test_value_in_range.sql — Custom Generic Test

    Asserts that a numeric column falls within a specified range.
    Used for risk scores (0 to 1), percentages, rates, etc.

    Usage in schema.yml:
        columns:
          - name: failure_rate_24h
            data_tests:
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

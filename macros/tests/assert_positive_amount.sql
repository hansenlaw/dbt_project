{% test assert_positive_amount(model, column_name) %}

-- Custom test: fail if any monetary value is zero or negative
-- Returns rows that FAIL the check (dbt fails when row count > 0)

select
    {{ column_name }} as amount
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}

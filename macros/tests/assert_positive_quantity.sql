{% test assert_positive_quantity(model, column_name) %}

-- Custom test: fail if any quantity is zero or negative
-- A transaction must sell at least 1 unit to be valid

select
    {{ column_name }} as quantity
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}

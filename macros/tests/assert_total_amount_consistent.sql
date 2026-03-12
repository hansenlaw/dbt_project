{% test assert_total_amount_consistent(model, column_name, qty_column, price_column, tolerance=1) %}

-- Custom test: fail if total_amount deviates from (quantity × unit_price) by more than tolerance
-- Catches data entry errors or upstream calculation bugs

select
    {{ column_name }}                              as total_amount,
    {{ qty_column }} * {{ price_column }}          as expected_amount,
    abs({{ column_name }} - ({{ qty_column }} * {{ price_column }})) as diff
from {{ model }}
where abs({{ column_name }} - ({{ qty_column }} * {{ price_column }})) > {{ tolerance }}

{% endtest %}

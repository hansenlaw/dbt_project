{% test no_invalid_tier(model, column_name) %}

-- Custom test: fail if customer tier contains a value outside allowed list
-- Valid values: Bronze, Silver, Gold
-- Returns rows that FAIL the check (dbt fails when row count > 0)

select
    {{ column_name }} as tier
from {{ model }}
where {{ column_name }} not in ('Bronze', 'Silver', 'Gold')

{% endtest %}

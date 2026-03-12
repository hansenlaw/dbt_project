{% test no_invalid_store_status(model, column_name) %}

-- Custom test: fail if store status contains a value outside allowed list
-- Valid values: active, closed
-- Returns rows that FAIL the check (dbt fails when row count > 0)

select
    {{ column_name }} as status
from {{ model }}
where {{ column_name }} not in ('active', 'closed')

{% endtest %}

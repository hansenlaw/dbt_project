{% test no_invalid_rfm_score(model, column_name) %}

-- Custom test: fail if any RFM score is outside the valid range (1, 2, 3)
-- RFM scoring rules assign exactly 1, 2, or 3 — any other value signals a logic bug
-- Returns rows that FAIL the check (dbt fails when row count > 0)

select
    {{ column_name }} as rfm_score
from {{ model }}
where {{ column_name }} not in (1, 2, 3)

{% endtest %}

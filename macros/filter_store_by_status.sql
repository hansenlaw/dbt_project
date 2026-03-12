{% macro filter_store_by_status(source_relation, status) %}

    SELECT
        initial_store_code,
        store_code,
        start_date,
        end_date
    FROM {{ source_relation }}
    {% if status == 'active' %}
    WHERE end_date = DATE '9999-12-31'
    {% elif status == 'closed' %}
    WHERE end_date < CURRENT_DATE
    {% endif %}

{% endmacro %}

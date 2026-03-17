{% macro clean_name(column_name) %}
nullif(initcap(trim({{ column_name }})), '')
{% endmacro %}

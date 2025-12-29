{% macro convert_to_est(column_name) %}
    CONVERT_TIMEZONE('UTC', 'America/New_York', {{ column_name }})
{% endmacro %}
{{ config(
    materialized='incremental',
    database='compliance_' ~ var('DEPLOYMENT_ENVIRONMENT'),
    unique_key=['table_name', 'column_name', 'execution_date'],
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY table_name, column_name;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ]
) }}

{% set silver_tables = dbt_utils.get_relations_by_prefix(schema='SILVER', prefix='%cleaned') %}

WITH fill_rates AS (
    {% for table in silver_tables %}
    {% set columns = adapter.get_columns_in_relation(table) %}
    {% for column in columns %}
    SELECT
        '{{ table.name }}' AS table_name,
        '{{ column.name }}' AS column_name,
        COUNT(*) AS total_rows,
        COUNT({{ column.name }}) AS non_null_count,
        ROUND((COUNT({{ column.name }}) * 100.0) / NULLIF(COUNT(*), 0), 2) AS fill_rate,
        current_date AS execution_date
    FROM {{ table }}
    WHERE created_at = (SELECT MAX(created_at) FROM {{ table }})
    AND '{{ table.name }}' like '%cleaned'
    {% if is_incremental() %}
        AND current_date > (SELECT MAX(execution_date) FROM {{ this }})
    {% endif %}
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

SELECT * FROM fill_rates

{{ config(
    materialized='incremental',
    unique_key='audit_inconsistent_dw_id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY audit_inconsistent_dw_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ]
) }}


    SELECT
        customer_dw_id AS source_table_id,
        'customer_cleaned' AS source_table_name,
        'ia' as state,
        is_inconsistent,
        inconsistency_id,
        created_at
    FROM {{ ref('customer_cleaned') }} 
    WHERE is_inconsistent = 1

{{ config(
    materialized='incremental',
    unique_key='audit_inconsistent_dw_id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY audit_inconsistent_dw_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ]
) }}

WITH source AS (
    SELECT
        customer_dw_id AS source_table_id,
        'customer_cleaned' AS source_table_name,
        is_inconsistent,
        type_inconsistent,
        created_at
    FROM {{ ref('customer_cleaned') }}
    WHERE is_inconsistent = 1
    UNION ALL
    SELECT
        batch_customer_dw_id AS source_table_id,
        'batch_customer_cleaned' AS source_table_name,
        is_inconsistent,
        type_inconsistent,
        created_at
    FROM {{ ref('batch_customer_cleaned') }}
    WHERE is_inconsistent = 1
    UNION ALL
    SELECT
        event_dw_id AS source_table_id,
        'customer_events_cleaned' AS source_table_name,
        is_inconsistent,
        type_inconsistent,
        created_at
    FROM {{ ref('customer_events_cleaned') }}
    WHERE is_inconsistent = 1
),
source2 AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['source_table_id','source_table_name',]) }} AS audit_inconsistent_dw_id,
        source_table_id,
        source_table_name,
        is_inconsistent,
        type_inconsistent,
        created_at
    FROM source
)

SELECT
    audit_inconsistent_dw_id,
    source_table_id,
    source_table_name,
    is_inconsistent,
    type_inconsistent,
    created_at
FROM source2
{% if is_incremental() %}
    WHERE created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}

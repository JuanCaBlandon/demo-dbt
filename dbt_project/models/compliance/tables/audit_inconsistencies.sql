{{ config(
    materialized='incremental',
    database='compliance_' ~ var('DEPLOYMENT_ENVIRONMENT'),
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
        'ia' as state,
        is_inconsistent,
        type_inconsistent,
        created_at
    FROM {{ ref('customer_cleaned') }}
    WHERE is_inconsistent = 1
    UNION ALL
    SELECT
        batch_customer_dw_id AS source_table_id,
        'batch_customer_cleaned' AS source_table_name,
        'ia' as state,
        is_inconsistent,
        type_inconsistent,
        created_at
    FROM {{ ref('batch_customer_cleaned') }}
    WHERE is_inconsistent = 1
    UNION ALL
    SELECT
        event_dw_id AS source_table_id,
        'customer_events_cleaned' AS source_table_name,
        'ia' as state,
        is_inconsistent,
        type_inconsistent,
        created_at
    FROM {{ ref('customer_events_cleaned') }}
    WHERE is_inconsistent = 1

    UNION ALL
    SELECT
        c.customer_dw_id AS source_table_id,
        'customer_cleaned' AS source_table_name,
        'ia' as state,
        1 AS is_inconsistent,
        'Not present in batch file' AS type_inconsistent,
        c.created_at
    FROM {{ ref('customer_cleaned') }} as c
    LEFT JOIN {{ ref('batch_customer_cleaned') }} as bc 
    ON c.drivers_license_number = bc.drivers_license_number
        AND RIGHT(bc.vin,6) = RIGHT(c.vin,6)
    WHERE
        c.is_inconsistent = 0
        AND  bc.batch_customer_dw_id IS NULL

    UNION ALL
    SELECT
        bc.batch_customer_dw_id AS source_table_id,
        'batch_customer_cleaned' AS source_table_name,
        'ia' as state,
        1 AS is_inconsistent,
        'Not present in actives customer table' AS type_inconsistent,
        c.created_at
    FROM {{ ref('batch_customer_cleaned') }} as bc
    LEFT JOIN {{ ref('customer_cleaned') }} as c
    ON c.drivers_license_number = bc.drivers_license_number
        AND RIGHT(bc.vin,6) = RIGHT(c.vin,6)
    WHERE
        bc.is_inconsistent = 0
        AND  c.customer_dw_id IS NULL

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

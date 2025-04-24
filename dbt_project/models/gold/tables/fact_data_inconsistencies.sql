{{ config(
    materialized='incremental',
    unique_key='inconsistency_dw_id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY inconsistency_dw_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ],
    tags=["gold_inconsistencies"]
) }}

WITH source AS (
    -- Customer inconsistencies
    SELECT
        customer_dw_id AS source_table_id,
        'customer_cleaned' AS source_table_name,
        is_inconsistent,
        inconsistency_id,
        CURRENT_TIMESTAMP() AS created_at
    FROM {{ ref('customer_cleaned') }}
    WHERE is_inconsistent = 1

    UNION ALL

    -- Product inconsistencies
    SELECT
        product_dw_id AS source_table_id,
        'product_cleaned' AS source_table_name,
        is_inconsistent,
        inconsistency_id,
        CURRENT_TIMESTAMP() AS created_at
    FROM {{ ref('product_cleaned') }}
    WHERE is_inconsistent = 1

    UNION ALL

    -- Store inconsistencies
    SELECT
        store_dw_id AS source_table_id,
        'store_cleaned' AS source_table_name,
        is_inconsistent,
        inconsistency_id,
        CURRENT_TIMESTAMP() AS created_at
    FROM {{ ref('store_cleaned') }}
    WHERE is_inconsistent = 1

    UNION ALL

    -- Product Cost History inconsistencies
    SELECT
        product_cost_dw_id AS source_table_id,
        'product_cost_history_cleaned' AS source_table_name,
        is_inconsistent,
        inconsistency_id,
        CURRENT_TIMESTAMP() AS created_at
    FROM {{ ref('product_cost_history_cleaned') }}
    WHERE is_inconsistent = 1

    UNION ALL

    -- Sales Order Header inconsistencies
    SELECT
        sales_header_dw_id AS source_table_id,
        'sales_order_header_cleaned' AS source_table_name,
        is_inconsistent,
        inconsistency_id,
        CURRENT_TIMESTAMP() AS created_at
    FROM {{ ref('sales_order_header_cleaned') }}
    WHERE is_inconsistent = 1
),

enriched_inconsistencies AS (
    SELECT
        source.*,
        im.description AS inconsistency_description,
        im.data_quality_dimension,
        im.rationale,
        -- Extraer la fecha de created_at para enlazar con dim_date
        dd.date_id,
        to_date(created_at) AS created_date
    FROM source
    LEFT JOIN {{ ref('inconsistency_master') }} im
    ON source.inconsistency_id = im.inconsistency_id
    INNER JOIN {{ ref('dim_date') }} dd
    ON to_date(created_at) = dd.date_full
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['source_table_id', 'source_table_name', 'inconsistency_id']) }} AS inconsistency_dw_id,
        source_table_id,
        source_table_name,
        is_inconsistent,
        inconsistency_id,
        inconsistency_description,
        data_quality_dimension,
        rationale,
        created_at,
        date_id
    FROM enriched_inconsistencies
)

SELECT * FROM final
{% if is_incremental() %}
    WHERE created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}
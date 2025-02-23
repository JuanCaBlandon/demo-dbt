{{ config(
    materialized = 'incremental',
    post_hook=["ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"],
    tags=["silver_ia_1"]
) }}

WITH violations AS (
    SELECT
    {{ dbt_utils.generate_surrogate_key(['id','description']) }} as record_type_dw_id, 
    id,
    description,
    created_at
    FROM {{ ref('record_type_raw') }}
)
SELECT 
    record_type_dw_id, 
    id,
    description,
    created_at
FROM violations

{% if is_incremental() %}

   WHERE record_type_dw_id NOT IN (select record_type_dw_id from {{ this }})

{% endif %}
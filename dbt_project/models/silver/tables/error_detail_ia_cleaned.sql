{{ config(
    materialized = 'incremental',
    post_hook=["ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"],
    tags=["silver_ia_1"]
) }}

WITH violations AS (
    SELECT
    {{ dbt_utils.generate_surrogate_key(['code','error_message']) }} as error_detail_dw_id, 
    code,
    error_message,
    created_at
    FROM {{ ref('error_detail_raw') }}
)
SELECT 
    error_detail_dw_id, 
    code,
    error_message,
    created_at
FROM violations

{% if is_incremental() %}

   WHERE error_detail_dw_id NOT IN (select error_detail_dw_id from {{ this }})

{% endif %}
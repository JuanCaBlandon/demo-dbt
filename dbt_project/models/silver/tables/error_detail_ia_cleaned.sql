{{ config(
    materialized = 'incremental',
    post_hook=["ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"]
) }}

WITH violations AS (
    SELECT
    {{ dbt_utils.generate_surrogate_key(['code','error_message']) }} as record_type_dw_id, 
    code,
    error_message,
    created_at
    FROM {{ ref('error_detail_ia') }}
)
SELECT 
    record_type_dw_id, 
    code,
    error_message,
    created_at
FROM violations

{% if is_incremental() %}

   WHERE record_type_dw_id NOT IN (select record_type_dw_id from {{ this }})

{% endif %}
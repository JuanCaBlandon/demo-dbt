{{ config(
    materialized = 'incremental',
    post_hook=["ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"]
) }}

SELECT 
    error_detail_dw_id, 
    code,
    error_message,
    created_at
FROM {{ ref('error_detail_ia_cleaned') }}

{% if is_incremental() %}

   WHERE error_detail_dw_id NOT IN (select error_detail_dw_id from {{ this }})

{% endif %}
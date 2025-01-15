{{ config(
    materialized = 'incremental',
    post_hook=["ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"]
) }}

SELECT 
    record_type_dw_id, 
    id,
    description,
    created_at
FROM {{ ref('record_type_ia_cleaned') }}

{% if is_incremental() %}

   WHERE record_type_dw_id NOT IN (select record_type_dw_id from {{ this }})

{% endif %}
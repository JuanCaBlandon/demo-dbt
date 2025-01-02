{{ config(
    materialized='incremental',
    unique_key='customer_dw_id',
    incremental_strategy="merge",
    merge_update_columns = ['effectice_end_date'],
post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY customer_reporting_state_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
) }}


select
    {{ dbt_utils.generate_surrogate_key(['table_name','type_inconsistent']) }} as audit_inconsistent_dw_id, 
    --customer_dw_id AD table_dw_id,
    --table_name
    --type_inconsistent,

    --created_at current_timestamp
from {{ ref('customer_cleaned') }}
where is_inconsistent = 1
{% if is_incremental() %}
    and customer_dw_id not in (select customer_dw_id from {{ this }})
{% endif %}
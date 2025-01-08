{{ config(
    materialized='incremental',
    unique_key='audit_inconsistent_dw_id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY audit_inconsistent_dw_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ]
) }}

with source as (
    select
        customer_dw_id as source_table_id,
        'customer_cleaned' as source_table_name,
        is_inconsistent,
        type_inconsistent,
        created_at
    from {{ ref('customer_cleaned') }}
    where is_inconsistent = 1
    UNION ALL
    select
        batch_customer_dw_id as source_table_id,
        'batch_customer_cleaned' as source_table_name,
        is_inconsistent,
        type_inconsistent,
        created_at
    from {{ ref('batch_customer_cleaned') }}
    where is_inconsistent = 1
    UNION ALL
    select
        violation_dw_id as source_table_id,
        'customer_violations_cleaned' as source_table_name,
        is_inconsistent,
        type_inconsistent,
        created_at
    from {{ ref('customer_violations_cleaned') }}
    where is_inconsistent = 1
)

,

source2 as (
    select
        {{ dbt_utils.generate_surrogate_key(['source_table_id','source_table_name',]) }} as audit_inconsistent_dw_id,
        source_table_id,
        source_table_name,
        is_inconsistent,
        type_inconsistent,
        created_at
    from source
)

select
    audit_inconsistent_dw_id,
    source_table_id,
    source_table_name,
    is_inconsistent,
    type_inconsistent,
    created_at
from source2
{% if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})
{% endif %}

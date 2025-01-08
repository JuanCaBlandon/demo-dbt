{{ config(
    materialized='incremental',
    unique_key='batch_customer_dw_id',
post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY customer_dw_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
) }}


SELECT
    batch_customer_dw_id,
    customer_dw_id,
    vendor_name,
    drivers_license_number,
    first_name,
    last_name,
    middle_name,
    date_of_birth,
    vin,
    offense_date,
    repeat_offender,
    IID_Start_Date,
    IID_End_Date,
    created_at
from {{ ref('batch_customer_cleaned') }}
where is_inconsistent = 0
{% if is_incremental() %}
    and batch_customer_dw_id not in (select batch_customer_dw_id from {{ this }})
{% endif %}
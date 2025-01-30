{{ config(
    materialized='incremental',
    unique_key='customer_dw_id',
    incremental_strategy="merge",
    merge_update_columns = ['effective_end_date'],
post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY customer_dw_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
) }}


select
    customer_dw_id,
    customer_reporting_state_id,
    customer_id,
    drivers_license_number,
    first_name,
    last_name,
    middle_name,
    date_of_birth,
    vin,
    install_date,
    deinstall_date,
    state_code,
    active_status,
    report_status_cd,
    customer_status,
    active_status_start_date,
    active_status_end_date,
    effective_start_date,
    effective_end_date,
    device_log_rptg_class_cd,
    create_date,
    create_user,
    modify_date,
    modify_user,
    repeat_offender,
    offense_date,
    iid_start_date,
    iid_end_date,
    created_at
from {{ ref('customer_cleaned') }}
where is_inconsistent = 0
{% if is_incremental() %}
    and customer_dw_id not in (select customer_dw_id from {{ this }})
{% endif %}
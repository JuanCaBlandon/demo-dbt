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
    customer_dw_id,
    customer_reporting_state_id,
    customer_id,
    drivers_license_number,
    first_name,
    last_name,
    middle_name,
    date_of_birth,
    vin,
    install_date_confirmed,
    deinstall_date_confirmed,
    device_log_rptg_class_cd,
    active_status,
    state_code,
    report_status_cd,
    effectice_start_date,
    effectice_end_date,
    first_report_date,
    stop_report_date,
    create_user,
    modify_date,
    modify_user,
    created_at
from {{ ref('customer_cleaned') }}
where is_inconsistent = 0
{% if is_incremental() %}
    and customer_dw_id not in (select customer_dw_id from {{ this }})
{% endif %}
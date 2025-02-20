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


SELECT
    CC.customer_dw_id,
    CC.customer_reporting_state_id,
    CC.customer_id,
    CC.drivers_license_number,
    CC.first_name,
    CC.last_name,
    CC.middle_name,
    CC.date_of_birth,
    CC.vin,
    CC.install_date,
    CC.deinstall_date,
    CC.state_code,
    CC.active_status,
    CASE
        WHEN BCC.RepeatOffender = 1 AND OffenseDate >= "{{ var("start_date", "2025-01-01") }}" THEN 'Active-Reported'
        ELSE 'Active-NotReported' 
    END AS ReportStatusCD,
    CC.customer_status,
    CC.active_status_start_date,
    CC.active_status_end_date,
    CC.effective_start_date,
    CC.effective_end_date,
    CC.device_log_rptg_class_cd,
    CC.create_date,
    CC.create_user,
    CC.modify_date,
    CC.modify_user,
    BCC.repeat_offender,
    BCC.offense_date,
    BCC.iid_start_date,
    BCC.iid_end_date,
    created_at
FROM {{ ref('customer_cleaned') }} CC
INNER JOIN {{ ref('batch_customer_cleaned') }} BCC
    ON CC.drivers_license_number = BCC.drivers_license_number
    AND RIGHT(CC.vin, 6) = RIGHT(BCC.vin, 6)
    AND BCC.created_at = (SELECT MAX(created_at) FROM {{ ref('batch_customer_cleaned') }}) 
WHERE is_inconsistent = 0
{% if is_incremental() %}
    AND customer_dw_id NOT IN (SELECT customer_dw_id FROM {{ this }})
{% endif %}
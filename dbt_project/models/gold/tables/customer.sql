{{ config(
    materialized='incremental',
    unique_key='customer_dw_id',
    incremental_strategy='merge',
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
        CASE
            WHEN BC.repeat_offender = 1 AND BC.offense_date >= '{{ var("start_date", "2025-01-01") }}' THEN 1
            ELSE 0
        END AS active_status,
        CASE
            WHEN BC.repeat_offender = 1 AND BC.offense_date >= '{{ var("start_date", "2025-01-01") }}' THEN 'Active-Reported'
            ELSE 'Active-NotReported'
        END AS new_report_status_cd,
        CC.customer_status,
        CASE
            WHEN BC.repeat_offender = 1 AND BC.offense_date >= '{{ var("start_date", "2025-01-01") }}' THEN '{{ var("execution_date") }}'
            ELSE CC.active_status_start_date
        END AS new_active_status_start_date,
        CC.active_status_end_date,
        NULL AS active_status_end_type,
        CC.effective_start_date,
        CC.effective_end_date,
        CC.device_log_rptg_class_cd,
        CC.create_date,
        CC.create_user,
        CC.modify_date,
        CC.modify_user,
        BC.repeat_offender,
        BC.offense_date,
        BC.iid_start_date,
        BC.iid_end_date,
        CC.created_at,
        CC.modification_date,
        CC.created_at,
        CC.is_current
    FROM {{ ref('customer_cleaned') }} CC
    WHERE
        AND CC.is_inconsistent = 0

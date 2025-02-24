-- models/gold/customer_status.sql
{{ config(
    materialized='incremental',
    unique_key='customer_dw_id',
    incremental_strategy='merge',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY customer_dw_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ]
) }}

WITH current_customers AS (
    SELECT *
    FROM {{ this }}
),

new_customers_status AS (
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
        CASE
            WHEN BC.repeat_offender = 1 AND BC.offense_date >= '{{ var("start_date", "2025-01-01") }}' THEN '{{ var("execution_date_date") }}'
            ELSE CC.active_status_start_date
        END AS new_active_status_start_date,
        CC.active_status_end_date,
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
        CC.created_at
    FROM {{ ref('customer_cleaned') }} CC
    LEFT JOIN {{ ref('batch_customer') }} BC
        ON CC.drivers_license_number = BC.drivers_license_number
        AND RIGHT(CC.vin, 6) = RIGHT(BC.vin, 6)
        AND BC.created_at = (SELECT MAX(created_at) FROM {{ ref('batch_customer_cleaned') }})
    WHERE CC.is_inconsistent = 0
),

inactive_customers AS (
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
        0 AS active_status,
        'Inactive' AS report_status_cd,
        CC.active_status_start_date,
        '{{ var("execution_date_date") }}' AS active_status_end_date,
        CC.effective_start_date,
        CC.effective_end_date,
        CC.device_log_rptg_class_cd,
        CC.create_date,
        CC.create_user,
        CC.modify_date,
        CC.modify_user,
        CC.repeat_offender,
        CC.offense_date,
        CC.iid_start_date,
        CC.iid_end_date,
        CC.created_at
    FROM current_customers CC
    LEFT JOIN new_customers_status NCS 
        USING(customer_dw_id)
    WHERE
        NCS.customer_dw_id IS NULL
        AND CC.repeat_offender = 0
)

SELECT
    NCS.customer_dw_id,
    NCS.customer_reporting_state_id,
    NCS.customer_id,
    NCS.drivers_license_number,
    NCS.first_name,
    NCS.last_name,
    NCS.middle_name,
    NCS.date_of_birth,
    NCS.vin,
    NCS.install_date,
    NCS.deinstall_date,
    NCS.state_code,
    NCS.active_status,
    CASE
        WHEN CC.report_status_cd = 'Active-Reported' THEN CC.report_status_cd
        ELSE NCS.new_report_status_cd
    END AS report_status_cd,
    CASE
        WHEN CC.active_status_start_date IS NOT NULL THEN CC.active_status_start_date
        ELSE NCS.new_active_status_start_date
    END AS active_status_start_date,
    NCS.active_status_end_date,
    NCS.effective_start_date,
    NCS.effective_end_date,
    NCS.device_log_rptg_class_cd,
    NCS.create_date,
    NCS.create_user,
    NCS.modify_date,
    NCS.modify_user,
    NCS.repeat_offender,
    NCS.offense_date,
    NCS.iid_start_date,
    NCS.iid_end_date,
    NCS.created_at
FROM new_customers_status NCS
LEFT JOIN current_customers CC
    ON NCS.customer_dw_id = CC.customer_dw_id

UNION ALL

SELECT *
FROM inactive_customers
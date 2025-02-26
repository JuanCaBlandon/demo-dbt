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
    {% if is_incremental() %}
        SELECT *
        FROM {{ this }}
    {% else %}
        SELECT
            NULL AS customer_dw_id,
            NULL AS customer_reporting_state_id,
            NULL AS customer_id,
            NULL AS drivers_license_number,
            NULL AS first_name,
            NULL AS last_name,
            NULL AS middle_name,
            NULL AS date_of_birth,
            NULL AS vin,
            NULL AS install_date,
            NULL AS deinstall_date,
            NULL AS state_code,
            NULL AS active_status,
            NULL AS report_status_cd,
            NULL AS customer_status,
            NULL AS active_status_start_date,
            NULL AS active_status_end_date,
            NULL AS active_status_end_type,
            NULL AS effective_start_date,
            NULL AS effective_end_date,
            NULL AS device_log_rptg_class_cd,
            NULL AS create_date,
            NULL AS create_user,
            NULL AS modify_date,
            NULL AS modify_user,
            NULL AS repeat_offender,
            NULL AS offense_date,
            NULL AS iid_start_date,
            NULL AS iid_end_date,
            NULL AS created_at,
            NULL AS modification_date,
            NULL AS is_current
    {% endif %}
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
    LEFT JOIN {{ ref('batch_customer') }} BC
        ON CC.drivers_license_number = BC.drivers_license_number
        AND RIGHT(CC.vin, 6) = RIGHT(BC.vin, 6)
        AND BC.created_at = (SELECT MAX(created_at) FROM {{ ref('batch_customer') }})
    WHERE
        CC.is_current = 1 
        AND CC.is_inconsistent = 0
),

inactive_customers AS (
    SELECT
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
        0 AS active_status,
        'Inactive' AS report_status_cd,
        customer_status,
        new_active_status_start_date AS active_status_start_date,
        '{{ var("execution_date") }}' AS active_status_end_date,
        'Not repeat offender' AS active_status_end_type,
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
        created_at,
        modification_date,
        NCS.is_current
    FROM new_customers_status NCS
    WHERE
        repeat_offender = 0 
        AND (
            modification_date < '{{ var("execution_date") }}'
            OR (modification_date IS NULL AND created_at <> '{{ var("execution_date") }}')
        )
),

deprecated_customers (
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
        CC.report_status_cd,
        CC.customer_status,
        CC.active_status_start_date,
        CC.active_status_end_date,
        CC.active_status_end_type,
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
        CC.created_at,
        CC.modification_date,
        0 AS is_current
    FROM current_customers CC
    INNER JOIN new_customers_status NCS
        ON CC.customer_id = NCS.customer_id
        AND CC.customer_dw_id != NCS.customer_dw_id
),

updated_customers (
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
        CC.active_status,
        CC.report_status_cd,
        NCS.customer_status,
        CC.active_status_start_date,
        CC.active_status_end_date,
        CC.active_status_end_type,
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
        NCS.created_at,
        NCS.modification_date,
        1 AS is_current
     FROM new_customers_status NCS
    INNER JOIN current_customers CC
        ON NCS.customer_id = CC.customer_id
        AND NCS.customer_dw_id != CC.customer_dw_id
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
    CASE
        WHEN CAST(CC.active_status AS INT) = 1 THEN CAST(CC.active_status AS INT)
        ELSE NCS.active_status
    END AS active_status,
    CASE
        WHEN CC.report_status_cd = 'Active-Reported' THEN CC.report_status_cd
        ELSE NCS.new_report_status_cd
    END AS report_status_cd,
    NCS.customer_status,
    CASE
        WHEN CC.active_status_start_date IS NOT NULL THEN CC.active_status_start_date
        ELSE NCS.new_active_status_start_date
    END AS active_status_start_date,
    NCS.active_status_end_date,
    NCS.active_status_end_type,
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
    NCS.created_at,
    NCS.modification_date,
    NCS.is_current
FROM new_customers_status NCS
LEFT JOIN current_customers CC
    ON NCS.customer_dw_id = CC.customer_dw_id
LEFT JOIN inactive_customers IC
    ON NCS.customer_dw_id = IC.customer_dw_id
LEFT JOIN updated_customers UC
    ON NCS.customer_dw_id = UC.customer_dw_id
WHERE 
    IC.customer_dw_id IS NULL
    AND UC.customer_dw_id IS NULL

UNION ALL
SELECT *
FROM inactive_customers

UNION ALL
SELECT *
FROM deprecated_customers

UNION ALL
SELECT *
FROM updated_customers
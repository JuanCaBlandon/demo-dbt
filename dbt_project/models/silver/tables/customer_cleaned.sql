{{ config(
		materialized='incremental',
    unique_key='customer_dw_id',
    incremental_strategy='merge',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY customer_id ;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
) }}

WITH Tmp AS(
  SELECT
    CustomerReportingStateID AS customer_reporting_state_id,
    CustomerID AS customer_id,
    DriversLicenseNumber AS drivers_license_number,
    FirstName AS first_name,
    LastName AS last_name,
    MiddleName AS middle_name,
    DateOfBirth AS date_of_birth,
    VIN AS vin,
    InstallDate AS install_date,
    DeInstallDate AS deinstall_date,
    StateCode AS state_code,
    ActiveStatus AS active_status,
    ReportStatusCd AS report_status_cd,
    CustomerStatus AS customer_status,
    ActiveStatusStartDate AS active_status_start_date,
    ActiveStatusEndDate AS active_status_end_date,
    EffectiveStartDate AS effective_start_date,
    EffectiveEndDate AS effective_end_date,
    DeviceLogRptgClassCd AS device_log_rptg_class_cd,
    CreateDate AS create_date,
    CreateUser AS create_user,
    ModifyDate AS modify_date,
    ModifyUser AS modify_user,
    CreationDate AS created_at,
    ROW_NUMBER() OVER (PARTITION BY CustomerID, DriversLicenseNumber, VIN, StateCode, EffectiveStartDate, EffectiveEndDate ORDER BY EffectiveStartDate) AS num_duplicates
  FROM {{ source('BRONZE', 'customer_raw') }}
  WHERE
    StateCode = 'IA'
  {% if is_incremental() %}
      AND CreationDate >= COALESCE((SELECT MAX(created_at) from {{ this }}),"{{ var("start_date", "2025-01-01") }}")
  {% endif %}



),
cleaned_data AS(

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id','drivers_license_number', 'first_name','last_name','date_of_birth','vin','effective_start_date','num_duplicates']) }} AS customer_dw_id,
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
    created_at,
    1 AS is_inconsistent,
    'duplicates' AS type_inconsistent,
    num_duplicates
FROM Tmp
WHERE num_duplicates > 1

UNION ALL
SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id','drivers_license_number', 'first_name','last_name','date_of_birth','vin','effective_start_date','num_duplicates']) }} AS customer_dw_id,
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
    created_at,
    1 AS is_inconsistent,
    'NULL values' AS type_inconsistent,
    num_duplicates
FROM Tmp
WHERE num_duplicates = 1 AND 
  (drivers_license_number IS NULL OR first_name IS NULL OR last_name IS NULL  OR date_of_birth IS NULL OR vin IS NULL)

UNION ALL
SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id','drivers_license_number', 'first_name','last_name','date_of_birth','vin','effective_start_date','num_duplicates']) }} AS customer_dw_id,
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
    created_at,
    1 AS is_inconsistent,
    'Max Character Limit' AS type_inconsistent,
    num_duplicates
FROM Tmp
WHERE num_duplicates = 1 AND (
    drivers_license_number IS NOT NULL  AND LENGTH(drivers_license_number) > 50 OR 
    first_name IS NOT NULL AND LENGTH(first_name) > 80 OR 
    last_name IS NOT NULL AND LENGTH(last_name) > 80 OR 
    date_of_birth IS NOT NULL AND LENGTH(date_of_birth) > 10 OR 
    vin IS NOT NULL AND LENGTH(vin) > 30
  )


UNION ALL
SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id','drivers_license_number', 'first_name','last_name','date_of_birth','vin','effective_start_date','num_duplicates']) }} AS customer_dw_id,
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
    created_at,
    1 AS is_inconsistent,
    'dont Match with batch file' AS type_inconsistent,
    num_duplicates
FROM Tmp
WHERE num_duplicates = 1
  AND drivers_license_number IS NOT NULL
  AND first_name IS NOT NULL 
  AND last_name IS NOT NULL 
  AND date_of_birth IS NOT NULL
  AND vin IS NOT NULL
)


SELECT
  *
FROM cleaned_data
{% if is_incremental() %}
  WHERE customer_dw_id NOT IN (SELECT c.customer_dw_id FROM {{ this }} c)
{% endif %}

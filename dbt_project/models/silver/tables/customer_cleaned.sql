{{ config(
		materialized='incremental',
    unique_key='customer_dw_id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY customer_id ;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
) }}

with Tmp as(
SELECT
  CustomerReportingStateID AS customer_reporting_state_id,
  CustomerID AS customer_id,
  DriversLicenseNumber AS drivers_license_number,
  FirstName AS first_name,
  LastName AS last_name,
  MiddleName AS middle_name,
  DateOfBirth AS date_of_birth,
  VIN AS vin,
  InstallDateConfirmed AS install_date_confirmed,
  DeInstallDateConfirmed AS deinstall_date_confirmed,
  DeviceLogRptgClassCd AS device_log_rptg_class_cd,
  ActiveStatus AS active_status,
  StateCode AS state_code,
  ReportStatusCd AS report_status_cd,
  EffectiveStartDate AS effectice_start_date,
  EffectiveEndDate AS effectice_end_date,
  FirstReportDate AS first_report_date,
  StopReportDate AS stop_report_date,
  CreateUser AS create_user,
  ModifyDate AS modify_date,
  ModifyUser AS modify_user,
  CreateDate As create_date,
  current_timestamp() as created_at,
  row_number() over (partition by CustomerID, DriversLicenseNumber,VIN,StateCode, EffectiveStartDate, EffectiveEndDate order by EffectiveStartDate) as num_duplicates
FROM
  {{ source('BRONZE', 'state_reported_customer') }}
WHERE StateCode = 'IA' and creation_date >= (select MAX(created_at) from {{ this }})


),
cleaned_data AS(

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id','drivers_license_number', 'first_name','last_name','date_of_birth','vin','effectice_start_date','num_duplicates']) }} as customer_dw_id,
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
    created_at,
    1 as is_inconsistent,
    'duplicates' as type_inconsistent,
    num_duplicates
FROM Tmp
WHERE num_duplicates > 1

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id','drivers_license_number', 'first_name','last_name','date_of_birth','vin','effectice_start_date','num_duplicates']) }} as customer_dw_id,
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
    created_at,
    1 as is_inconsistent,
    'NULL values' as type_inconsistent,
    num_duplicates
FROM Tmp
WHERE num_duplicates = 1 AND 
  (drivers_license_number IS NULL OR first_name IS NULL OR last_name IS NULL  OR date_of_birth IS NULL OR vin IS NULL)

UNION ALL

SELECT
      {{ dbt_utils.generate_surrogate_key(['customer_id','drivers_license_number', 'first_name','last_name','date_of_birth','vin','effectice_start_date','num_duplicates']) }} as customer_dw_id,
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
    created_at,
    0 as is_inconsistent,
    'N/A' as type_inconsistent,
    num_duplicates
FROM Tmp
WHERE num_duplicates = 1 AND drivers_license_number IS NOT NULL AND first_name IS NOT NULL 
  AND last_name IS NOT NULL  AND date_of_birth IS NOT NULL AND vin IS NOT NULL

)


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
    created_at,
    is_inconsistent,
    type_inconsistent,
    num_duplicates
  FROM cleaned_data
  {% if is_incremental() %}
    where customer_dw_id not in (select customer_dw_id from {{ this }})
{% endif %}





{{ config(
		materialized = 'table' , 
		post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY customer_id , creation_date ;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
) }}

with Tmp as(
SELECT
  srcus.StateReportedCustomerId,
  srcus.CustomerID,
  srcus.DriversLicenseNumber,
  srcus.FirstName,
  srcus.LastName,
  srcus.MiddleName,
  srcus.DateOfBirth,
  srcus.VIN,
  srcus.InstallDateConfirmed,
  srcus.DeInstallDateConfirmed,
  srcus.DeviceLogRptgClassCd,
  srcus.ActiveStatus,
  srcus.StateCode,
  srcus.ReportStatusCd,
  srcus.FirstReportDate,
  srcus.StopReportDate,
  srcus.CreateDate,
  srcus.CreateUser,
  srcus.ModifyDate,
  srcus.ModifyUser,
  afcus.Offense_Date,
  afcus.Repeat_Offender,
  afcus.IID_Start_Date,
  afcus.IID_End_Date
FROM
  {{ source('BRONZE', 'state_reported_customer') }} as srcus
INNER JOIN
  {{ source('BRONZE', 'aditional_field_customer_ia') }} afcus
  ON
    srcus.DriversLicenseNumber = afcus.DriversLicenseNumber AND
    srcus.LastName = afcus.LastName AND
    srcus.FirstName = afcus.FirstName AND
    srcus.VIN = afcus.VIN

)

SELECT
    StateReportedCustomerId AS state_reported_customer_id,
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
    FirstReportDate AS first_report_date,
    StopReportDate AS stop_report_date,
    
    CreateUser AS create_user,
    ModifyDate AS modify_date,
    ModifyUser AS modify_user,
    -- from batch files
    (case when Repeat_Offender > 1 then 1 else 0 end )AS feed_reporting_flag,
    Offense_Date AS offense_date,
    IID_Start_Date AS iid_start_date,
    IID_End_Date AS iid_end_date,
    CreateDate AS create_date,
FROM Tmp
Order by creation_date asc

{{ config(
		materialized='incremental',
    unique_key='batch_customer_dw_id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY offense_date ;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
) }}

with Tmp AS (
  SELECT
    VendorName AS vendor_name,
    DriversLicenseNumber AS drivers_license_number,
    FirstName AS first_name,
    LastName AS last_name,
    MiddleName AS middle_name,
    DateOfBirth AS date_of_birth,
    VIN AS vin,
    OffenseDate AS offense_date,
    CASE WHEN RepeatOffender = 1 AND OffenseDate >= "{{ var("start_date", "2025-01-01") }}" THEN 1 ELSE 0 END AS repeat_offender,
    IIDStartDate AS IID_Start_Date,
    IIDEndDate AS IID_End_Date,
    CreatedAt AS created_at,
    row_number() OVER (PARTITION BY VendorName,DriversLicenseNumber ,FirstName, LastName, MiddleName, DateOfBirth, VIN, OffenseDate, RepeatOffender ORDER BY OffenseDate) AS num_duplicates
  FROM
    {{ source('BRONZE', 'state_batch_customer_data_ia') }}
  WHERE
    CreatedAt >= (SELECT MAX(CreatedAt) FROM {{ source('BRONZE', 'state_batch_customer_data_ia') }})


),
cleaned_data AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['vendor_name','drivers_license_number','first_name','last_name','middle_name','date_of_birth', 'vin', 'offense_date', 'created_at']) }} AS batch_customer_dw_id, 
    'N/A' AS customer_dw_id,
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
    created_at,
    1 AS is_inconsistent,
    'duplicates' AS type_inconsistent,
    num_duplicates
  FROM Tmp
  WHERE num_duplicates > 1


UNION ALL

SELECT
  {{ dbt_utils.generate_surrogate_key(['vendor_name','drivers_license_number','first_name','last_name','middle_name','date_of_birth', 'vin', 'offense_date', 'created_at']) }} AS batch_customer_dw_id,
  'N/A' AS customer_dw_id,
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
  created_at,
  1 AS is_inconsistent,
  'NULL values' AS type_inconsistent,
  num_duplicates
FROM Tmp
WHERE num_duplicates = 1 AND 
  (drivers_license_number IS NULL OR first_name IS NULL OR last_name IS NULL  OR date_of_birth IS NULL OR vin IS NULL)

UNION ALL

SELECT
  {{ dbt_utils.generate_surrogate_key(['vendor_name','drivers_license_number','first_name','last_name','middle_name','date_of_birth', 'vin', 'offense_date', 'created_at']) }} AS batch_customer_dw_id,
  'N/A' AS customer_dw_id,
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
  created_at,
  1 AS is_inconsistent,
  'repeat offender without dates' AS type_inconsistent,
  num_duplicates
FROM Tmp
WHERE num_duplicates = 1 AND 
repeat_offender = 1 AND  (IID_Start_Date IS NULL OR IID_End_Date IS NULL )

UNION ALL

SELECT
  {{ dbt_utils.generate_surrogate_key(['vendor_name','drivers_license_number','first_name','last_name','middle_name','date_of_birth', 'vin', 'offense_date', 'created_at']) }} AS batch_customer_dw_id,
  'N/A' AS customer_dw_id,
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
  created_at,
  1 AS is_inconsistent,
  'no repeat offender with dates' AS type_inconsistent,
  num_duplicates
FROM Tmp
WHERE num_duplicates = 1 AND 
repeat_offender = 0 AND  (IID_Start_Date IS NOT NULL OR IID_End_Date IS NOT NULL )

  UNION ALL
  SELECT
      {{ dbt_utils.generate_surrogate_key(['vendor_name','Tmp.drivers_license_number','Tmp.first_name','Tmp.last_name','Tmp.middle_name','Tmp.date_of_birth', 'Tmp.vin', 'Tmp.offense_date', 'Tmp.created_at']) }} AS batch_customer_dw_id,
      'N/A' AS customer_dw_id,
      vendor_name,
      Tmp.drivers_license_number,
      Tmp.first_name,
      Tmp.last_name,
      Tmp.middle_name,
      Tmp.date_of_birth,
      Tmp.vin,
      Tmp.offense_date,
      Tmp.repeat_offender,
      Tmp.IID_Start_Date,
      Tmp.IID_End_Date,
      Tmp.created_at,
      1 AS is_inconsistent,
      'Without reference entity' AS type_inconsistent,
      Tmp.num_duplicates
  FROM Tmp
  LEFT JOIN {{ ref('customer_cleaned') }} AS c
    ON  UPPER(Tmp.drivers_license_number) = UPPER(c.drivers_license_number)
    AND UPPER(RIGHT(Tmp.vin,6)) = UPPER(RIGHT(c.vin, 6))
  WHERE 
    Tmp.num_duplicates = 1
    AND Tmp.drivers_license_number IS NOT NULL
    AND Tmp.first_name IS NOT NULL 
    AND Tmp.last_name IS NOT NULL
    AND Tmp.date_of_birth IS NOT NULL
    AND Tmp.vin IS NOT NULL
    AND c.drivers_license_number IS NULL
    AND (c.customer_id IS NULL or c.active_status = false)
    

  UNION ALL
  SELECT
      {{ dbt_utils.generate_surrogate_key(['vendor_name','Tmp.drivers_license_number','Tmp.first_name','Tmp.last_name','Tmp.middle_name','Tmp.date_of_birth', 'Tmp.vin', 'Tmp.offense_date', 'Tmp.created_at']) }} AS batch_customer_dw_id,
      c.customer_dw_id,
      Tmp.vendor_name,
      Tmp.drivers_license_number,
      Tmp.first_name,
      Tmp.last_name,
      Tmp.middle_name,
      Tmp.date_of_birth,
      Tmp.vin,
      Tmp.offense_date,
      Tmp.repeat_offender,
      Tmp.IID_Start_Date,
      Tmp.IID_End_Date,
      Tmp.created_at,
      0 AS is_inconsistent,
      'N/A' AS type_inconsistent,
      Tmp.num_duplicates
  FROM Tmp
  INNER JOIN {{ ref('customer_cleaned') }} AS c
    ON  UPPER(Tmp.drivers_license_number) = UPPER(c.drivers_license_number)
    AND UPPER(RIGHT(Tmp.vin,6)) = UPPER(RIGHT(c.vin, 6))
  WHERE
    Tmp.num_duplicates = 1
    AND Tmp.drivers_license_number IS NOT NULL
    AND Tmp.first_name IS NOT NULL 
    AND Tmp.last_name IS NOT NULL
    AND Tmp.date_of_birth IS NOT NULL
    AND Tmp.vin IS NOT NULL
    AND c.is_inconsistent = 0
    AND c.active_status = true

),
cleaned_data2 as (

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
    created_at,
    is_inconsistent,
    type_inconsistent,
    num_duplicates,
    CASE WHEN  repeat_offender = 1 AND  (IID_Start_Date IS  NULL OR IID_End_Date IS  NULL ) then 1 else 0  end as ro_without_dates
  FROM cleaned_data
)

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
    created_at,
    is_inconsistent,
    type_inconsistent,
    num_duplicates
  FROM cleaned_data2
  WHERE ro_without_dates = 0
  {% if is_incremental() %}
    where batch_customer_dw_id not in (select batch_customer_dw_id from {{ this }})
{% endif %}

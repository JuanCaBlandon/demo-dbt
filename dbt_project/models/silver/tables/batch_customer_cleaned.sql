{{ config(
		materialized='incremental',
    unique_key='batch_customer_dw_id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY offense_date ;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
) }}

with Tmp as(
SELECT
  vendor_name,
  DriversLicenseNumber AS drivers_license_number,
  FirstName AS first_name,
  LastName AS last_name,
  MiddleName AS middle_name,
  DateOfBirth AS date_of_birth,
  VIN AS vin,
  offense_date,
  case when repeat_offender = 1 and offense_date >= "{{ var("start_date", "2024-01-01") }}" then 1 else 0 end as repeat_offender,
  IID_Start_Date as IID_Start_Date,
  IID_End_Date as IID_End_Date,
  created_at,
  row_number() over (partition by vendor_name,DriversLicenseNumber ,FirstName, LastName, MiddleName, DateOfBirth, VIN, offense_date, repeat_offender order by offense_date) as num_duplicates
FROM
  {{ source('BRONZE', 'state_batch_customer_data_ia') }}
WHERE
  created_at >= (SELECT MAX(created_at) FROM {{ source('BRONZE', 'state_batch_customer_data_ia') }})


),
cleaned_data AS(

SELECT
  {{ dbt_utils.generate_surrogate_key(['vendor_name','drivers_license_number','first_name','last_name','middle_name','date_of_birth', 'vin', 'offense_date']) }} as batch_customer_dw_id, 
  'N/A' as customer_dw_id,
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
  1 as is_inconsistent,
  'duplicates' as type_inconsistent,
  num_duplicates
FROM Tmp
WHERE num_duplicates > 1

UNION ALL

SELECT
  {{ dbt_utils.generate_surrogate_key(['vendor_name','drivers_license_number','first_name','last_name','middle_name','date_of_birth', 'vin', 'offense_date']) }} as batch_customer_dw_id,
  'N/A' as customer_dw_id,
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
  1 as is_inconsistent,
  'NULL values' as type_inconsistent,
  num_duplicates
FROM Tmp
WHERE num_duplicates = 1 AND 
  (drivers_license_number IS NULL OR first_name IS NULL OR last_name IS NULL  OR date_of_birth IS NULL OR vin IS NULL)

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(['vendor_name','Tmp.drivers_license_number','Tmp.first_name','Tmp.last_name','Tmp.middle_name','Tmp.date_of_birth', 'Tmp.vin', 'Tmp.offense_date']) }} as batch_customer_dw_id,
    'N/A' as customer_dw_id,
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
    1 as is_inconsistent,
    'Without reference entity' as type_inconsistent,
    Tmp.num_duplicates
FROM Tmp
LEFT JOIN {{ ref('customer_cleaned') }} as c
ON  Tmp.drivers_license_number = c.drivers_license_number AND
    Tmp.first_name= c.first_name AND
    Tmp.last_name =c.last_name AND
    Tmp.date_of_birth = c.date_of_birth AND
    Tmp.vin = c.vin
WHERE Tmp.num_duplicates = 1 AND Tmp.drivers_license_number IS NOT NULL AND Tmp.first_name IS NOT NULL 
  AND Tmp.last_name IS NOT NULL  AND Tmp.date_of_birth IS NOT NULL AND Tmp.vin IS NOT NULL
  AND c.drivers_license_number IS NULL
  AND (c.customer_id IS NULL or c.active_status = false)
  
UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(['vendor_name','Tmp.drivers_license_number','Tmp.first_name','Tmp.last_name','Tmp.middle_name','Tmp.date_of_birth', 'Tmp.vin', 'Tmp.offense_date']) }} as batch_customer_dw_id,
    c.customer_dw_id,
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
    0 as is_inconsistent,
    'N/A' as type_inconsistent,
    Tmp.num_duplicates
FROM Tmp
INNER JOIN {{ ref('customer_cleaned') }} as c
ON  Tmp.drivers_license_number = c.drivers_license_number AND
    Tmp.first_name= c.first_name AND
    Tmp.last_name =c.last_name AND
    Tmp.date_of_birth = c.date_of_birth AND
    Tmp.vin = c.vin
WHERE Tmp.num_duplicates = 1 AND Tmp.drivers_license_number IS NOT NULL AND Tmp.first_name IS NOT NULL 
  AND Tmp.last_name IS NOT NULL  AND Tmp.date_of_birth IS NOT NULL AND Tmp.vin IS NOT NULL
  AND c.is_inconsistent = 0 and c.active_status = true

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
  FROM cleaned_data
  {% if is_incremental() %}
    where batch_customer_dw_id not in (select batch_customer_dw_id from {{ this }})
{% endif %}

{{ config(
		materialized='incremental',
    unique_key='batch_customer_dw_id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY offense_date ;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ],
    tags=["silver_ia_1"]
) }}

WITH Tmp AS (
  SELECT
    VendorName AS vendor_name,
    UPPER(DriversLicenseNumber) AS drivers_license_number,
    FirstName AS first_name,
    LastName AS last_name,
    MiddleName AS middle_name,
    DateOfBirth AS date_of_birth,
    UPPER(VIN) AS vin,
    OffenseDate AS offense_date,
    RepeatOffender AS repeat_offender,
    IIDStartDate AS iid_start_date,
    IIDEndDate AS iid_end_date,
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
    vendor_name,
    drivers_license_number,
    first_name,
    last_name,
    middle_name,
    date_of_birth,
    vin,
    offense_date,
    repeat_offender,
    iid_start_date,
    iid_end_date,
    created_at,
    1 AS is_inconsistent,
    1 AS inconsistency_id, --Duplicates
    num_duplicates
  FROM Tmp
  WHERE num_duplicates > 1


  UNION ALL
  SELECT
    {{ dbt_utils.generate_surrogate_key(['vendor_name','drivers_license_number','first_name','last_name','middle_name','date_of_birth', 'vin', 'offense_date', 'created_at']) }} AS batch_customer_dw_id,
    vendor_name,
    drivers_license_number,
    first_name,
    last_name,
    middle_name,
    date_of_birth,
    vin,
    offense_date,
    repeat_offender,
    iid_start_date,
    iid_end_date,
    created_at,
    1 AS is_inconsistent,
    2 AS inconsistency_id, --Null values
    num_duplicates
  FROM Tmp
  WHERE num_duplicates = 1 AND 
    (drivers_license_number IS NULL OR first_name IS NULL OR last_name IS NULL  OR date_of_birth IS NULL OR vin IS NULL)
    AND NOT (
      (repeat_offender = 1 AND (iid_start_date IS NULL OR offense_date IS NULL))
      OR repeat_offender = 0 AND  (iid_start_date IS NOT NULL OR iid_end_date IS NOT NULL OR offense_date IS NOT NULL)
    )


  UNION ALL
  SELECT
    {{ dbt_utils.generate_surrogate_key(['vendor_name','drivers_license_number','first_name','last_name','middle_name','date_of_birth', 'vin', 'offense_date', 'created_at']) }} AS batch_customer_dw_id,
    vendor_name,
    drivers_license_number,
    first_name,
    last_name,
    middle_name,
    date_of_birth,
    vin,
    offense_date,
    repeat_offender,
    iid_start_date,
    iid_end_date,
    created_at,
    1 AS is_inconsistent,
    3 AS inconsistency_id, -- Repeat offender without dates
    num_duplicates
  FROM Tmp
  WHERE num_duplicates = 1 AND 
  repeat_offender = 1 AND (iid_start_date IS NULL OR offense_date IS NULL)


  UNION ALL
  SELECT
    {{ dbt_utils.generate_surrogate_key(['vendor_name','drivers_license_number','first_name','last_name','middle_name','date_of_birth', 'vin', 'offense_date', 'created_at']) }} AS batch_customer_dw_id,
    vendor_name,
    drivers_license_number,
    first_name,
    last_name,
    middle_name,
    date_of_birth,
    vin,
    offense_date,
    repeat_offender,
    iid_start_date,
    iid_end_date,
    created_at,
    1 AS is_inconsistent,
    4 AS inconsistency_id, -- Not repeat offender with dates
    num_duplicates
  FROM Tmp
  WHERE num_duplicates = 1 AND 
  repeat_offender = 0 AND  (iid_start_date IS NOT NULL OR iid_end_date IS NOT NULL OR offense_date IS NOT NULL)
    

  UNION ALL
  SELECT
      {{ dbt_utils.generate_surrogate_key(['vendor_name','drivers_license_number','first_name','last_name','middle_name','date_of_birth', 'vin', 'offense_date', 'created_at']) }} AS batch_customer_dw_id,
      vendor_name,
      drivers_license_number,
      first_name,
      last_name,
      middle_name,
      date_of_birth,
      vin,
      offense_date,
      repeat_offender,
      iid_start_date,
      iid_end_date,
      created_at,
      0 AS is_inconsistent,
      0 AS inconsistency_id,
      num_duplicates
  FROM Tmp
  WHERE
    num_duplicates = 1
    AND drivers_license_number IS NOT NULL
    AND first_name IS NOT NULL 
    AND last_name IS NOT NULL
    AND date_of_birth IS NOT NULL
    AND vin IS NOT NULL
    AND (
        (repeat_offender = 0 AND iid_start_date IS NULL AND iid_end_date IS NULL AND offense_date IS NULL)
        OR
        (repeat_offender = 1 AND (iid_start_date IS NOT NULL AND offense_date IS NOT NULL))
    )
)

SELECT
    batch_customer_dw_id,
    vendor_name,
    drivers_license_number,
    first_name,
    last_name,
    middle_name,
    date_of_birth,
    vin,
    offense_date,
    repeat_offender,
    iid_start_date,
    iid_end_date,
    created_at,
    is_inconsistent,
    inconsistency_id,
    num_duplicates
  FROM cleaned_data
  {% if is_incremental() %}
    where batch_customer_dw_id not in (select batch_customer_dw_id from {{ this }})
{% endif %}

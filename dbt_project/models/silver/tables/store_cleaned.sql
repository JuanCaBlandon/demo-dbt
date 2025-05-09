{{ config(
    materialized='incremental',
    unique_key='store_dw_id',
    incremental_strategy='merge',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY store_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
) }}

WITH Tmp AS(
  SELECT
    -- Limpiar los caracteres extraños del inicio
    `S t o r e I D ` AS store_id,
    ` S t o r e N a m e ` AS store_name,
    ` R e g i o n I D ` AS region_id,
    -- Limpiar los caracteres extraños del XML
    ` S u r v e y I n f o ` AS store_survey,
    ` G U I D ` AS guid,
    -- Eliminar el & del final
    REGEXP_REPLACE(` M o d i f i e d D a t e `, '&$', '') AS modified_date,
    -- Extraer información del XML StoreSurvey, omitiendo los caracteres extraños
    REGEXP_EXTRACT(REGEXP_REPLACE(` S u r v e y I n f o `, '^��', ''), '<AnnualSales>(.*?)</AnnualSales>', 1) AS annual_sales,
    REGEXP_EXTRACT(REGEXP_REPLACE(` S u r v e y I n f o `, '^��', ''), '<AnnualRevenue>(.*?)</AnnualRevenue>', 1) AS annual_revenue,
    REGEXP_EXTRACT(REGEXP_REPLACE(` S u r v e y I n f o `, '^��', ''), '<BankName>(.*?)</BankName>', 1) AS bank_name,
    REGEXP_EXTRACT(REGEXP_REPLACE(` S u r v e y I n f o `, '^��', ''), '<BusinessType>(.*?)</BusinessType>', 1) AS business_type,
    REGEXP_EXTRACT(REGEXP_REPLACE(` S u r v e y I n f o `, '^��', ''), '<YearOpened>(.*?)</YearOpened>', 1) AS year_opened,
    REGEXP_EXTRACT(REGEXP_REPLACE(` S u r v e y I n f o `, '^��', ''), '<Specialty>(.*?)</Specialty>', 1) AS specialty,
    REGEXP_EXTRACT(REGEXP_REPLACE(` S u r v e y I n f o `, '^��', ''), '<SquareFeet>(.*?)</SquareFeet>', 1) AS square_feet,
    REGEXP_EXTRACT(REGEXP_REPLACE(` S u r v e y I n f o `, '^��', ''), '<Brands>(.*?)</Brands>', 1) AS brands,
    REGEXP_EXTRACT(REGEXP_REPLACE(` S u r v e y I n f o `, '^��', ''), '<Internet>(.*?)</Internet>', 1) AS internet,
    REGEXP_EXTRACT(REGEXP_REPLACE(` S u r v e y I n f o `, '^��', ''), '<NumberEmployees>(.*?)</NumberEmployees>', 1) AS number_employees,
    ROW_NUMBER() OVER (PARTITION BY `S t o r e I D ` ORDER BY REGEXP_REPLACE(` M o d i f i e d D a t e `, '&$', '') DESC) AS num_duplicates
  FROM {{ source('BRONZE', 'store_raw') }}
),

cleaned_data AS(
  -- Primero agregamos el registro N/A para store
  SELECT
      {{ dbt_utils.generate_surrogate_key(['-1']) }} AS store_dw_id,
      '-1' AS store_id,
      'N/A' AS store_name,
      '-1' AS region_id,
      '<StoreSurvey xmlns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"><AnnualSales>0</AnnualSales><AnnualRevenue>0</AnnualRevenue><BankName>N/A</BankName><BusinessType>NA</BusinessType><YearOpened>2000</YearOpened><Specialty>N/A</Specialty><SquareFeet>0</SquareFeet><Brands>0</Brands><Internet>N/A</Internet><NumberEmployees>0</NumberEmployees></StoreSurvey>' AS store_survey,
      'N/A' AS guid,
      CURRENT_TIMESTAMP() AS modified_date,
      '0' AS annual_sales,
      '0' AS annual_revenue,
      'N/A' AS bank_name,
      'NA' AS business_type,
      '2000' AS year_opened,
      'N/A' AS specialty,
      '0' AS square_feet,
      '0' AS brands,
      'N/A' AS internet,
      '0' AS number_employees,
      0 AS is_inconsistent,
      0 AS inconsistency_id,
      1 AS num_duplicates,
      1 AS is_current

  UNION ALL
  
  -- Identificar duplicados
  SELECT
      {{ dbt_utils.generate_surrogate_key(['store_id', 'store_name', 'region_id', 'num_duplicates']) }} AS store_dw_id,
      store_id,
      store_name,
      region_id,
      store_survey,
      guid,
      modified_date,
      annual_sales,
      annual_revenue,
      bank_name,
      business_type,
      year_opened,
      specialty,
      square_feet,
      brands,
      internet,
      number_employees,
      1 AS is_inconsistent,
      1 AS inconsistency_id, -- Duplicados
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE num_duplicates > 1

  UNION ALL
  
  -- Identificar valores nulos en campos críticos
  SELECT
      {{ dbt_utils.generate_surrogate_key(['store_id', 'store_name', 'region_id', 'num_duplicates']) }} AS store_dw_id,
      store_id,
      store_name,
      region_id,
      store_survey,
      guid,
      modified_date,
      annual_sales,
      annual_revenue,
      bank_name,
      business_type,
      year_opened,
      specialty,
      square_feet,
      brands,
      internet,
      number_employees,
      1 AS is_inconsistent,
      2 AS inconsistency_id, -- Valores nulos
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE num_duplicates = 1 AND 
    (store_id IS NULL OR store_name IS NULL OR region_id IS NULL)

  UNION ALL
  
  -- Identificar valores fuera de rango o inválidos
  SELECT
      {{ dbt_utils.generate_surrogate_key(['store_id', 'store_name', 'region_id', 'num_duplicates']) }} AS store_dw_id,
      store_id,
      store_name,
      region_id,
      store_survey,
      guid,
      modified_date,
      annual_sales,
      annual_revenue,
      bank_name,
      business_type,
      year_opened,
      specialty,
      square_feet,
      brands,
      internet,
      number_employees,
      1 AS is_inconsistent,
      3 AS inconsistency_id, -- Valores fuera de rango
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE num_duplicates = 1 AND (
      TRY_CAST(store_id AS INT) <= 0 OR
      TRY_CAST(region_id AS INT) <= 0 OR
      TRY_CAST(annual_sales AS DOUBLE) < 0 OR
      TRY_CAST(annual_revenue AS DOUBLE) < 0 OR
      (TRY_CAST(year_opened AS INT) IS NOT NULL AND 
        (TRY_CAST(year_opened AS INT) < 1900 OR TRY_CAST(year_opened AS INT) > YEAR(CURRENT_DATE()))) OR
      (TRY_CAST(square_feet AS INT) IS NOT NULL AND TRY_CAST(square_feet AS INT) <= 0) OR
      (TRY_CAST(number_employees AS INT) IS NOT NULL AND TRY_CAST(number_employees AS INT) < 0) OR
      TRY_CAST(modified_date AS TIMESTAMP) > CURRENT_TIMESTAMP()  -- Fecha futura
    )

  UNION ALL
  
  -- Identificar registros con formato incorrecto o inconsistencias de negocio
  SELECT
      {{ dbt_utils.generate_surrogate_key(['store_id', 'store_name', 'region_id', 'num_duplicates']) }} AS store_dw_id,
      store_id,
      store_name,
      region_id,
      store_survey,
      guid,
      modified_date,
      annual_sales,
      annual_revenue,
      bank_name,
      business_type,
      year_opened,
      specialty,
      square_feet,
      brands,
      internet,
      number_employees,
      1 AS is_inconsistent,
      4 AS inconsistency_id, -- Formato incorrecto o inconsistencias de negocio
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE num_duplicates = 1 AND (
      (TRY_CAST(annual_sales AS DOUBLE) IS NOT NULL AND 
       TRY_CAST(annual_revenue AS DOUBLE) IS NOT NULL AND 
       TRY_CAST(annual_revenue AS DOUBLE) > TRY_CAST(annual_sales AS DOUBLE)) 
    )

  UNION ALL
  
  -- Datos correctos
  SELECT
      {{ dbt_utils.generate_surrogate_key(['store_id', 'store_name', 'region_id', 'num_duplicates']) }} AS store_dw_id,
      store_id,
      store_name,
      region_id,
      store_survey,
      guid,
      modified_date,
      annual_sales,
      annual_revenue,
      bank_name,
      business_type,
      year_opened,
      specialty,
      square_feet,
      brands,
      internet,
      number_employees,
      0 AS is_inconsistent,
      0 AS inconsistency_id, -- Sin inconsistencias
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE 
    num_duplicates = 1 AND
    store_id IS NOT NULL AND TRY_CAST(store_id AS INT) > 0 AND
    store_name IS NOT NULL AND
    region_id IS NOT NULL AND TRY_CAST(region_id AS INT) > 0 AND
    (annual_sales IS NULL OR TRY_CAST(annual_sales AS DOUBLE) >= 0) AND
    (annual_revenue IS NULL OR TRY_CAST(annual_revenue AS DOUBLE) >= 0) AND
    (annual_sales IS NULL OR annual_revenue IS NULL OR 
     TRY_CAST(annual_revenue AS DOUBLE) <= TRY_CAST(annual_sales AS DOUBLE)) AND
    (year_opened IS NULL OR 
     (TRY_CAST(year_opened AS INT) >= 1900 AND TRY_CAST(year_opened AS INT) <= YEAR(CURRENT_DATE()))) AND
    (square_feet IS NULL OR TRY_CAST(square_feet AS INT) > 0) AND
    (number_employees IS NULL OR TRY_CAST(number_employees AS INT) >= 0) AND
    (business_type IS NULL OR business_type IN ('BM', 'BS', 'BC', 'BO')) AND
    (guid IS NULL OR REGEXP_LIKE(guid, '^[A-F0-9\\-]+$')) AND
    (modified_date IS NULL OR TRY_CAST(modified_date AS TIMESTAMP) <= CURRENT_TIMESTAMP())
)

{% if is_incremental() %}
  -- Si la tabla ya existe y estamos en modo incremental, solo agregar registros nuevos
  SELECT * FROM cleaned_data
  WHERE 
    store_id != '-1' AND  -- No intentar insertar el registro N/A de nuevo
    NOT EXISTS (
      SELECT 1 FROM {{ this }} existing
      WHERE existing.store_id = cleaned_data.store_id
    )
{% else %}
  -- Primera carga, incluir todos los registros 
  SELECT * FROM cleaned_data
{% endif %}
{{ config(
    materialized='incremental',
    unique_key='product_dw_id',
    incremental_strategy='merge',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY product_key;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
) }}

WITH Tmp AS(
  SELECT
    ProductKey AS product_key,
    ProductName AS product_name,
    ProductNumber AS product_number,
    NewFlag AS new_flag,
    Flag2 AS flag2,
    StandardCost AS standard_cost,
    DealerPrice AS dealer_price,
    Flag3 AS flag3,
    Flag4 AS flag4,
    Flag5 AS flag5,
    StartDate AS start_date,
    GUID AS guid,
    ModifiedDate AS modified_date,
    ROW_NUMBER() OVER (PARTITION BY ProductNumber ORDER BY ModifiedDate DESC) AS num_duplicates
  FROM {{ source('BRONZE', 'product_raw') }}
),

cleaned_data AS(
  -- Identificar duplicados
  SELECT
      {{ dbt_utils.generate_surrogate_key(['product_key', 'product_number', 'product_name', 'num_duplicates']) }} AS product_dw_id,
      product_key,
      product_name,
      product_number,
      new_flag,
      flag2,
      standard_cost,
      dealer_price,
      flag3,
      flag4,
      flag5,
      start_date,
      guid,
      modified_date,
      1 AS is_inconsistent,
      1 AS inconsistency_id, -- Duplicados
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE num_duplicates > 1

  UNION ALL
  
  -- Identificar valores nulos en campos críticos
  SELECT
      {{ dbt_utils.generate_surrogate_key(['product_key', 'product_number', 'product_name', 'num_duplicates']) }} AS product_dw_id,
      product_key,
      product_name,
      product_number,
      new_flag,
      flag2,
      standard_cost,
      dealer_price,
      flag3,
      flag4,
      flag5,
      start_date,
      guid,
      modified_date,
      1 AS is_inconsistent,
      2 AS inconsistency_id, -- Valores nulos
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE num_duplicates = 1 AND 
    (product_number IS NULL OR product_name IS NULL OR product_key IS NULL)

  UNION ALL
  
  -- Identificar valores fuera de rango o inválidos
  SELECT
      {{ dbt_utils.generate_surrogate_key(['product_key', 'product_number', 'product_name', 'num_duplicates']) }} AS product_dw_id,
      product_key,
      product_name,
      product_number,
      new_flag,
      flag2,
      standard_cost,
      dealer_price,
      flag3,
      flag4,
      flag5,
      start_date,
      guid,
      modified_date,
      1 AS is_inconsistent,
      3 AS inconsistency_id, -- Valores fuera de rango
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE num_duplicates = 1 AND (
      product_key <= 0 OR
      dealer_price < 0
    )

  UNION ALL
  
  -- Identificar registros con formato incorrecto
  SELECT
      {{ dbt_utils.generate_surrogate_key(['product_key', 'product_number', 'product_name', 'num_duplicates']) }} AS product_dw_id,
      product_key,
      product_name,
      product_number,
      new_flag,
      flag2,
      standard_cost,
      dealer_price,
      flag3,
      flag4,
      flag5,
      start_date,
      guid,
      modified_date,
      1 AS is_inconsistent,
      4 AS inconsistency_id, -- Formato incorrecto
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE num_duplicates = 1 AND (
      product_number IS NOT NULL AND NOT REGEXP_LIKE(product_number, '^[A-Z]{2}-[0-9A-Z]{4}-[0-9A-Z]{1,2}$') 
    )

  UNION ALL
  
  -- Datos correctos
  SELECT
      {{ dbt_utils.generate_surrogate_key(['product_key', 'product_number', 'product_name', 'num_duplicates']) }} AS product_dw_id,
      product_key,
      product_name,
      product_number,
      new_flag,
      flag2,
      standard_cost,
      dealer_price,
      flag3,
      flag4,
      flag5,
      start_date,
      guid,
      modified_date,
      0 AS is_inconsistent,
      0 AS inconsistency_id, -- Sin inconsistencias
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE 
    num_duplicates = 1 AND
    product_key IS NOT NULL AND
    product_key > 0 AND
    product_name IS NOT NULL AND
    product_number IS NOT NULL AND
    (dealer_price IS NULL OR dealer_price >= 0) AND
    (product_number IS NULL OR REGEXP_LIKE(product_number, '^[A-Z]{2}-[0-9A-Z]{4}-[0-9A-Z]{1,2}$'))
)

{% if is_incremental() %}
  -- Encontrar registros previos que deben marcarse como obsoletos
  , deprecated_records AS (
    SELECT 
      existing.product_dw_id
    FROM {{ this }} existing
    INNER JOIN cleaned_data new_data
      ON existing.product_number = new_data.product_number
      AND existing.product_dw_id != new_data.product_dw_id
  )

  -- Marcar registros antiguos como obsoletos e inconsistentes
  , marked_as_deprecated AS (
    SELECT 
      product_dw_id,
      product_key,
      product_name,
      product_number,
      new_flag,
      flag2,
      standard_cost,
      dealer_price,
      flag3,
      flag4,
      flag5,
      start_date,
      guid,
      modified_date,
      is_inconsistent,
      inconsistency_id,
      num_duplicates,
      0 AS is_current
    FROM {{ this }} 
    WHERE product_dw_id IN (SELECT product_dw_id FROM deprecated_records)
  )
  
  SELECT * FROM marked_as_deprecated
  
  UNION ALL
  
  SELECT * FROM cleaned_data

{% else %}
  SELECT * FROM cleaned_data
{% endif %}
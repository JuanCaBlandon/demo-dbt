{{ config(
    materialized='incremental',
    unique_key='customer_dw_id',
    incremental_strategy='merge',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY customer_key;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
) }}

WITH Tmp AS(
  SELECT
    CustomerKey AS customer_key,
    GeographyKey AS geography_key,
    CustomerType AS customer_type,
    CustomerID AS customer_id,
    GUID AS guid,
    TO_TIMESTAMP(DateCreated, 'M/d/yy H:mm') AS date_created,
    --DateCreated AS date_created,
    ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY TO_TIMESTAMP(DateCreated, 'M/d/yy H:mm')  DESC) AS num_duplicates
  FROM {{ source('BRONZE', 'customer_raw') }}
),

cleaned_data AS(
  -- Identificar duplicados
  SELECT
      {{ dbt_utils.generate_surrogate_key(['customer_key', 'customer_id', 'guid', 'date_created', 'num_duplicates']) }} AS customer_dw_id,
      customer_key,
      geography_key,
      customer_type,
      customer_id,
      guid,
      date_created,
      1 AS is_inconsistent,
      1 AS inconsistency_id, -- Duplicados
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE num_duplicates > 1

  UNION ALL
  
  -- Identificar valores nulos en campos críticos
  SELECT
      {{ dbt_utils.generate_surrogate_key(['customer_key', 'customer_id', 'guid', 'date_created', 'num_duplicates']) }} AS customer_dw_id,
      customer_key,
      geography_key,
      customer_type,
      customer_id,
      guid,
      date_created,
      1 AS is_inconsistent,
      2 AS inconsistency_id, -- Valores nulos
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE num_duplicates = 1 AND 
    (customer_id IS NULL OR customer_key IS NULL)

  UNION ALL
  
  -- Identificar valores fuera de rango
  SELECT
      {{ dbt_utils.generate_surrogate_key(['customer_key', 'customer_id', 'guid', 'date_created', 'num_duplicates']) }} AS customer_dw_id,
      customer_key,
      geography_key,
      customer_type,
      customer_id,
      guid,
      date_created,
      1 AS is_inconsistent,
      3 AS inconsistency_id, -- Valores fuera de rango
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE num_duplicates = 1 AND (
      customer_key < 0 OR
      geography_key < 0 OR
      customer_type NOT IN ('1', '2', '3','4','5','6','7','8', '9','10') OR
      date_created > CURRENT_TIMESTAMP() -- Fecha futura
    )

  UNION ALL
  
  -- Identificar registros con formato incorrecto
  SELECT
      {{ dbt_utils.generate_surrogate_key(['customer_key', 'customer_id', 'guid', 'date_created', 'num_duplicates']) }} AS customer_dw_id,
      customer_key,
      geography_key,
      customer_type,
      customer_id,
      guid,
      date_created,
      1 AS is_inconsistent,
      4 AS inconsistency_id, -- Formato incorrecto
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE num_duplicates = 1 AND (
      customer_id IS NOT NULL AND NOT REGEXP_LIKE(customer_id, '^AW[0-9]{8}$') OR
      guid IS NOT NULL AND NOT REGEXP_LIKE(guid, '^\\{[A-F0-9\\-]+\\}$')
    )

  UNION ALL
  
  -- Datos correctos
  SELECT
      {{ dbt_utils.generate_surrogate_key(['customer_key', 'customer_id', 'guid', 'date_created', 'num_duplicates']) }} AS customer_dw_id,
      customer_key,
      geography_key,
      customer_type,
      customer_id,
      guid,
      date_created,
      0 AS is_inconsistent,
      0 AS inconsistency_id, -- Sin inconsistencias
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE 
    num_duplicates = 1 AND
    customer_id IS NOT NULL AND 
    customer_key IS NOT NULL AND
    customer_key >= 0 AND
    (geography_key IS NULL OR geography_key >= 0) AND
    (customer_type IS NULL OR customer_type IN ('1', '2', '3')) AND
    (date_created IS NULL OR date_created <= CURRENT_TIMESTAMP()) AND
    (customer_id IS NULL OR REGEXP_LIKE(customer_id, '^AW[0-9]{8}$')) AND
    (guid IS NULL OR REGEXP_LIKE(guid, '^\\{[A-F0-9\\-]+\\}$'))
)

{% if is_incremental() %}
  -- Encontrar registros previos que deben marcarse como obsoletos
  , deprecated_records AS (
    SELECT 
      existing.customer_dw_id
    FROM {{ this }} existing
    INNER JOIN cleaned_data new_data
      ON existing.customer_id = new_data.customer_id
      AND existing.customer_dw_id != new_data.customer_dw_id
  )

  -- Marcar registros antiguos como obsoletos e inconsistentes
  , marked_as_deprecated AS (
    SELECT 
      customer_dw_id,
      customer_key,
      geography_key,
      customer_type,
      customer_id,
      guid,
      date_created,
      is_inconsistent,
      inconsistency_id,
      num_duplicates,
      0 AS is_current
    FROM {{ this }} 
    WHERE customer_dw_id IN (SELECT customer_dw_id FROM deprecated_records)
  )
  
  SELECT * FROM marked_as_deprecated
  
  UNION ALL
  
  SELECT * FROM cleaned_data

{% else %}
  SELECT * FROM cleaned_data
{% endif %}
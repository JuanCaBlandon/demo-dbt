{{ config(
    materialized='incremental',
    unique_key='product_cost_dw_id',
    incremental_strategy='merge',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY product_key, start_date;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
) }}

WITH Tmp AS(
  SELECT
    ProductKey AS product_key,
    StartDate AS start_date,
    EndDate AS end_date,
    StandardCost AS standard_cost,
    ModifiedDate AS modified_date,
    ROW_NUMBER() OVER (PARTITION BY ProductKey, StartDate ORDER BY ModifiedDate DESC) AS num_duplicates,
    -- Para dimensiones de tipo 2, verificamos si hay superposición de rangos
    LEAD(StartDate) OVER (PARTITION BY ProductKey ORDER BY StartDate) AS next_start_date
  FROM {{ source('BRONZE', 'product_cost_history_raw') }}
),

cleaned_data AS(
  -- Identificar duplicados (mismo ProductKey y StartDate)
  SELECT
      {{ dbt_utils.generate_surrogate_key(['product_key', 'start_date', 'standard_cost', 'num_duplicates']) }} AS product_cost_dw_id,
      product_key,
      start_date,
      end_date,
      standard_cost,
      modified_date,
      1 AS is_inconsistent,
      1 AS inconsistency_id, -- Duplicados
      num_duplicates,
      1 AS is_current,
      next_start_date
  FROM Tmp
  WHERE num_duplicates > 1

  UNION ALL
  
  -- Identificar valores nulos en campos críticos
  SELECT
      {{ dbt_utils.generate_surrogate_key(['product_key', 'start_date', 'standard_cost', 'num_duplicates']) }} AS product_cost_dw_id,
      product_key,
      start_date,
      end_date,
      standard_cost,
      modified_date,
      1 AS is_inconsistent,
      2 AS inconsistency_id, -- Valores nulos
      num_duplicates,
      1 AS is_current,
      next_start_date
  FROM Tmp
  WHERE num_duplicates = 1 AND 
    (product_key IS NULL OR start_date IS NULL OR standard_cost IS NULL)

  UNION ALL
  
  -- Identificar valores fuera de rango o inválidos
  SELECT
      {{ dbt_utils.generate_surrogate_key(['product_key', 'start_date', 'standard_cost', 'num_duplicates']) }} AS product_cost_dw_id,
      product_key,
      start_date,
      end_date,
      standard_cost,
      modified_date,
      1 AS is_inconsistent,
      3 AS inconsistency_id, -- Valores fuera de rango
      num_duplicates,
      1 AS is_current,
      next_start_date
  FROM Tmp
  WHERE num_duplicates = 1 AND (
      product_key <= 0 OR
      standard_cost < 0 OR
      start_date > CURRENT_TIMESTAMP() OR -- Fecha futura
      (end_date IS NOT NULL AND end_date < start_date) OR -- Rango de fechas inválido
      (modified_date IS NOT NULL AND modified_date > CURRENT_TIMESTAMP()) -- Fecha futura
    )

  UNION ALL
  
  -- Identificar problemas en la continuidad de la dimensión tipo 2
  SELECT
      {{ dbt_utils.generate_surrogate_key(['product_key', 'start_date', 'standard_cost', 'num_duplicates']) }} AS product_cost_dw_id,
      product_key,
      start_date,
      end_date,
      standard_cost,
      modified_date,
      1 AS is_inconsistent,
      4 AS inconsistency_id, -- Problemas de continuidad temporal
      num_duplicates,
      1 AS is_current,
      next_start_date
  FROM Tmp
  WHERE num_duplicates = 1 AND (
      -- Hay un hueco entre el fin de este registro y el inicio del siguiente
      (end_date IS NOT NULL AND next_start_date IS NOT NULL AND DATE_ADD(end_date, 1) < next_start_date) OR
       
      -- Hay superposición entre este registro y el siguiente
      (end_date IS NOT NULL AND next_start_date IS NOT NULL AND DATE_ADD(end_date, 1) > next_start_date)
    )

  UNION ALL
  
  -- Datos correctos
  SELECT
      {{ dbt_utils.generate_surrogate_key(['product_key', 'start_date', 'standard_cost', 'num_duplicates']) }} AS product_cost_dw_id,
      product_key,
      start_date,
      end_date,
      standard_cost,
      modified_date,
      0 AS is_inconsistent,
      0 AS inconsistency_id, -- Sin inconsistencias
      num_duplicates,
      -- Un registro es actual si no tiene fecha de fin o su fecha de fin es posterior a hoy
      CASE 
        WHEN end_date IS NULL OR end_date >= CURRENT_DATE() THEN 1
        ELSE 0
      END AS is_current,
      next_start_date
  FROM Tmp
  WHERE 
    num_duplicates = 1 AND
    product_key IS NOT NULL AND product_key > 0 AND
    start_date IS NOT NULL AND start_date <= CURRENT_TIMESTAMP() AND
    standard_cost IS NOT NULL AND standard_cost >= 0 AND
    (end_date IS NULL OR end_date >= start_date) AND
    (modified_date IS NULL OR modified_date <= CURRENT_TIMESTAMP()) AND
    -- No hay problemas de continuidad
    NOT (
      (end_date IS NOT NULL AND next_start_date IS NOT NULL AND end_date < next_start_date) OR
      (end_date IS NOT NULL AND next_start_date IS NOT NULL AND end_date > next_start_date)
    )
)

{% if is_incremental() %}
  -- Encontrar registros previos que deben marcarse como obsoletos
  , deprecated_records AS (
    SELECT 
      existing.product_cost_dw_id
    FROM {{ this }} existing
    INNER JOIN cleaned_data new_data
      ON existing.product_key = new_data.product_key
      AND existing.start_date = new_data.start_date
      AND existing.product_cost_dw_id != new_data.product_cost_dw_id
  )

  -- Marcar registros antiguos como obsoletos e inconsistentes
  , marked_as_deprecated AS (
    SELECT 
      product_cost_dw_id,
      product_key,
      start_date,
      end_date,
      standard_cost,
      modified_date,
      is_inconsistent,
      inconsistency_id,
      num_duplicates,
      0 AS is_current,
      next_start_date
    FROM {{ this }} 
    WHERE product_cost_dw_id IN (SELECT product_cost_dw_id FROM deprecated_records)
  )
  
  -- Para dimensiones tipo 2, también necesitamos actualizar registros cuyo estado "current" haya cambiado
  , update_current_status AS (
    SELECT
      existing.product_cost_dw_id,
      existing.product_key,
      existing.start_date,
      existing.end_date,
      existing.standard_cost,
      existing.modified_date,
      existing.is_inconsistent,
      existing.inconsistency_id,
      existing.num_duplicates,
      -- Actualizar is_current basado en la fecha actual
      CASE 
        WHEN existing.end_date IS NULL OR existing.end_date >= CURRENT_DATE() THEN 1
        ELSE 0
      END AS is_current,
      existing.next_start_date
    FROM {{ this }} existing
    WHERE 
      existing.product_cost_dw_id NOT IN (SELECT product_cost_dw_id FROM deprecated_records) AND
      (
        (existing.is_current = 1 AND existing.end_date IS NOT NULL AND existing.end_date < CURRENT_DATE()) OR
        (existing.is_current = 0 AND (existing.end_date IS NULL OR existing.end_date >= CURRENT_DATE()))
      )
  )
  
  SELECT * FROM marked_as_deprecated
  
  UNION ALL
  
  SELECT * FROM update_current_status
  
  UNION ALL
  
  SELECT * FROM cleaned_data
  WHERE product_cost_dw_id NOT IN (
    SELECT product_cost_dw_id FROM update_current_status
  )

{% else %}
  SELECT 
    product_cost_dw_id,
    product_key,
    start_date,
    end_date,
    standard_cost,
    modified_date,
    is_inconsistent,
    inconsistency_id,
    num_duplicates,
    is_current,
    next_start_date
  FROM cleaned_data
{% endif %}
-- models/inconsistency_recurrence.sql
{{ config(
    materialized='incremental',
    database='compliance_' ~ var('DEPLOYMENT_ENVIRONMENT'),
    unique_key='id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY id, business_key;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ]
) }}

WITH current_inconsistencies_agg AS (
    -- Agrupar inconsistencias actuales por tipo y clave de negocio
    SELECT
        inconsistency_type,
        inconsistency_id as original_inconsistency_id,
        business_key,
        entity,
        MIN(detection_date) as first_occurrence,
        MAX(detection_date) as last_occurrence,
        COUNT(*) as current_count
    FROM {{ ref('current_inconsistencies') }}
    GROUP BY inconsistency_type, inconsistency_id, business_key, entity
),

{% if adapter.get_relation(this.database, 'historical_inconsistencies') is not none %}
historical_inconsistencies_agg AS (
    -- Agrupar inconsistencias históricas por tipo y clave de negocio
    SELECT
        inconsistency_type,
        original_inconsistency_id,
        business_key,
        entity,
        MIN(detection_date) as first_occurrence,
        MAX(resolution_date) as last_occurrence,
        COUNT(*) as historical_count
    FROM {{ ref('historical_inconsistencies') }}
    GROUP BY inconsistency_type, original_inconsistency_id, business_key, entity
),
{% else %}
-- Si historical_inconsistencies aún no existe, crear un conjunto vacío
historical_inconsistencies_agg AS (
    SELECT
        CAST(NULL AS STRING) as inconsistency_type,
        CAST(NULL AS INT) as original_inconsistency_id,
        CAST(NULL AS STRING) as business_key,
        CAST(NULL AS STRING) as entity,
        CAST(NULL AS TIMESTAMP) as first_occurrence,
        CAST(NULL AS TIMESTAMP) as last_occurrence,
        CAST(0 AS INT) as historical_count
    WHERE 1=0
),
{% endif %}

-- Unir ambas fuentes para obtener un conteo total
all_inconsistencies AS (
    SELECT
        COALESCE(c.inconsistency_type, h.inconsistency_type) as inconsistency_type,
        COALESCE(c.original_inconsistency_id, h.original_inconsistency_id) as inconsistency_id,
        COALESCE(c.business_key, h.business_key) as business_key,
        COALESCE(c.entity, h.entity) as entity,
        CASE 
            WHEN c.first_occurrence IS NOT NULL AND h.first_occurrence IS NOT NULL 
            THEN LEAST(c.first_occurrence, h.first_occurrence)
            WHEN c.first_occurrence IS NOT NULL THEN c.first_occurrence
            ELSE h.first_occurrence
        END as first_occurrence,
        CASE 
            WHEN c.last_occurrence IS NOT NULL AND h.last_occurrence IS NOT NULL 
            THEN GREATEST(c.last_occurrence, h.last_occurrence)
            WHEN c.last_occurrence IS NOT NULL THEN c.last_occurrence
            ELSE h.last_occurrence
        END as last_occurrence,
        COALESCE(c.current_count, 0) + COALESCE(h.historical_count, 0) as total_count
    FROM current_inconsistencies_agg c
    FULL OUTER JOIN historical_inconsistencies_agg h
        ON c.inconsistency_type = h.inconsistency_type
        AND c.original_inconsistency_id = h.original_inconsistency_id
        AND c.business_key = h.business_key
),

-- Detectar patrones basados en la frecuencia y proximidad de las inconsistencias
pattern_detection AS (
    SELECT
        inconsistency_type,
        inconsistency_id,
        business_key,
        entity,
        first_occurrence,
        last_occurrence,
        total_count,
        CASE
            -- Patrones basados en frecuencia y tiempo
            WHEN total_count >= 5 AND DATEDIFF('day', first_occurrence, last_occurrence) <= 30 
                THEN 'HIGH_FREQUENCY_SHORT_PERIOD'
            WHEN total_count >= 10 
                THEN 'PERSISTENT_ISSUE'
            WHEN DATEDIFF('day', first_occurrence, last_occurrence) >= 90 AND total_count >= 3 
                THEN 'RECURRING_LONG_TERM'
            WHEN total_count = 1 
                THEN NULL -- No hay patrón con una sola ocurrencia
            ELSE 'SPORADIC_ISSUE'
        END as pattern
    FROM all_inconsistencies
)

-- Construir la tabla final
SELECT
    {{ dbt_utils.generate_surrogate_key(['pd.inconsistency_type', 'pd.business_key']) }} as id,
    pd.inconsistency_type,
    pd.inconsistency_id,
    pd.business_key,
    pd.total_count as counter,
    pd.first_occurrence,
    pd.last_occurrence,
    pd.entity,
    pd.pattern
FROM pattern_detection pd

{% if is_incremental() %}
    WHERE pd.last_occurrence > (SELECT COALESCE(max(last_occurrence), '1970-01-01') FROM {{ this }})
{% endif %}
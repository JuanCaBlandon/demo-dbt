-- models/inconsistency_recurrence.sql
-- depends_on: {{ ref('current_inconsistencies') }}
-- depends_on: {{ ref('historical_inconsistencies') }}

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
        source,
        inconsistency_id as original_inconsistency_id,
        business_key,
        entity,
        id as current_inconsistency_id, -- Añadido para tener referencia a current_inconsistency_id
        MIN(detection_date) as first_occurrence,
        MAX(detection_date) as last_occurrence,
        COUNT(*) as current_count
    FROM {{ ref('current_inconsistencies') }}
    GROUP BY source, inconsistency_id, business_key, entity, id
),

-- Verificar existencia de historical_inconsistencies de manera segura
{% if execute %}
    {% set historical_table_exists = adapter.get_relation(this.database, 'compliance', 'historical_inconsistencies') is not none %}
{% else %}
    {% set historical_table_exists = false %}
{% endif %}

{% if historical_table_exists %}
historical_inconsistencies_agg AS (
    -- Agrupar inconsistencias históricas por tipo y clave de negocio
    SELECT
        source,
        original_inconsistency_id,
        business_key,
        entity,
        current_inconsistency_id, -- Columna clave para relacionar con otras tablas
        MIN(detection_date) as first_occurrence,
        MAX(resolution_date) as last_occurrence,
        COUNT(*) as historical_count
    FROM {{ ref('historical_inconsistencies') }}
    GROUP BY source, original_inconsistency_id, business_key, entity, current_inconsistency_id
),
{% else %}
-- Si historical_inconsistencies aún no existe, crear un conjunto vacío
historical_inconsistencies_agg AS (
    SELECT
        CAST(NULL AS STRING) as source,
        CAST(NULL AS INT) as original_inconsistency_id,
        CAST(NULL AS STRING) as business_key,
        CAST(NULL AS STRING) as entity,
        CAST(NULL AS STRING) as current_inconsistency_id, -- Columna clave para relacionar
        CAST(NULL AS TIMESTAMP) as first_occurrence,
        CAST(NULL AS TIMESTAMP) as last_occurrence,
        CAST(0 AS INT) as historical_count
    WHERE 1=0
),
{% endif %}

-- Unir ambas fuentes para obtener un conteo total
all_inconsistencies AS (
    SELECT
        COALESCE(c.source, h.source) as source,
        COALESCE(c.original_inconsistency_id, h.original_inconsistency_id) as original_inconsistency_id,
        COALESCE(c.business_key, h.business_key) as business_key,
        COALESCE(c.entity, h.entity) as entity,
        COALESCE(c.current_inconsistency_id, h.current_inconsistency_id) as current_inconsistency_id, -- Columna clave
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
        ON c.source = h.source
        AND c.original_inconsistency_id = h.original_inconsistency_id
        AND c.business_key = h.business_key
),

-- Detectar patrones basados en la frecuencia y proximidad de las inconsistencias
pattern_detection AS (
    SELECT
        source,
        original_inconsistency_id, -- Renombrado para claridad
        business_key,
        entity,
        current_inconsistency_id, -- Añadido para referencias
        first_occurrence,
        last_occurrence,
        total_count,
        CASE
            -- Patrones basados en frecuencia y tiempo
            -- Sintaxis correcta de DATEDIFF para Databricks sin comillas en la unidad
            WHEN total_count >= 5 AND DATEDIFF(DAY, first_occurrence, last_occurrence) <= 30 
                THEN 'HIGH_FREQUENCY_SHORT_PERIOD'
            WHEN total_count >= 10 
                THEN 'PERSISTENT_ISSUE'
            WHEN DATEDIFF(DAY, first_occurrence, last_occurrence) >= 90 AND total_count >= 3 
                THEN 'RECURRING_LONG_TERM'
            WHEN total_count = 1 
                THEN NULL -- No hay patrón con una sola ocurrencia
            ELSE 'SPORADIC_ISSUE'
        END as pattern
    FROM all_inconsistencies
)

-- Construir la tabla final
SELECT
    {{ dbt_utils.generate_surrogate_key(['pd.source', 'pd.business_key']) }} as id,
    pd.source,
    pd.original_inconsistency_id, -- Renombrado para claridad
    pd.business_key,
    pd.current_inconsistency_id, -- Añadido para referencias
    pd.total_count as counter,
    pd.first_occurrence,
    pd.last_occurrence,
    pd.entity,
    pd.pattern
FROM pattern_detection pd

{% if is_incremental() %}
    WHERE pd.last_occurrence > (SELECT COALESCE(max(last_occurrence), '1970-01-01') FROM {{ this }})
{% endif %}
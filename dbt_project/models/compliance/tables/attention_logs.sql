-- models/attention_logs.sql
{{ config(
    materialized='incremental',
    database='compliance_' ~ var('DEPLOYMENT_ENVIRONMENT'),
    unique_key='id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY id, inconsistency_id, user_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ]
) }}

-- Este modelo puede obtener datos de una tabla de seguimiento de actividades existente
-- o puede ser una tabla que se llena mediante la aplicación frontend

{% if source_exists('raw', 'inconsistency_action_logs') %}
    -- Si existe la fuente de datos, usarla
    WITH log_source AS (
        SELECT
            log_id,
            inconsistency_id,
            user_id,
            action_timestamp,
            action_type,
            action_details
        FROM {{ source('raw', 'inconsistency_action_logs') }}
        {% if is_incremental() %}
            WHERE action_timestamp > (SELECT COALESCE(max(action_date), '1970-01-01') FROM {{ this }})
        {% endif %}
    ),

    -- Normalizar los valores de acción para asegurar consistencia
    normalized_actions AS (
        SELECT
            ls.*,
            CASE 
                WHEN UPPER(ls.action_type) IN ('VIEW', 'VIEWED', 'SEE', 'SEEN') THEN 'VIEWED'
                WHEN UPPER(ls.action_type) IN ('ASSIGN', 'ASSIGNED', 'ALLOCATE') THEN 'ASSIGNED'
                WHEN UPPER(ls.action_type) IN ('PROGRESS', 'IN_PROGRESS', 'WORKING', 'WIP') THEN 'IN_PROGRESS'
                WHEN UPPER(ls.action_type) IN ('RESOLVE', 'RESOLVED', 'FIX', 'FIXED', 'COMPLETE', 'COMPLETED') THEN 'RESOLVED'
                ELSE 'VIEWED' -- Valor por defecto
            END as normalized_action
        FROM log_source ls
    )

    SELECT
        {{ dbt_utils.generate_surrogate_key(['na.log_id']) }} as id,
        na.inconsistency_id,
        na.user_id,
        na.action_timestamp as action_date,
        na.normalized_action as action,
        na.action_details as details
    FROM normalized_actions na

{% else %}
    -- Si no existe la fuente, crear la estructura inicial vacía
    -- Este modelo luego será llenado por la aplicación frontend o ETL externo

    WITH sample_data AS (
        -- Muestra de estructura para documentación, no inserta registros
        SELECT 
            CAST(NULL AS STRING) as id,
            CAST(NULL AS STRING) as inconsistency_id,
            CAST(NULL AS STRING) as user_id,
            CAST(NULL AS TIMESTAMP) as action_date,
            CAST(NULL AS STRING) as action,
            CAST(NULL AS STRING) as details
        WHERE 1=0
    )

    SELECT * FROM sample_data

{% endif %}
-- models/historical_inconsistencies.sql
{{ config(
    materialized='incremental',
    database='compliance_' ~ var('DEPLOYMENT_ENVIRONMENT'),
    unique_key='id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY id, inconsistency_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ]
) }}

{% if adapter.get_relation(this.database, 'attention_logs') is not none %}

WITH resolved_inconsistencies AS (
    -- Identifica inconsistencias que han sido resueltas según los logs de atención
    SELECT 
        ci.id as inconsistency_id,
        ci.inconsistency_type,
        ci.inconsistency_id as original_inconsistency_id,
        ci.description,
        ci.detection_date,
        ci.entity,
        ci.business_key,
        ci.source,
        ci.technical_details,
        ci.batch_id,
        al.action_date as resolution_date,
        al.user_id as resolution_user_id,
        'RESOLVED' as final_status, -- Por defecto se marca como resuelto
        al.details as comments,
        'Manual review and correction' as action_taken -- Valor por defecto
    FROM {{ ref('current_inconsistencies') }} ci
    JOIN {{ ref('attention_logs') }} al 
        ON ci.id = al.inconsistency_id
    WHERE al.action = 'RESOLVED'
    AND NOT EXISTS (
        -- Verificar que no exista ya en el histórico
        SELECT 1 FROM {{ this }} hist
        WHERE hist.inconsistency_id = ci.id
    )
),

-- Unir con información de usuarios para obtener el nombre
user_info AS (
    SELECT 
        ri.*,
        u.name as resolution_user_name
    FROM resolved_inconsistencies ri
    LEFT JOIN {{ ref('compliance_users') }} u
        ON ri.resolution_user_id = u.id
)

-- Construir la tabla final
SELECT
    {{ dbt_utils.generate_surrogate_key(['ui.inconsistency_id', 'ui.resolution_date']) }} as id,
    ui.inconsistency_id,
    ui.inconsistency_type,
    ui.original_inconsistency_id,
    ui.description,
    ui.detection_date,
    ui.resolution_date,
    ui.entity,
    ui.final_status,
    COALESCE(ui.resolution_user_name, 'System') as resolution_user,
    ui.comments,
    ui.action_taken,
    ui.business_key,
    ui.source,
    ui.batch_id
FROM user_info ui

{% if is_incremental() %}
    WHERE ui.resolution_date > (SELECT COALESCE(max(resolution_date), '1970-01-01') FROM {{ this }})
{% endif %}

{% else %}
-- Si attention_logs aún no existe, crear una estructura vacía
SELECT
    CAST(NULL AS STRING) as id,
    CAST(NULL AS STRING) as inconsistency_id,
    CAST(NULL AS STRING) as inconsistency_type,
    CAST(NULL AS INT) as original_inconsistency_id,
    CAST(NULL AS STRING) as description,
    CAST(NULL AS TIMESTAMP) as detection_date,
    CAST(NULL AS TIMESTAMP) as resolution_date,
    CAST(NULL AS STRING) as entity,
    CAST(NULL AS STRING) as final_status,
    CAST(NULL AS STRING) as resolution_user,
    CAST(NULL AS STRING) as comments,
    CAST(NULL AS STRING) as action_taken,
    CAST(NULL AS STRING) as business_key,
    CAST(NULL AS STRING) as source,
    CAST(NULL AS STRING) as batch_id
WHERE 1=0  -- No insertará registros, solo define la estructura

{% endif %}
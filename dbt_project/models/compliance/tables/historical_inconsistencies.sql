-- models/historical_inconsistencies.sql
-- depends_on: {{ ref('current_inconsistencies') }}
-- depends_on: {{ ref('attention_logs') }}
-- depends_on: {{ ref('compliance_users') }}

{{ config(
    materialized='incremental',
    database='compliance_' ~ var('DEPLOYMENT_ENVIRONMENT'),
    unique_key='id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY id, current_inconsistency_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ]
) }}

-- Verificar existencia de tablas de manera segura
{% if execute %}
    {% set attention_logs_exists = adapter.get_relation(this.database, 'compliance', 'attention_logs') is not none %}
    {% set compliance_users_exists = adapter.get_relation(this.database, 'compliance', 'compliance_users') is not none %}
    {% set hist_table_exists = adapter.get_relation(this.database, 'compliance', 'historical_inconsistencies') is not none %}
{% else %}
    {% set attention_logs_exists = false %}
    {% set compliance_users_exists = false %}
    {% set hist_table_exists = false %}
{% endif %}

{% if attention_logs_exists %}

WITH resolved_inconsistencies AS (
    -- Identifica inconsistencias que han sido resueltas según los logs de atención
    SELECT 
        ci.id as current_inconsistency_id,
        ci.inconsistency_id as original_inconsistency_id,
        ci.description,
        ci.detection_date,
        ci.entity,
        ci.business_key,
        ci.source, -- Solo una vez
        ci.technical_details,
        ci.batch_id,
        al.action_date as resolution_date,
        al.user_id as resolution_user_id,
        'RESOLVED' as final_status, -- Por defecto se marca como resuelto
        al.details as comments,
        'Manual review and correction' as action_taken -- Valor por defecto
    FROM {{ ref('current_inconsistencies') }} ci
    JOIN {{ ref('attention_logs') }} al 
        ON ci.id = al.current_inconsistency_id
    WHERE al.action = 'RESOLVED'
    {% if hist_table_exists and not flags.FULL_REFRESH %}
    -- Solo verificar existencia si la tabla ya existe y no es full refresh
    AND NOT EXISTS (
        SELECT 1 FROM {{ this }} hist
        WHERE hist.current_inconsistency_id = ci.id
    )
    {% endif %}
),

{% if compliance_users_exists %}
-- Unir con información de usuarios para obtener el nombre
user_info AS (
    SELECT 
        ri.*,
        u.name as resolution_user_name
    FROM resolved_inconsistencies ri
    LEFT JOIN {{ ref('compliance_users') }} u
        ON ri.resolution_user_id = u.id
)
{% else %}
-- Si compliance_users no existe aún
user_info AS (
    SELECT 
        ri.*,
        'System' as resolution_user_name
    FROM resolved_inconsistencies ri
)
{% endif %}

-- Construir la tabla final
SELECT
    {{ dbt_utils.generate_surrogate_key(['ui.current_inconsistency_id', 'ui.resolution_date']) }} as id,
    ui.current_inconsistency_id,
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
    ui.source, -- Solo una vez
    ui.batch_id
FROM user_info ui

{% if is_incremental() and hist_table_exists and not flags.FULL_REFRESH %}
    WHERE ui.resolution_date > (SELECT COALESCE(max(resolution_date), '1970-01-01') FROM {{ this }})
{% endif %}

{% else %}
-- Si attention_logs aún no existe, crear una estructura vacía
SELECT
    CAST(NULL AS STRING) as id,
    CAST(NULL AS STRING) as current_inconsistency_id,
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
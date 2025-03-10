

-- models/inconsistency_lifecycle.sql
-- depends_on: {{ ref('current_inconsistencies') }}
-- depends_on: {{ ref('historical_inconsistencies') }}
-- depends_on: {{ ref('inconsistency_recurrence') }}
-- depends_on: {{ ref('attention_logs') }}
-- depends_on: {{ ref('compliance_users') }}

{{ config(
    materialized='view',
    database='compliance_' ~ var('DEPLOYMENT_ENVIRONMENT')
) }}

WITH current_data AS (
    SELECT
        ci.id as current_inconsistency_id,  
        ci.inconsistency_id as original_inconsistency_id,
        ci.description,
        ci.detection_date,
        ci.entity,
        ci.status,
        ci.business_key,
        ci.source,
        ci.batch_id,
        1 as occurrence_count, 
        NULL as pattern,
        NULL as resolution_date,
        NULL as resolution_user,
        NULL as action_taken,
        NULL as final_status,
        NULL as comments,
        'current' as data_source
    FROM {{ ref('current_inconsistencies') }} ci
),

historical_data AS (

    SELECT
        hi.current_inconsistency_id, 
        hi.original_inconsistency_id,
        hi.description,
        hi.detection_date,
        hi.entity,
        'RESOLVED' as status,
        hi.business_key,
        hi.source,
        hi.batch_id,
        1 as occurrence_count, 
        NULL as pattern,
        hi.resolution_date,
        hi.resolution_user,
        hi.action_taken,
        hi.final_status,
        hi.comments,
        'historical' as data_source
    FROM {{ ref('historical_inconsistencies') }} hi
),

combined_data AS (
    SELECT * FROM current_data
    UNION ALL
    SELECT * FROM historical_data
)

SELECT
    current_inconsistency_id,
    original_inconsistency_id,
    description,
    detection_date,
    entity,
    status,
    business_key,
    source,
    batch_id,
    occurrence_count,
    pattern,
    resolution_date,
    resolution_user,
    action_taken,
    final_status,
    comments,
    data_source,
    CASE
        WHEN data_source = 'current' AND status = 'PENDING' AND detection_date < DATEADD(DAY, -2, CURRENT_DATE()) 
            THEN true
        ELSE false
    END as is_delayed,
    DATEDIFF(DAY, detection_date, COALESCE(resolution_date, CURRENT_DATE())) as days_open
FROM combined_data
WHERE current_inconsistency_id IS NOT NULL
ORDER BY 
    CASE WHEN data_source = 'current' THEN 0 ELSE 1 END,
    detection_date DESC

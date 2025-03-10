-- models/attention_logs.sql
{{ config(
    materialized='incremental',
    database='compliance_' ~ var('DEPLOYMENT_ENVIRONMENT'),
    unique_key='id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY id, current_inconsistency_id, user_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ]
) }}


    WITH sample_data AS (
        -- Muestra de estructura para documentación, no inserta registros
        SELECT 
            CAST(NULL AS STRING) as id,
            CAST(NULL AS STRING) as current_inconsistency_id,
            CAST(NULL AS STRING) as user_id,
            CAST(NULL AS TIMESTAMP) as action_date,
            CAST(NULL AS STRING) as action,
            CAST(NULL AS STRING) as details
        WHERE 1=0
    )

    SELECT * FROM sample_data


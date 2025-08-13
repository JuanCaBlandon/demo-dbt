{{ config(
    materialized='view',
    tags=["gold_inconsistencies", "dashboard"]
) }}

WITH dashboard_data AS (
    SELECT
        fi.inconsistency_dw_id,
        fi.source_table_id,
        fi.source_table_name,
        fi.inconsistency_id,
        fi.inconsistency_description,
        fi.data_quality_dimension,
        fi.rationale,
        fi.created_at,
        fi.date_id,
        
        -- Dimensión de fecha
        dd.date_full,
        dd.day_of_week,
        dd.day_name, 
        -- Conteo para agregaciones
        1 as inconsistency_count
    FROM {{ ref('fact_data_inconsistencies') }} fi
    INNER JOIN {{ ref('dim_date') }} dd
        ON fi.date_id = dd.date_id
    INNER JOIN {{ ref('inconsistency_master') }} im
        ON fi.inconsistency_id = im.inconsistency_id
)

SELECT
    -- Dimensiones principales
    inconsistency_dw_id,
    source_table_id,
    source_table_name,
    inconsistency_id,
    inconsistency_description,
    data_quality_dimension,
    rationale,
    
    -- Información temporal
    created_at,
    date_id,
    date_full,
    day_of_week,
    day_name,
    
    -- Métricas
    inconsistency_count,
    
    -- Campos calculados útiles para el dashboard
    CASE 
        WHEN data_quality_dimension = 'Accuracy' THEN inconsistency_count 
        ELSE 0 
    END AS accuracy_issues,
    
    CASE 
        WHEN data_quality_dimension = 'Completeness' THEN inconsistency_count 
        ELSE 0 
    END AS completeness_issues,
    
    CASE 
        WHEN data_quality_dimension = 'Consistency' THEN inconsistency_count 
        ELSE 0 
    END AS consistency_issues,
    
    CASE 
        WHEN data_quality_dimension = 'Validity' THEN inconsistency_count 
        ELSE 0 
    END AS validity_issues,
    
    CASE 
        WHEN data_quality_dimension = 'Timeliness' THEN inconsistency_count 
        ELSE 0 
    END AS timeliness_issues
FROM dashboard_data
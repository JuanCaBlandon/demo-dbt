{{ config(
    materialized='table',
    schema='silver',
    tags=["metadata"]
) }}

WITH inconsistency_master AS (
    SELECT 
        1 AS inconsistency_id,
        'Duplicados' AS description,
        'Uniqueness' AS data_quality_dimension,
        'Registros duplicados indican problemas de unicidad en los datos, lo cual afecta la capacidad de identificar registros individuales de manera confiable.' AS rationale
    
    UNION ALL
    
    SELECT 
        2 AS inconsistency_id,
        'Valores nulos en campos críticos' AS description,
        'Completeness' AS data_quality_dimension,
        'La ausencia de valores en campos críticos indica información incompleta, afectando la utilidad de los datos para análisis y operaciones.' AS rationale
    
    UNION ALL
    
    SELECT 
        3 AS inconsistency_id,
        'Valores fuera de rango' AS description,
        'Accuracy' AS data_quality_dimension,
        'Valores numéricos o fechas que están fuera de rangos esperados indican datos imprecisos o erróneos.' AS rationale
    
    UNION ALL
    
    SELECT 
        4 AS inconsistency_id,
        'Formato incorrecto' AS description,
        'Validity' AS data_quality_dimension,
        'Datos que no cumplen con el formato predefinido violan las restricciones establecidas para ese tipo de dato.' AS rationale
    
    UNION ALL
    
    SELECT 
        5 AS inconsistency_id,
        'Problemas de integridad referencial' AS description,
        'Integrity' AS data_quality_dimension,
        'Referencias a entidades que no existen en las tablas relacionadas comprometen la estructura relacional de los datos.' AS rationale
    
    UNION ALL
    
    SELECT 
        6 AS inconsistency_id,
        'Valores que exceden longitud máxima' AS description,
        'Validity' AS data_quality_dimension,
        'Datos que exceden la longitud permitida no cumplen con las restricciones predefinidas para estos campos.' AS rationale
    
    UNION ALL
    
    SELECT 
        7 AS inconsistency_id,
        'Problemas de continuidad temporal' AS description,
        'Consistency' AS data_quality_dimension,
        'Interrupciones o superposiciones en rangos de fechas crean inconsistencias en la secuencia temporal de los datos.' AS rationale
    
    UNION ALL
    
    SELECT 
        8 AS inconsistency_id,
        'Valores obsoletos' AS description,
        'Timeliness' AS data_quality_dimension,
        'Datos que ya no son actuales y han sido reemplazados por versiones más recientes.' AS rationale
    
    UNION ALL
    
    SELECT 
        9 AS inconsistency_id,
        'Valores inconsistentes entre fuentes' AS description,
        'Consistency' AS data_quality_dimension,
        'Cuando los mismos datos aparecen de manera diferente en distintas fuentes, generando conflictos en la interpretación.' AS rationale
)

SELECT * FROM inconsistency_master
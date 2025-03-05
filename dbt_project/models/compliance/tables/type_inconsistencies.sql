 {{ config(
		materialized = 'table',
        database='compliance_' ~ var('DEPLOYMENT_ENVIRONMENT'),
		post_hook=["ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"]
) }}

WITH inconsistencies AS (
    SELECT 
        inconsistency_id,
        inconsistency_description,
        status,
        created_at,
        updated_at
    FROM
    VALUES
    
        (0,'N/A',1,'2025-02-27','2025-02-27'),
        (1,'Duplicates',1,'2025-02-27','2025-02-27'),
        (2,'NULL values',1,'2025-02-27','2025-02-27'),
        (3,'Repeat offender without dates',1,'2025-02-27','2025-02-27'),
        (4,'Not repeat offender with dates',1,'2025-02-27','2025-02-27'),
        (5,'Without reference entity',1,'2025-02-27','2025-02-27'),
        (6,'Max character limit',1,'2025-02-27','2025-02-27'),
        (7,'Not present in batch file',1,'2025-02-27','2025-02-27'),
        (8,'Batch file insufficient info',1,'2025-02-27','2025-02-27'),
        (9,'Not present in actives customer table',1,'2025-02-27','2025-02-27'),
        (10,'Reference entity inconsistent',1,'2025-02-27','2025-02-27')
        AS DATA (inconsistency_id,inconsistency_description,status,created_at,updated_at)
)
SELECT 
    inconsistency_id,
    inconsistency_description,
    status,
    created_at,
    updated_at

FROM inconsistencies;
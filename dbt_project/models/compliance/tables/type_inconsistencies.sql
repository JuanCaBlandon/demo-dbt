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
        inconsistency_category,
        created_at,
        updated_at
    FROM
    VALUES
    
        (0,'N/A',1,'Process Inconsistency','2025-02-27','2025-02-27'),
        (1,'Duplicates',1,'Process Inconsistency','2025-02-27','2025-02-27'),
        (2,'NULL values',1,'Process Inconsistency','2025-02-27','2025-02-27'),
        (3,'Repeat offender without dates',1,'Process Inconsistency','2025-02-27','2025-02-27'),
        (4,'Not repeat offender with dates',1,'Process Inconsistency','2025-02-27','2025-02-27'),
        (5,'Without reference entity',1,'Process Inconsistency','2025-02-27','2025-02-27'),
        (6,'Max character limit',1,'Process Inconsistency','2025-02-27','2025-02-27'),
        (7,'Not present in batch file',1,'Process Inconsistency','2025-02-27','2025-02-27'),
        (8,'Batch file insufficient info',1,'Process Inconsistency','2025-02-27','2025-02-27'),
        (9,'Not present in actives customer table',1,'Process Inconsistency','2025-02-27','2025-02-27'),
        (10,'Reference entity inconsistent',1,'Process Inconsistency','2025-02-27','2025-02-27'),
        (11,'Uninstall violation reported, but Record Type 7 previously submitted',1,'WS Rejection','2025-03-09','2025-03-09'),
        (12,'Authorized Uninstall reported, but no Record Type 7 previously submitted',1,'WS Rejection','2025-03-09','2025-03-09'),
        (13,'Final Compliance reached, but has violations in the previous 60 days',1,'WS Rejection','2025-03-09','2025-03-09')
        AS DATA (inconsistency_id,inconsistency_description,status,inconsistency_category,created_at,updated_at)
)
SELECT 
    inconsistency_id,
    inconsistency_description,
    status,
    inconsistency_category,
    created_at,
    updated_at

FROM inconsistencies;
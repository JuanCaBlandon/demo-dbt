 {{ config(
		materialized = 'table',
		post_hook=["ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"]
) }}

WITH violations AS (
    SELECT id, description
    FROM
    VALUES
        (1, 'Violation - 5 fails in a 24-hour period'),
        (2, 'Violation - 10 fails in a 30-day period'),
        (3, 'Tampering Violation'),
        (4, 'Uninstall Violation'),
        (5, 'Authorized Uninstall'),
        (6, 'Switched Vehicle'),
        (7, 'Final Compliance') AS DATA (id, description)
)
SELECT 
    id,
    description,
    current_timestamp() as created_at
FROM violations;
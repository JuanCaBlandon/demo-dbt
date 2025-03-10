-- models/compliance_users.sql
{{ config(
    materialized='table',
    database='compliance_' ~ var('DEPLOYMENT_ENVIRONMENT'),
    unique_key='id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY id, email;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ]
) }}

WITH manual_users AS (
        SELECT 
            1 as user_id, 'Analist Compliance' as user_name, 'ANALYST' as user_role, 
            'analist@intoxalock.com' as user_email, CURRENT_TIMESTAMP() as last_login_time
        UNION ALL SELECT 
            2, 'Supervisor Compliance', 'SUPERVISOR', 'supervisor@intoxalock.com', CURRENT_TIMESTAMP()
        UNION ALL SELECT 
            3, 'Administrator System', 'ADMIN', 'admin@intoxalock.com', CURRENT_TIMESTAMP()
    )
SELECT
    {{ dbt_utils.generate_surrogate_key(['mu.user_id']) }} as id,
    mu.user_name as name,
    mu.user_role,
    mu.user_email as email,
    mu.last_login_time as last_access
FROM manual_users mu

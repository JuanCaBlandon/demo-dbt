-- models/attention_logs_status_master.sql
{{ config(
    materialized='table',
    database='compliance_' ~ var('DEPLOYMENT_ENVIRONMENT')
) }}

-- Master table for attention log statuses
SELECT
    'VIEWED' as status,
    'User has viewed the inconsistency' as description
UNION ALL SELECT
    'ASSIGNED' as status,
    'Inconsistency assigned to a user for review' as description
UNION ALL SELECT
    'IN_PROGRESS' as status,
    'User is working on resolving the inconsistency' as description
UNION ALL SELECT
    'RESOLVED' as status,
    'Inconsistency has been resolved' as description
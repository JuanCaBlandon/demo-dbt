-- models/inconsistency_status_master.sql
{{ config(
    materialized='table',
    database='compliance_' ~ var('DEPLOYMENT_ENVIRONMENT')
) }}

-- Master table for current inconsistency statuses
SELECT
    'PENDING' as status,
    'Inconsistency detected but not addressed' as description
UNION ALL SELECT
    'IN_PROGRESS' as status,
    'Inconsistency in the process of being resolved' as description
UNION ALL SELECT
    'RESOLVED' as status,
    'Inconsistency resolved' as description
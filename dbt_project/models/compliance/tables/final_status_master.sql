-- models/final_status_master.sql
{{ config(
    materialized='table',
    database='compliance_' ~ var('DEPLOYMENT_ENVIRONMENT')
) }}

-- Master table for final statuses of historical inconsistencies
SELECT
    'RESOLVED' as status,
    'Inconsistency properly resolved' as description
UNION ALL SELECT
    'FALSE_POSITIVE' as status,
    'The detected inconsistency was not actually a problem' as description
UNION ALL SELECT
    'NOT_APPLICABLE' as status,
    'The inconsistency does not apply to the current context' as description

-- models/source_master.sql
{{ config(
    materialized='table',
    database='compliance_' ~ var('DEPLOYMENT_ENVIRONMENT')
) }}
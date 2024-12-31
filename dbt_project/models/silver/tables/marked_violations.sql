{{ config(
    materialized='incremental',
    unique_key='violation_dw_id, record_type',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY customer_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ]
) }}

WITH base_data AS (
    SELECT
        violation_dw_id,
        customer_id,
        device_usage_violation_id,
        violation_type,
        violation_id,
        normalized_date violation_reporting_approval_date
    FROM {{ ref('customer_violations_cleaned') }}
    WHERE is_inconsistent = 0
    {% if is_incremental() %}
        AND normalized_date > (
            SELECT MAX(violation_reporting_approval_date) FROM {{ this }}
        )
    {% endif %}
),

-- Reglas dinámicas (24 horas, 30 días)
marked_violations_24hr AS (
    SELECT
        a.customer_id,
        a.device_usage_violation_id,
        a.violation_reporting_approval_date,
        COUNT(*) OVER (
            PARTITION BY a.customer_id
            ORDER BY a.violation_reporting_approval_date
            RANGE BETWEEN INTERVAL '24 HOURS' PRECEDING AND CURRENT ROW
        ) AS violation_count_24hr
    FROM base_data a
    WHERE violation_id in (1,11)
),

marked_violations_30d AS (
    SELECT
        a.customer_id,
        a.device_usage_violation_id,
        a.violation_reporting_approval_date,
        COUNT(*) OVER (
            PARTITION BY a.customer_id
            ORDER BY a.violation_reporting_approval_date
            RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
        ) AS violation_count_30d
    FROM base_data a
    WHERE violation_id in (1,11)
)

SELECT
    customer_id,
    device_usage_violation_id,
    violation_reporting_approval_date,
    CASE
        WHEN violation_count_24hr >= 5 AND violation_count_24hr % 5 = 0 THEN 1
        ELSE 0
    END AS record_type,
    '24 hrs' record_description
FROM marked_violations_24hr m24
WHERE violation_count_24hr >= 5 AND violation_count_24hr % 5 = 0

UNION ALL
SELECT
    customer_id,
    device_usage_violation_id,
    violation_reporting_approval_date,
    CASE
        WHEN violation_count_30d >= 10 AND violation_count_30d % 10 = 0 THEN 2
        ELSE 0
    END AS record_type,
    '30 days' record_description
FROM marked_violations_30d m30
WHERE violation_count_30d >= 10 AND violation_count_30d % 10 = 0


UNION ALL
SELECT
    b.customer_id,
    b.device_usage_violation_id,
    b.violation_reporting_approval_date,
    '3' record_type,
    'Tampering' record_description
FROM base_data b
WHERE violation_type = 'TYPE 3'
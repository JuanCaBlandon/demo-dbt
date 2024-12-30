{{ config(
    materialized='incremental',
    unique_key='violation_dw_id, violation_type',
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
        violation_reported,
        violation_reporting_approval_date,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY violation_reporting_approval_date
        ) AS violation_rank
    FROM {{ ref('violation_transaction_cleaned') }}
    {% if is_incremental() %}
    WHERE violation_reporting_approval_date > (
        SELECT MAX(violation_reporting_approval_date) FROM {{ this }}
    )
    AND is_inconsistent = 0 and violation_id in (1,11)
    {% endif %}
),

{% if is_incremental() %}
previous_data AS (
    -- Traer datos previamente marcados en el modelo
    SELECT
        violation_dw_id,
        customer_id,
        device_usage_violation_id,
        violation_reported,
        violation_reporting_approval_date,
        violation_type
    FROM {{ this }}
),
{% endif %}

merged_data AS (
    -- Unir los datos previos con los nuevos para asegurar continuidad
    {% if is_incremental() %}
    SELECT * FROM previous_data
    UNION ALL
    {% endif %}
    SELECT * FROM base_data
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
        ) AS violation_count_24hr,
        CASE
            WHEN COUNT(*) OVER (
                PARTITION BY a.customer_id
                ORDER BY a.violation_reporting_approval_date
                RANGE BETWEEN INTERVAL '24 HOURS' PRECEDING AND CURRENT ROW
            ) >= 5
            AND COUNT(*) OVER (
                PARTITION BY a.customer_id
                ORDER BY a.violation_reporting_approval_date
                RANGE BETWEEN INTERVAL '24 HOURS' PRECEDING AND CURRENT ROW
            ) % 5 = 0
            THEN 1
            ELSE 0
        END AS is_marked_24hr
    FROM merged_data a
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
        ) AS violation_count_30d,
        CASE
            WHEN COUNT(*) OVER (
                PARTITION BY a.customer_id
                ORDER BY a.violation_reporting_approval_date
                RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
            ) = 10
            {% if is_incremental() %}
            AND NOT EXISTS (
                SELECT 1
                FROM merged_data b
                WHERE b.customer_id = a.customer_id
                AND b.violation_reporting_approval_date BETWEEN a.violation_reporting_approval_date - INTERVAL 30 DAYS AND a.violation_reporting_approval_date
                AND b.violation_type = 'Type 2 (30 days)'
            )
            {% endif %}
            THEN 1
            ELSE 0
        END AS is_marked_30d
    FROM merged_data a
)

SELECT
    v.customer_id,
    v.device_usage_violation_id,
    v.violation_reporting_approval_date,
    CASE
        WHEN m24.is_marked_24hr = 1 THEN 'Type 1 (24 hrs)'
        ELSE 'Not marked'
    END AS violation_type
FROM base_data v
LEFT JOIN marked_violations_24hr m24
    ON v.customer_id = m24.customer_id
    AND v.device_usage_violation_id = m24.device_usage_violation_id
WHERE m24.is_marked_24hr = 1

UNION all
SELECT
    v.customer_id,
    v.device_usage_violation_id,
    v.violation_reporting_approval_date,
    CASE
        WHEN m30.is_marked_30d = 1 THEN 'Type 2 (30 days)'
        ELSE 'Not marked'
    END AS violation_type
FROM base_data v
LEFT JOIN marked_violations_30d m30
    ON v.customer_id = m30.customer_id
    AND v.device_usage_violation_id = m30.device_usage_violation_id
WHERE m30.is_marked_30d = 1;

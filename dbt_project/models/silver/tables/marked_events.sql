{{ config(
    materialized='incremental',
    unique_key='event_dw_id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY customer_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ]
) }}

WITH base_data AS (
    SELECT
        event_dw_id,
        customer_id,
        device_usage_violation_id,
        device_usage_event_violation_id,
        customer_transaction_id,
        event_type,
        event_date
    FROM {{ ref('customer_events_cleaned') }} AS cec
    WHERE is_inconsistent = 0
    {% if is_incremental() %}
        AND event_date > (
            SELECT COALESCE(MAX(event_date), '2024-01-01') FROM {{ this }}
            WHERE 
                customer_id = cec.customer_id
                AND CONCAT('TYPE ', record_type) = cec.event_type
        )
        
    {% endif %}
),

-- Dynamic rules (24 hours, 30 days)
marked_violations_24hr AS (
    SELECT
        a.event_dw_id,
        a.customer_id,
        a.device_usage_violation_id,
        a.event_date,
        COUNT(*) OVER (
            PARTITION BY a.customer_id
            ORDER BY a.event_date
            RANGE BETWEEN INTERVAL '24 HOURS' PRECEDING AND CURRENT ROW
        ) AS violation_count_24hr
    FROM base_data a
    WHERE event_type ='TYPE 1-2'
),

marked_violations_30d AS (
    SELECT
        a.event_dw_id,
        a.customer_id,
        a.device_usage_violation_id,
        a.event_date,
        COUNT(*) OVER (
            PARTITION BY a.customer_id
            ORDER BY a.event_date
            RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
        ) AS violation_count_30d
    FROM base_data a
    WHERE event_type ='TYPE 1-2'
)

SELECT
    event_dw_id,
    customer_id,
    'device_usage_violation_id' event_id_type,
    device_usage_violation_id event_id,
    event_date,
    CASE
        WHEN violation_count_24hr >= 5 AND violation_count_24hr % 5 = 0 THEN 1
        ELSE 0
    END AS record_type,
    '24 hrs' record_description
FROM marked_violations_24hr m24
WHERE violation_count_24hr >= 5 AND violation_count_24hr % 5 = 0

UNION ALL
SELECT
    event_dw_id,
    customer_id,
    'device_usage_violation_id' event_id_type,
    device_usage_violation_id event_id,
    event_date,
    CASE
        WHEN violation_count_30d >= 10 AND violation_count_30d % 10 = 0 THEN 2
        ELSE 0
    END AS record_type,
    '30 days' record_description
FROM marked_violations_30d m30
WHERE violation_count_30d >= 10 AND violation_count_30d % 10 = 0


UNION ALL
SELECT
    event_dw_id,
    customer_id,
    CASE WHEN device_usage_violation_id IS NULL THEN 'device_usage_event_violation_id' ELSE 'device_usage_violation_id' END event_id_type,
    COALESCE(device_usage_violation_id, device_usage_event_violation_id) event_id,
    event_date,
    '3' record_type,
    'Tampering' record_description
FROM base_data b
WHERE event_type = 'TYPE 3'

UNION ALL
SELECT
    event_dw_id,
    customer_id,
    'device_usage_event_violation_id' event_id_type,
    device_usage_event_violation_id event_id,
    event_date,
    '4' record_type,
    'autorized_uninstall' record_description
FROM base_data b
WHERE event_type = 'TYPE 4'
--TODO: Update client status when submited

UNION ALL
SELECT
    event_dw_id,
    customer_id,
    'customer_transaction_id' event_id_type,
    customer_transaction_id event_id,
    event_date,
    '5' record_type,
    'unautorized_uninstall' record_description
FROM base_data b
WHERE event_type = 'TYPE 5'

UNION ALL
SELECT
    event_dw_id,
    customer_id,
    'customer_transaction_id' event_id_type,
    customer_transaction_id event_id,
    event_date,
    '6' record_type,
    'switched_vehicle' record_description
FROM base_data b
WHERE event_type = 'TYPE 6'


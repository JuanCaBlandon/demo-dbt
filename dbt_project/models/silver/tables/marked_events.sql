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
        cec.event_dw_id,
        cec.customer_id,
        cc.drivers_license_number,
        cec.device_usage_violation_id,
        cec.device_usage_event_violation_id,
        cec.customer_transaction_id,
        cec.event_type,
        cec.event_date,
        cec.new_vin
    FROM {{ ref('customer_events_cleaned') }}  AS cec
    INNER JOIN {{ ref('customer_cleaned') }}  cc ON cc.customer_id = cec.customer_id
    WHERE cec.is_inconsistent = 0
     {% if is_incremental() %}
        AND event_date > (
            SELECT COALESCE(MAX(event_date), '2024-01-01') FROM {{ this }}
            WHERE 
                customer_id = cec.customer_id
                AND CONCAT('TYPE ', record_type) = cec.event_type
        )
        
    {% endif %}
),

-- 24 hours violations with reset counter
marked_violations_24hr AS (
    SELECT
        a.event_dw_id,
        a.drivers_license_number,
        a.customer_id,
        a.device_usage_violation_id,
        a.event_date,
        COUNT(*) OVER (
            PARTITION BY a.drivers_license_number
            ORDER BY a.event_date
            RANGE BETWEEN INTERVAL '24 HOURS' PRECEDING AND CURRENT ROW
        ) AS violation_count_24hr,
        COUNT(*) OVER (
            PARTITION BY a.drivers_license_number
            ORDER BY a.event_date
        ) - 
        COALESCE(
            MAX(CASE 
                WHEN COUNT(*) OVER (
                    PARTITION BY a.drivers_license_number
                    ORDER BY a.event_date
                    RANGE BETWEEN INTERVAL '24 HOURS' PRECEDING AND CURRENT ROW
                ) >= 5 
                THEN COUNT(*) OVER (PARTITION BY a.drivers_license_number ORDER BY a.event_date)
            END) OVER (
                PARTITION BY a.drivers_license_number
                ORDER BY a.event_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ),
            0
        ) AS events_since_last_mark_24hr
    FROM base_data a
    WHERE event_type = 'TYPE 1-2'
),

-- 30 days violations with reset counter
marked_violations_30d AS (
    SELECT
        a.event_dw_id,
        a.drivers_license_number,
        a.customer_id,
        a.device_usage_violation_id,
        a.event_date,
        COUNT(*) OVER (
            PARTITION BY a.drivers_license_number
            ORDER BY a.event_date
            RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
        ) AS violation_count_30d,
        COUNT(*) OVER (
            PARTITION BY a.drivers_license_number
            ORDER BY a.event_date
        ) - 
        COALESCE(
            MAX(CASE 
                WHEN COUNT(*) OVER (
                    PARTITION BY a.drivers_license_number
                    ORDER BY a.event_date
                    RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
                ) >= 10 
                THEN COUNT(*) OVER (PARTITION BY a.drivers_license_number ORDER BY a.event_date)
            END) OVER (
                PARTITION BY a.drivers_license_number
                ORDER BY a.event_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ),
            0
        ) AS events_since_last_mark_30d
    FROM base_data a
    WHERE event_type = 'TYPE 1-2'
)

-- 24 hour violations
SELECT
    event_dw_id,
    drivers_license_number,
    customer_id,
    NULL AS new_vin,
    'device_usage_violation_id' event_id_type,
    device_usage_violation_id event_id,
    event_date,
    1 AS record_type,
    '24 hrs' record_description
FROM marked_violations_24hr m24
WHERE violation_count_24hr >= 5 
    AND events_since_last_mark_24hr >= 5

UNION ALL

-- 30 day violations
SELECT
    event_dw_id,
    drivers_license_number,
    customer_id,
    NULL AS new_vin,
    'device_usage_violation_id' event_id_type,
    device_usage_violation_id event_id,
    event_date,
    2 AS record_type,
    '30 days' record_description
FROM marked_violations_30d m30
WHERE violation_count_30d >= 10 
    AND events_since_last_mark_30d >= 10

UNION ALL

-- Tampering events
SELECT
    event_dw_id,
    drivers_license_number,
    customer_id,
    NULL AS new_vin,
    CASE WHEN device_usage_violation_id IS NULL THEN 'device_usage_event_violation_id' ELSE 'device_usage_violation_id' END event_id_type,
    COALESCE(device_usage_violation_id, device_usage_event_violation_id) event_id,
    event_date,
    3 AS record_type,
    'Tampering' record_description
FROM base_data b
WHERE event_type = 'TYPE 3'

UNION ALL

-- Authorized uninstall
SELECT
    event_dw_id,
    drivers_license_number,
    customer_id,
    NULL AS new_vin,
    'device_usage_event_violation_id' event_id_type,
    device_usage_event_violation_id event_id,
    event_date,
    4 AS record_type,
    'autorized_uninstall' record_description
FROM base_data b
WHERE event_type = 'TYPE 4'

UNION ALL

-- Unauthorized uninstall
SELECT
    event_dw_id,
    drivers_license_number,
    customer_id,
    NULL AS new_vin,
    'customer_transaction_id' event_id_type,
    customer_transaction_id event_id,
    event_date,
    5 AS record_type,
    'unautorized_uninstall' record_description
FROM base_data b
WHERE event_type = 'TYPE 5'

UNION ALL

-- Switched vehicle
SELECT
    event_dw_id,
    drivers_license_number,
    customer_id,
    new_vin,
    'customer_transaction_id' event_id_type,
    customer_transaction_id event_id,
    event_date,
    6 AS record_type,
    'switched_vehicle' record_description
FROM base_data b
WHERE event_type = 'TYPE 6'
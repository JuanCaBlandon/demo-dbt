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
--Logic to group violations by drivers license and identify record types 1-2 groups
violation_counts AS (
    SELECT
        event_dw_id,
        customer_id,
        drivers_license_number,
        device_usage_violation_id,
        event_date,
        COUNT(*) OVER (
            PARTITION BY drivers_license_number
            ORDER BY event_date
            RANGE BETWEEN INTERVAL 24 HOURS PRECEDING AND CURRENT ROW
        ) AS violation_count_24h,
        COUNT(*) OVER (
            PARTITION BY drivers_license_number
            ORDER BY event_date
            RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
        ) AS violation_count_30d
    FROM base_data
),

marking_data AS (
    SELECT
        *,
        -- Calculate cumulative events since last mark
        COUNT(*) OVER (PARTITION BY drivers_license_number ORDER BY event_date)
        - COALESCE(
            MAX(CASE WHEN violation_count_24h >= 5 AND violation_count_24h % 5 = 0 
                     THEN COUNT(*) OVER (PARTITION BY drivers_license_number ORDER BY event_date) END)
            OVER (PARTITION BY drivers_license_number ORDER BY event_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
            0
        ) AS events_since_last_24_mark,
        COUNT(*) OVER (PARTITION BY drivers_license_number ORDER BY event_date)
        - COALESCE(
            MAX(CASE WHEN violation_count_30d >= 10 AND violation_count_30d % 10 = 0 
                     THEN COUNT(*) OVER (PARTITION BY drivers_license_number ORDER BY event_date) END)
            OVER (PARTITION BY drivers_license_number ORDER BY event_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
            0
        ) AS events_since_last_30_mark
    FROM violation_counts
),

event_groups_1_2 AS (
    SELECT
        *,
        -- Determine if it should be marked
        CASE 
            WHEN violation_count_24h >= 5 
                 AND events_since_last_24_mark >= 5 THEN 1
            ELSE 0
        END AS should_mark_24,
        CASE 
            WHEN violation_count_30d >= 10
                 AND events_since_last_30_mark >= 10 THEN 1
            ELSE 0
        END AS should_mark_30
    FROM marking_data
)


-- 24 hours violations
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
FROM event_groups_1_2
WHERE should_mark_24 = 1

-- 30 day violations
UNION ALL
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
FROM event_groups_1_2
WHERE should_mark_30 = 1


-- Tampering events
UNION ALL
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


-- Authorized uninstall
UNION ALL
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


-- Unauthorized uninstall
UNION ALL
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


-- Switched vehicle
UNION ALL
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
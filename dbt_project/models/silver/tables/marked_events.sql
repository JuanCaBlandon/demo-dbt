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
    INNER JOIN {{ ref('customer_cleaned') }}  cc
        ON cc.customer_id = cec.customer_id
        AND cc.is_inconsistent = 0
    INNER JOIN {{ ref('batch_customer_cleaned') }} AS bcc
        ON cc.drivers_license_number = bcc.drivers_license_number
        AND RIGHT(bcc.vin,6) = RIGHT(cc.vin,6)
        AND bcc.created_at = (SELECT MAX(created_at) FROM {{ ref('batch_customer_cleaned') }})
        AND bcc.is_inconsistent = 0
        AND bcc.repeat_offender = 1
        AND bcc.offense_date >= "{{ var('start_date', '2025-01-01') }}"
    WHERE 
        cec.is_inconsistent = 0
        AND cec.event_type <> 'TYPE 1-2'
     {% if is_incremental() %}
        AND event_date > (
            SELECT COALESCE(MAX(event_date), "{{ var('start_date', '2025-01-01') }}") FROM {{ this }}
            WHERE 
                customer_id = cec.customer_id
                AND CONCAT('TYPE ', record_type) = cec.event_type
        )
        
    {% endif %}
)

-- 24 hours violations
SELECT
    {{ dbt_utils.generate_surrogate_key(['event_dw_id',"'1'"]) }} AS record_dw_id,
    event_dw_id,
    drivers_license_number,
    customer_id,
    NULL AS new_vin,
    event_id_type,
    event_id,
    event_date,
    record_type,
    record_description
FROM {{ source('SILVER', 'rt_1_2') }} me24
WHERE
    record_type = 1
{% if is_incremental() %}
    AND event_date > (
        SELECT COALESCE(MAX(event_date), "{{ var('start_date', '2025-01-01') }}") FROM {{ this }}
        WHERE 
            drivers_license_number = me24.drivers_license_number
            AND record_type = 1
    )
{% endif %}

-- -- 30 day violations
UNION ALL
SELECT
    {{ dbt_utils.generate_surrogate_key(['event_dw_id',"'2'"]) }} AS record_dw_id, 
    event_dw_id,
    drivers_license_number,
    customer_id,
    NULL AS new_vin,
    event_id_type,
    event_id,
    event_date,
    record_type,
    record_description
FROM {{ source('SILVER', 'rt_1_2') }} me30
WHERE
    record_type = 2
{% if is_incremental() %}
    AND event_date > (
        SELECT COALESCE(MAX(event_date), "{{ var('start_date', '2025-01-01') }}") FROM {{ this }}
        WHERE 
            customer_id = me30.customer_id
            AND record_type = 2
    )
{% endif %}


-- Tampering events
UNION ALL
SELECT
    {{ dbt_utils.generate_surrogate_key(['event_dw_id',"'3'"]) }} AS record_dw_id, 
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
    {{ dbt_utils.generate_surrogate_key(['event_dw_id',"'4'"]) }} AS record_dw_id, 
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
    {{ dbt_utils.generate_surrogate_key(['event_dw_id',"'5'"]) }} AS record_dw_id, 
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
    {{ dbt_utils.generate_surrogate_key(['event_dw_id',"'6'"]) }} AS record_dw_id, 
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

-- Final Compliance
UNION ALL
SELECT
    {{ dbt_utils.generate_surrogate_key(['event_dw_id',"'7'"]) }} AS record_dw_id, 
    event_dw_id,
    drivers_license_number,
    customer_id,
    new_vin,
    'customer_transaction_id' event_id_type,
    customer_transaction_id event_id,
    event_date,
    7 AS record_type,
    'final_compliance' record_description
FROM base_data b
WHERE event_type = 'TYPE 7'
 {{ config(
    materialized='incremental',
    unique_key='customer_state_dw_id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY datetime_id, customer_dw_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ]
) }}

WITH bc AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['c.customer_dw_id','batch_customer_dw_id', 'mv.violation_dw_id', 'rt.record_type_dw_id', 'dd.datetime_id']) }} AS customer_state_dw_id,
        c.customer_dw_id,
        bc.batch_customer_dw_id,
        mv.violation_dw_id,
        rt.record_type_dw_id,
        dd.datetime_id,
        NULL AS error_detail_dw_id
    FROM {{ ref('marked_violations')}} AS mv
    INNER JOIN {{ ref('customer')}} AS c
        ON mv.customer_id = c.customer_id
    INNER JOIN {{ ref('batch_customer') }} AS bc
        ON c.drivers_license_number = bc.drivers_license_number
        AND bc.first_name= c.first_name
        AND bc.last_name =c.last_name
        AND bc.date_of_birth = c.date_of_birth
        AND bc.vin = c.vin
    INNER JOIN {{ ref('dim_date_time') }} AS dd 
        ON year(mv.violation_reporting_approval_date) = dd.year
        AND month(mv.violation_reporting_approval_date) = dd.month
        AND day(mv.violation_reporting_approval_date) = dd.day
        AND HOUR(mv.violation_reporting_approval_date) = dd.hour
        AND  MINUTE(mv.violation_reporting_approval_date) = dd.minute
        AND  SECOND(mv.violation_reporting_approval_date) = dd.second
    INNER JOIN {{ ref('record_type')}} AS rt
        ON mv.record_type = rt.id
)
SELECT
    customer_state_dw_id,
    batch_customer_dw_id,
    customer_dw_id,
    violation_dw_id,
    record_type_dw_id,
    datetime_id,
    error_detail_dw_id,
    current_timestamp() AS current_timestamp
FROM bc
{% if is_incremental() %}
     WHERE datetime_id > (select MAX(datetime_id) from {{ this }})
 {% endif %}


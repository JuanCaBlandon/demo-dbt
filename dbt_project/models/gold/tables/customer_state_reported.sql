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
        {{ dbt_utils.generate_surrogate_key(['c.customer_dw_id','batch_customer_dw_id', 'me.record_dw_id', 'rt.record_type_dw_id', 'dd.datetime_id']) }} AS customer_state_dw_id,
        c.customer_dw_id,
        bc.batch_customer_dw_id,
        me.record_dw_id,
        rt.record_type_dw_id,
        dd.datetime_id,
        CAST(NULL AS INT) AS status
    FROM {{ ref('marked_events')}} AS me
    INNER JOIN {{ ref('customer')}} AS c
        ON me.customer_id = c.customer_id
    INNER JOIN {{ ref('batch_customer') }} AS bc
        ON c.drivers_license_number = bc.drivers_license_number
        AND bc.first_name= c.first_name
        AND bc.last_name =c.last_name
        AND bc.date_of_birth = c.date_of_birth
        AND bc.vin = c.vin
        AND bc.created_at = (SELECT MAX(created_at) FROM {{ ref('batch_customer') }}) --TODO: Needed? Ask Cami
    INNER JOIN {{ ref('dim_date_time') }} AS dd 
        ON year(me.event_date) = dd.year
        AND month(me.event_date) = dd.month
        AND day(me.event_date) = dd.day
        AND HOUR(me.event_date) = dd.hour
        AND  MINUTE(me.event_date) = dd.minute
        AND  SECOND(me.event_date) = dd.second
    INNER JOIN {{ ref('record_type')}} AS rt
        ON me.record_type = rt.id
)
SELECT
    customer_state_dw_id,
    batch_customer_dw_id,
    customer_dw_id,
    record_dw_id,
    record_type_dw_id,
    datetime_id,
    status,
    current_timestamp() AS current_timestamp
FROM bc
{% if is_incremental() %}
     WHERE datetime_id > (select MAX(datetime_id) from {{ this }})
 {% endif %}

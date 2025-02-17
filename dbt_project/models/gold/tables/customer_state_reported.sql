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
),
t7_events AS (
  SELECT
      me.record_dw_id
  FROM state_reporting_prd.silver.marked_events me
  WHERE 
    me.record_type = 7 
    AND NOT EXISTS (
        SELECT 1
        FROM state_reporting_prd.gold.customer_state_reported csr
        INNER JOIN state_reporting_prd.gold.record_type rt
            ON csr.record_type_dw_id = rt.record_type_dw_id
        INNER JOIN state_reporting_prd.gold.customer c
            ON csr.customer_dw_id = c.customer_dw_id
        INNER JOIN state_reporting_prd.silver.marked_events me2
            ON me2.record_dw_id = csr.record_dw_id
        WHERE
            c.drivers_license_number = me.drivers_license_number
            AND rt.id BETWEEN 1 AND 4
            AND me2.event_date BETWEEN c.IID_End_Date - INTERVAL '60' DAY AND c.IID_End_Date
            --   AND csr.status = 1
        )
),
t4_events AS (
  SELECT
      me.record_dw_id
  FROM state_reporting_prd.silver.marked_events me
  WHERE 
    me.record_type = 4
    AND NOT EXISTS (
      SELECT 1
      FROM state_reporting_prd.gold.customer_state_reported csr
      INNER JOIN state_reporting_prd.gold.record_type rt
          ON csr.record_type_dw_id = rt.record_type_dw_id
      INNER JOIN state_reporting_prd.gold.customer c
          ON csr.customer_dw_id = c.customer_dw_id
      WHERE
          c.customer_id = me.customer_id
          AND rt.id = 7
          --   AND csr.status = 1
    )
),
t5_events AS (
  SELECT
      me.record_dw_id
  FROM state_reporting_prd.silver.marked_events me
  WHERE 
    me.record_type = 5
    AND EXISTS (
      SELECT 1
      FROM state_reporting_prd.gold.customer_state_reported csr
      INNER JOIN state_reporting_prd.gold.record_type rt
          ON csr.record_type_dw_id = rt.record_type_dw_id
      INNER JOIN state_reporting_prd.gold.customer c
          ON csr.customer_dw_id = c.customer_dw_id
      WHERE
          c.customer_id = me.customer_id
          AND rt.id = 7
          --   AND csr.status = 1
    )
)
SELECT
    bc.customer_state_dw_id,
    bc.batch_customer_dw_id,
    bc.customer_dw_id,
    bc.record_dw_id,
    bc.record_type_dw_id,
    bc.datetime_id,
    bc.status,
    CAST(NULL AS TIMESTAMP) AS submitted_at
FROM bc
INNER JOIN {{ ref('record_type')}} AS rt
    ON bc.record_type_dw_id = rt.record_type_dw_id
WHERE rt.id IN (1,2,3,6)
{% if is_incremental() %}
    AND NOT EXISTS (SELECT 1 FROM {{ this }} prev WHERE prev.customer_state_dw_id = bc.customer_state_dw_id)
 {% endif %}

--TYPE 7 VALIDATION
UNION ALL
SELECT
    bc.customer_state_dw_id,
    bc.batch_customer_dw_id,
    bc.customer_dw_id,
    bc.record_dw_id,
    bc.record_type_dw_id,
    bc.datetime_id,
    bc.status,
    CAST(NULL AS TIMESTAMP) AS submitted_at
FROM bc
INNER JOIN t7_events USING(record_dw_id)
{% if is_incremental() %}
    WHERE NOT EXISTS (SELECT 1 FROM {{ this }} prev WHERE prev.customer_state_dw_id = bc.customer_state_dw_id)
 {% endif %}

--TYPE 4 VALIDATION
UNION ALL
SELECT
    bc.customer_state_dw_id,
    bc.batch_customer_dw_id,
    bc.customer_dw_id,
    bc.record_dw_id,
    bc.record_type_dw_id,
    bc.datetime_id,
    bc.status,
    CAST(NULL AS TIMESTAMP) AS submitted_at
FROM bc
INNER JOIN t4_events USING(record_dw_id)
{% if is_incremental() %}
    WHERE NOT EXISTS (SELECT 1 FROM {{ this }} prev WHERE prev.customer_state_dw_id = bc.customer_state_dw_id)
 {% endif %}


--TYPE 5 VALIDATION
UNION ALL
SELECT
    bc.customer_state_dw_id,
    bc.batch_customer_dw_id,
    bc.customer_dw_id,
    bc.record_dw_id,
    bc.record_type_dw_id,
    bc.datetime_id,
    bc.status,
    CAST(NULL AS TIMESTAMP) AS submitted_at
FROM bc
INNER JOIN t5_events USING(record_dw_id)
{% if is_incremental() %}
    WHERE NOT EXISTS (SELECT 1 FROM {{ this }} prev WHERE prev.customer_state_dw_id = bc.customer_state_dw_id)
 {% endif %}
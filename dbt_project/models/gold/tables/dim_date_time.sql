{{ config(
		materialized='incremental',
    unique_key='datetime_id',
    partition_by = "year" ,
		post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY datetime_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"

        ]
) }}

WITH hours AS (
  SELECT EXPLODE(sequence(0, 23)) AS hour
), minutes AS (
  SELECT EXPLODE(sequence(0, 59)) AS minute
), seconds AS (
  SELECT EXPLODE(sequence(0, 59)) AS second
)
SELECT
    CONCAT(date_id, LPAD(CAST(hour AS STRING), 2, '0'), LPAD(CAST(minute AS STRING), 2, '0'), LPAD(CAST(second AS STRING), 2, '0')) AS datetime_id,
    TO_TIMESTAMP(format_string('%s %02d:%02d:%02d', CAST(date_full AS STRING), hour, minute,second)) AS datetime_full,
    year,
    quarter,
    month,
    year_month,
    week,
    day,
    hour,
    minute,
    second,
    day_of_week,
    day_name,
    month_name,
    is_weekday,
    is_leapyear
FROM (
    SELECT date_id, date_full,year,quarter,month,year_month,week,day,day_of_week, day_name, month_name, is_weekday, is_leapyear
    FROM {{ ref('dim_date') }}
    WHERE date_full >= "{{ var("start_date", "2025-01-01") }}"
    AND date_full <= DATE_TRUNC('MONTH', ADD_MONTHS("{{ var("execution_date", "2025-01-01") }}", 1)) 
    {% if is_incremental() %}
     AND month > (select MAX(month) from {{ this }})
 {% endif %}
) AS dim_date
CROSS JOIN hours
CROSS JOIN minutes
CROSS JOIN seconds;
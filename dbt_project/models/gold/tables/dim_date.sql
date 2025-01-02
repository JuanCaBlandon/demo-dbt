
  {{ config(
		materialized = 'table' , 
		post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY date_id;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"

        ]
) }}

SELECT
  replace(calendar_date, '-', '') as date_id,
  calendar_date AS date_full,
  YEAR(calendar_date) AS year,
  QUARTER(calendar_date) AS quarter,
  MONTH(calendar_date) AS month,
  CASE
    WHEN length((CAST(MONTH(calendar_date) AS STRING))) = 1 THEN CONCAT(
      CAST(YEAR(calendar_date) AS STRING),
      '_',
      CONCAT('0', CAST(MONTH(calendar_date) AS STRING))
    )
    ELSE CONCAT(
      CAST(YEAR(calendar_date) AS STRING),
      '_',
      CAST(MONTH(calendar_date) AS STRING)
    )
  END AS year_month,
  weekofyear(calendar_date) AS week,
  DAY(calendar_date) AS day,
  WEEKDAY(calendar_date) AS day_of_week,
  date_format(calendar_date, 'E') as day_name,
  date_format(calendar_date, 'MMMM') as month_name,
  CASE
    WHEN dayofweek(calendar_date) IN (1, 7) THEN 0
    ELSE 1
  END AS is_weekday,
  CASE
    WHEN year(calendar_date) % 4 = 0 THEN 1
    ELSE 0
  END AS is_leapyear
FROM
  (
    SELECT
      explode(
        sequence(
          DATE '1900-01-01',
          DATE '9999-12-31',
          INTERVAL 1 DAY
        )
      ) as calendar_date
  ) AS dates;
%sql
CREATE OR REFRESH STREAMING TABLE bronze.orders_bronze_demo2
AS
SELECT
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  "${source}/orders", 
  format => 'CSV',
  header => true
);




CREATE OR REFRESH STREAMING TABLE silver.orders_silver_demo2
AS
SELECT
  CAST(SalesOrderNumber AS BIGINT) as order_id,
  CAST(OrderDate AS TIMESTAMP) as order_timestamp,
  CustomerKey,
  CAST(TotalDue AS DECIMAL(10,2)) as amount,
  processing_time
FROM STREAM bronze.orders_bronze_demo2
WHERE SalesOrderNumber IS NOT NULL
  AND CustomerKey IS NOT NULL
  AND TotalDue > 0;



CREATE OR REFRESH MATERIALIZED VIEW gold.gold_orders_by_date_demo2
AS
SELECT
  DATE(order_timestamp) AS order_date,
  COUNT(*) AS total_daily_orders,
  SUM(amount) AS total_daily_revenue,
  AVG(amount) AS avg_order_value,
  COUNT(DISTINCT CustomerKey) AS unique_customers
FROM silver.orders_silver_demo2
GROUP BY DATE(order_timestamp);
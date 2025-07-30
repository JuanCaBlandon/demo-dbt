{{ config(
    materialized='incremental',
    unique_key='customer_dw_id',
    incremental_strategy='merge',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY customer_key;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ],
    tags=["gold_customer"]
) }}

-- Obtener datos limpios de la capa silver y enriquecerlos
WITH enriched_customers AS (
    SELECT 
        c.customer_dw_id,
        c.customer_key,
        c.geography_key,
        c.customer_type,
        c.customer_id,
        c.guid,
        c.date_created,
        -- Añadir métricas enriquecidas calculadas con datos de ventas 
        COALESCE(COUNT(s.sales_order_number), 0) AS total_purchases,
        COALESCE(AVG(s.total_due), 0) AS avg_purchase_amount,
        MIN(s.order_date) AS first_purchase_date,
        MAX(s.order_date) AS last_purchase_date,
        -- Segmentación básica de clientes
        CASE
            WHEN COUNT(s.sales_order_number) > 10 THEN 'Frecuente'
            WHEN COUNT(s.sales_order_number) > 5 THEN 'Regular'
            WHEN COUNT(s.sales_order_number) > 0 THEN 'Ocasional'
            ELSE 'Nuevo'
        END AS customer_segment,
        -- Determinar si el cliente está activo (compra en los últimos 90 días)
        CASE
            WHEN MAX(s.order_date) > DATE_ADD(CURRENT_DATE(), -90) THEN 1
            ELSE 0
        END AS is_active,
        1 AS is_current,
        -- Relacionar con dimensión de fecha
        dd.date_id
    FROM {{ ref('customer_cleaned') }} c
    LEFT JOIN {{ ref('sales_order_header_cleaned') }} s
        ON c.customer_key = s.customer_key
        AND s.is_inconsistent = 0
    INNER JOIN {{ ref('dim_date') }} dd
        ON TO_DATE(c.date_created) = dd.date_full
    WHERE 
        c.is_inconsistent = 0
        AND c.is_current = 1
    GROUP BY 
        c.customer_dw_id,
        c.customer_key,
        c.geography_key,
        c.customer_type,
        c.customer_id,
        c.guid,
        c.date_created,
        dd.date_id
)

SELECT * FROM enriched_customers

{% if is_incremental() %}
  -- Este filtro es crucial para 'append' y asegurar que solo se inserten nuevos clientes
  WHERE customer_dw_id NOT IN (SELECT customer_dw_id FROM {{ this }})
{% endif %}
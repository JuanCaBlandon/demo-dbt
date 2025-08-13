{{ config(
    materialized='incremental',
    unique_key='sales_fact_dw_id',
    incremental_strategy='merge',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY sales_order_number;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ],
    tags=["gold_sales_fact"]
) }}

WITH sales_metrics AS (
    SELECT 
        -- Claves de negocio
        soh.sales_order_number,
        soh.sales_order_id,
        soh.customer_key,
        soh.store_key,
        soh.sales_person_key,
        
        -- Fechas para relaciones con dim_date
        soh.order_date,
        soh.due_date,
        soh.ship_date,
        
        -- Métricas financieras
        COALESCE(soh.sub_total, 0) AS sub_total,
        COALESCE(soh.tax_amount, 0) AS tax_amount,
        COALESCE(soh.freight, 0) AS freight,
        COALESCE(soh.total_due, 0) AS total_due,
        
        -- Métricas calculadas
        CASE 
            WHEN soh.sub_total > 0 THEN 
                ROUND((soh.tax_amount / soh.sub_total) * 100, 2)
            ELSE 0 
        END AS tax_percentage,
        
        CASE 
            WHEN soh.sub_total > 0 THEN 
                ROUND((soh.freight / soh.sub_total) * 100, 2)
            ELSE 0 
        END AS freight_percentage,
        
        -- Métricas de tiempo
        CASE 
            WHEN soh.ship_date IS NOT NULL AND soh.order_date IS NOT NULL THEN
                DATEDIFF(soh.ship_date, soh.order_date)
            ELSE NULL 
        END AS days_to_ship,
        
        CASE 
            WHEN soh.due_date IS NOT NULL AND soh.order_date IS NOT NULL THEN
                DATEDIFF(soh.due_date, soh.order_date)
            ELSE NULL 
        END AS days_to_due,
        
        -- Indicadores de negocio
        CASE 
            WHEN soh.online_order_flag = 1 THEN 'Online'
            WHEN soh.online_order_flag = 0 THEN 'Offline'
            ELSE 'Unknown'
        END AS order_channel,
        
        CASE 
            WHEN soh.status = 1 THEN 'In Process'
            WHEN soh.status = 2 THEN 'Approved'
            WHEN soh.status = 3 THEN 'Backordered'
            WHEN soh.status = 4 THEN 'Rejected'
            WHEN soh.status = 5 THEN 'Shipped'
            WHEN soh.status = 6 THEN 'Cancelled'
            ELSE 'Unknown'
        END AS order_status,
        
        -- Indicadores de cumplimiento
        CASE 
            WHEN soh.ship_date IS NOT NULL THEN 1
            ELSE 0 
        END AS is_shipped,
        
        CASE 
            WHEN soh.ship_date IS NOT NULL AND soh.due_date IS NOT NULL 
                 AND soh.ship_date <= soh.due_date THEN 1
            WHEN soh.ship_date IS NOT NULL AND soh.due_date IS NULL THEN 1
            ELSE 0 
        END AS is_on_time,
        
        -- Indicadores de calidad de datos
        soh.is_inconsistent,
        soh.inconsistency_id,
        
        -- Campos de auditoría
        soh.modified_date,
        CURRENT_TIMESTAMP() AS fact_created_at
        
    FROM {{ ref('sales_order_header_cleaned') }} soh
    WHERE soh.is_inconsistent = 0  -- Solo datos consistentes para el fact
),

enriched_sales AS (
    SELECT 
        sm.*,
        
        -- Relación con dim_date para order_date
        dd_order.date_id AS order_date_id,
        dd_order.year AS order_year,
        dd_order.quarter AS order_quarter,
        dd_order.month AS order_month,
        dd_order.week AS order_week,
        dd_order.day_name AS order_day_name,
        dd_order.is_weekday AS order_is_weekday,
        
        -- Relación con dim_date para due_date
        dd_due.date_id AS due_date_id,
        dd_due.year AS due_year,
        dd_due.quarter AS due_quarter,
        dd_due.month AS due_month,
        
        -- Relación con dim_date para ship_date
        dd_ship.date_id AS ship_date_id,
        dd_ship.year AS ship_year,
        dd_ship.quarter AS ship_quarter,
        dd_ship.month AS ship_month,
        
        -- Información del cliente
        c.customer_type,
        c.customer_segment,
        c.is_active AS customer_is_active,
        
        -- Información de la tienda
        s.store_name,
        s.specialty
,
        
        -- Información del producto (si hay detalles de línea)
        -- Aquí podrías agregar joins con sales_order_detail si existe
        
        -- Métricas adicionales calculadas
        CASE 
            WHEN sm.total_due >= 1000 THEN 'High Value'
            WHEN sm.total_due >= 500 THEN 'Medium Value'
            WHEN sm.total_due >= 100 THEN 'Low Value'
            ELSE 'Minimal Value'
        END AS order_value_category,
        
        -- Indicador de orden urgente
        CASE 
            WHEN sm.days_to_due IS NOT NULL AND sm.days_to_due <= 3 THEN 1
            ELSE 0 
        END AS is_urgent_order,
        
        -- Indicador de orden premium
        CASE 
            WHEN sm.total_due >= 2000 OR sm.freight_percentage > 10 THEN 1
            ELSE 0 
        END AS is_premium_order
        
    FROM sales_metrics sm
    
    -- Join con dim_date para order_date
    LEFT JOIN {{ ref('dim_date') }} dd_order
        ON TO_DATE(sm.order_date) = dd_order.date_full
    
    -- Join con dim_date para due_date
    LEFT JOIN {{ ref('dim_date') }} dd_due
        ON TO_DATE(sm.due_date) = dd_due.date_full
    
    -- Join con dim_date para ship_date
    LEFT JOIN {{ ref('dim_date') }} dd_ship
        ON TO_DATE(sm.ship_date) = dd_ship.date_full
    
    -- Join con customer dimension
    LEFT JOIN {{ ref('customer') }} c
        ON sm.customer_key = c.customer_key
        AND c.is_current = 1
    
    -- Join con store (asumiendo que existe una dimensión store)
    LEFT JOIN {{ ref('store_cleaned') }} s
        ON sm.store_key = s.store_id
        AND s.is_current = 1
        AND s.is_inconsistent = 0
),

final_fact AS (
    SELECT
        -- Clave primaria del fact
        {{ dbt_utils.generate_surrogate_key(['sales_order_number', 'customer_key', 'order_date']) }} AS sales_fact_dw_id,
        
        -- Claves de negocio
        sales_order_number,
        sales_order_id,
        customer_key,
        store_key,
        sales_person_key,
        
        -- Claves de fecha (dimensiones)
        order_date_id,
        due_date_id,
        ship_date_id,
        
        -- Métricas financieras
        sub_total,
        tax_amount,
        freight,
        total_due,
        tax_percentage,
        freight_percentage,
        
        -- Métricas de tiempo
        days_to_ship,
        days_to_due,
        
        -- Categorías y segmentaciones
        order_channel,
        order_status,
        order_value_category,
        customer_type,
        customer_segment,
        store_name,
        specialty
,
        
        -- Indicadores de negocio
        is_shipped,
        is_on_time,
        is_urgent_order,
        is_premium_order,
        customer_is_active,
        
        -- Indicadores de calidad
        is_inconsistent,
        inconsistency_id,
        
        -- Campos de auditoría
        modified_date,
        fact_created_at,
        
        -- Campos de fecha para análisis temporal
        order_year,
        order_quarter,
        order_month,
        order_week,
        order_day_name,
        order_is_weekday
        
    FROM enriched_sales
)


SELECT * FROM final_fact

{% if is_incremental() %}
   -- Filtra solo los registros que son nuevos o han sido modificados en la fuente
  -- desde la última ejecución incremental.
  WHERE final_fact.modified_date > (SELECT MAX(modified_date) FROM {{ this }})
{% endif %}
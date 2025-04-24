{{ config(
    materialized='incremental',
    unique_key='sales_header_dw_id',
    incremental_strategy='merge',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY sales_order_number;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
) }}

WITH Tmp AS(
  SELECT
    SalesOrderNumber AS sales_order_number,
    RevisionNumber AS revision_number,
    OrderDate AS order_date,
    DueDate AS due_date,
    ShipDate AS ship_date,
    Status AS status,
    OnlineOrderFlag AS online_order_flag,
    SalesOrderID AS sales_order_id,
    PurchaseOrderNumber AS purchase_order_number,
    AccountNumber AS account_number,
    CustomerKey AS customer_key,
    StoreKey AS store_key,
    SalesPersonKey AS sales_person_key,
    BillToAddressKey AS bill_to_address_key,
    ShipToAddressKey AS ship_to_address_key,
    ShipMethodKey AS ship_method_key,
    CreditCardKey AS credit_card_key,
    CreditCardApprovalCode AS credit_card_approval_code,
    SubTotal AS sub_total,
    TaxAmount AS tax_amount,
    Freight AS freight,
    TotalDue AS total_due,
    Comment AS comment,
    GUID AS guid,
    ModifiedDate AS modified_date,
    ROW_NUMBER() OVER (PARTITION BY SalesOrderNumber ORDER BY ModifiedDate DESC) AS num_duplicates
  FROM {{ source('BRONZE', 'sales_order_header_raw') }}
),

-- Buscar clientes válidos para validación de integridad referencial
valid_customers AS (
  SELECT DISTINCT customer_key
  FROM {{ ref('customer_cleaned') }}
  WHERE is_current = 1 AND is_inconsistent = 0
),

-- Buscar tiendas válidas
valid_stores AS (
  SELECT DISTINCT store_id AS store_key  
  FROM {{ ref('store_cleaned') }}
  WHERE is_current = 1
),

cleaned_data AS(
  -- Identificar duplicados
  SELECT
      {{ dbt_utils.generate_surrogate_key(['sales_order_number', 'customer_key', 'order_date', 'num_duplicates']) }} AS sales_header_dw_id,
      sales_order_number,
      revision_number,
      order_date,
      due_date,
      ship_date,
      status,
      online_order_flag,
      sales_order_id,
      purchase_order_number,
      account_number,
      customer_key,
      -- Normalizar store_key a -1 (N/A) si no existe o es nulo
      CASE 
        WHEN store_key IS NULL OR 
             store_key NOT IN (SELECT store_key FROM valid_stores)
        THEN -1
        ELSE store_key 
      END AS store_key,
      sales_person_key,
      bill_to_address_key,
      ship_to_address_key,
      ship_method_key,
      credit_card_key,
      credit_card_approval_code,
      sub_total,
      tax_amount,
      freight,
      total_due,
      comment,
      guid,
      modified_date,
      1 AS is_inconsistent,
      1 AS inconsistency_id, -- Duplicados
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE num_duplicates > 1

  UNION ALL
  
  -- Identificar valores nulos en campos críticos
  SELECT
      {{ dbt_utils.generate_surrogate_key(['sales_order_number', 'customer_key', 'order_date', 'num_duplicates']) }} AS sales_header_dw_id,
      sales_order_number,
      revision_number,
      order_date,
      due_date,
      ship_date,
      status,
      online_order_flag,
      sales_order_id,
      purchase_order_number,
      account_number,
      customer_key,
      CASE 
        WHEN store_key IS NULL OR 
             store_key NOT IN (SELECT store_key FROM valid_stores)
        THEN -1
        ELSE store_key 
      END AS store_key,
      sales_person_key,
      bill_to_address_key,
      ship_to_address_key,
      ship_method_key,
      credit_card_key,
      credit_card_approval_code,
      sub_total,
      tax_amount,
      freight,
      total_due,
      comment,
      guid,
      modified_date,
      1 AS is_inconsistent,
      2 AS inconsistency_id, -- Valores nulos
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE num_duplicates = 1 AND 
    (sales_order_number IS NULL OR customer_key IS NULL OR order_date IS NULL)

  UNION ALL
  
  -- Identificar valores fuera de rango o inválidos
  SELECT
      {{ dbt_utils.generate_surrogate_key(['sales_order_number', 'customer_key', 'order_date', 'num_duplicates']) }} AS sales_header_dw_id,
      sales_order_number,
      revision_number,
      order_date,
      due_date,
      ship_date,
      status,
      online_order_flag,
      sales_order_id,
      purchase_order_number,
      account_number,
      customer_key,
      CASE 
        WHEN store_key IS NULL OR 
             store_key NOT IN (SELECT store_key FROM valid_stores)
        THEN -1
        ELSE store_key 
      END AS store_key,
      sales_person_key,
      bill_to_address_key,
      ship_to_address_key,
      ship_method_key,
      credit_card_key,
      credit_card_approval_code,
      sub_total,
      tax_amount,
      freight,
      total_due,
      comment,
      guid,
      modified_date,
      1 AS is_inconsistent,
      3 AS inconsistency_id, -- Valores fuera de rango
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE num_duplicates = 1 AND (
      sales_order_number <= 0 OR
      sub_total < 0 OR
      tax_amount < 0 OR
      freight < 0 OR
      total_due < 0 OR
      order_date > CURRENT_TIMESTAMP() OR -- Fecha futura
      due_date < order_date OR -- Fecha de vencimiento anterior a la orden
      (ship_date IS NOT NULL AND ship_date < order_date) OR -- Fecha de envío anterior a la orden
      modified_date > CURRENT_TIMESTAMP() -- Fecha futura
    )

  UNION ALL
  
  -- Identificar problemas de integridad referencial con Customer
  SELECT
      {{ dbt_utils.generate_surrogate_key(['sales_order_number', 'customer_key', 'order_date', 'num_duplicates']) }} AS sales_header_dw_id,
      sales_order_number,
      revision_number,
      order_date,
      due_date,
      ship_date,
      status,
      online_order_flag,
      sales_order_id,
      purchase_order_number,
      account_number,
      customer_key,
      CASE 
        WHEN store_key IS NULL OR 
             store_key NOT IN (SELECT store_key FROM valid_stores)
        THEN -1
        ELSE store_key 
      END AS store_key,
      sales_person_key,
      bill_to_address_key,
      ship_to_address_key,
      ship_method_key,
      credit_card_key,
      credit_card_approval_code,
      sub_total,
      tax_amount,
      freight,
      total_due,
      comment,
      guid,
      modified_date,
      1 AS is_inconsistent,
      5 AS inconsistency_id, -- Problemas de integridad referencial
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE num_duplicates = 1 
    AND customer_key IS NOT NULL
    AND customer_key NOT IN (SELECT customer_key FROM valid_customers)

  UNION ALL
  
  -- Datos correctos
  SELECT
      {{ dbt_utils.generate_surrogate_key(['sales_order_number', 'customer_key', 'order_date', 'num_duplicates']) }} AS sales_header_dw_id,
      sales_order_number,
      revision_number,
      order_date,
      due_date,
      ship_date,
      status,
      online_order_flag,
      sales_order_id,
      purchase_order_number,
      account_number,
      customer_key,
      CASE 
        WHEN store_key IS NULL OR 
             store_key NOT IN (SELECT store_key FROM valid_stores)
        THEN -1
        ELSE store_key 
      END AS store_key,
      sales_person_key,
      bill_to_address_key,
      ship_to_address_key,
      ship_method_key,
      credit_card_key,
      credit_card_approval_code,
      sub_total,
      tax_amount,
      freight,
      total_due,
      comment,
      guid,
      modified_date,
      0 AS is_inconsistent,
      0 AS inconsistency_id, -- Sin inconsistencias
      num_duplicates,
      1 AS is_current
  FROM Tmp
  WHERE 
    num_duplicates = 1 AND
    sales_order_number IS NOT NULL AND 
    customer_key IS NOT NULL AND
    order_date IS NOT NULL AND
    -- Valor en rango
    sales_order_number > 0 AND
    (sub_total IS NULL OR sub_total >= 0) AND
    (tax_amount IS NULL OR tax_amount >= 0) AND
    (freight IS NULL OR freight >= 0) AND
    (total_due IS NULL OR total_due >= 0) AND
    -- Fechas válidas
    order_date <= CURRENT_TIMESTAMP() AND
    (due_date IS NULL OR due_date >= order_date) AND
    (ship_date IS NULL OR ship_date >= order_date) AND
    (modified_date IS NULL OR modified_date <= CURRENT_TIMESTAMP()) AND
    -- Integridad referencial con customer
    customer_key IN (SELECT customer_key FROM valid_customers)
)

{% if is_incremental() %}
  SELECT * FROM cleaned_data
  WHERE NOT EXISTS (
    SELECT 1 FROM {{ this }} existing
    WHERE existing.sales_order_number = cleaned_data.sales_order_number
  )
{% else %}
  SELECT * FROM cleaned_data
{% endif %}
{{ config(
		materialized='incremental',
    unique_key='event_dw_id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY customer_id, event_type, event_date;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ],
    tags=["silver_ia_1"]
) }}

WITH tmp AS(
  SELECT
      CustomerId AS customer_id,
      DeviceUsageViolationID AS device_usage_violation_id,
      DeviceUsageEventViolationID AS device_usage_event_violation_id,
      CustomerTransactionID AS customer_transaction_id,
      DeviceUsageID AS device_usage_id,
      EventType AS event_type,
      ViolationReportingApprovalCd AS violation_reporting_approval_cd,
      ViolationReportingApprovalUser AS violation_reporting_approval_user,
      CreateDate AS create_date,
      CreateUser AS create_user,
      ModifyDate AS modify_date,
      ModifyUser AS modify_user,
      LogEntryTime AS log_entry_time,
      EventDate AS event_date,
      VIN AS vin,
      NewVIN AS new_vin,
      CreationDate AS created_at,
      ModificationDate AS modification_date,
      row_number() OVER (PARTITION BY CustomerId,EventType,EventDate ORDER BY EventDate) AS num_duplicates
  FROM
    {{ source('BRONZE', 'customer_events') }}
),
cleaned_data AS(
  SELECT
      {{ dbt_utils.generate_surrogate_key(['customer_id','event_type','event_date']) }} AS event_dw_id, 
      'N/A' AS customer_dw_id,
      customer_id,
      device_usage_violation_id,
      device_usage_event_violation_id,
      customer_transaction_id,
      device_usage_id,
      event_type,
      violation_reporting_approval_cd,
      violation_reporting_approval_user,
      create_date,
      create_user,
      modify_date,
      modify_user,
      log_entry_time,
      event_date,
      vin,
      new_vin,
      created_at,
      modification_date,
      1 AS is_inconsistent,
      'Duplicates' AS type_inconsistent,
      num_duplicates
  FROM tmp
  WHERE num_duplicates > 1

  UNION ALL
  SELECT
      {{ dbt_utils.generate_surrogate_key(['customer_id','event_type','event_date']) }} AS event_dw_id, 
      'N/A' AS customer_dw_id,
      customer_id,
      device_usage_violation_id,
      device_usage_event_violation_id,
      customer_transaction_id,
      device_usage_id,
      event_type,
      violation_reporting_approval_cd,
      violation_reporting_approval_user,
      create_date,
      tmp.create_user,
      tmp.modify_date,
      tmp.modify_user,
      log_entry_time,
      event_date,
      vin,
      new_vin,
      created_at,
      modification_date,
      1 AS is_inconsistent,
      'NULL values' AS type_inconsistent,
    num_duplicates
  FROM tmp
  WHERE num_duplicates = 1 AND 
    (customer_id IS NULL OR event_date IS NULL )

  UNION ALL
  SELECT
      {{ dbt_utils.generate_surrogate_key(['tmp.customer_id','event_type','event_date']) }} AS event_dw_id, 
      'N/A' AS customer_dw_id,
      tmp.customer_id,
      tmp.device_usage_violation_id,
      tmp.device_usage_event_violation_id,
      tmp.customer_transaction_id,
      tmp.device_usage_id,
      tmp.event_type,
      tmp.violation_reporting_approval_cd,
      tmp.violation_reporting_approval_user,
      tmp.create_date,
      tmp.create_user,
      tmp.modify_date,
      tmp.modify_user,
      tmp.log_entry_time,
      tmp.event_date,
      tmp.vin,
      tmp.new_vin,
      tmp.created_at,
      tmp.modification_date,
      1 AS is_inconsistent,
      'Without reference entity' AS type_inconsistent,
      tmp.num_duplicates
  FROM tmp
  LEFT JOIN {{ ref('customer_cleaned') }} AS c
    ON  tmp.customer_id = c.customer_id
  WHERE 
    tmp.num_duplicates = 1
    AND tmp.customer_id IS NOT NULL
    AND tmp.event_date IS NOT NULL 
    AND c.customer_id IS NULL

  UNION ALL
  SELECT
      {{ dbt_utils.generate_surrogate_key(['tmp.customer_id','event_type','event_date']) }} AS event_dw_id, 
      'N/A' AS customer_dw_id,
      tmp.customer_id,
      tmp.device_usage_violation_id,
      tmp.device_usage_event_violation_id,
      tmp.customer_transaction_id,
      tmp.device_usage_id,
      tmp.event_type,
      tmp.violation_reporting_approval_cd,
      tmp.violation_reporting_approval_user,
      tmp.create_date,
      tmp.create_user,
      tmp.modify_date,
      tmp.modify_user,
      tmp.log_entry_time,
      tmp.event_date,
      tmp.vin,
      tmp.new_vin,
      tmp.created_at,
      tmp.modification_date,
      1 AS is_inconsistent,
      'Reference entity inconsistent' AS type_inconsistent,
      tmp.num_duplicates
  FROM tmp
  INNER JOIN {{ ref('customer_cleaned') }} AS c
    ON  tmp.customer_id = c.customer_id
    AND c.is_inconsistent = 1
  WHERE 
    tmp.num_duplicates = 1
    AND tmp.customer_id IS NOT NULL
    AND tmp.event_date IS NOT NULL 
    
  UNION ALL
  SELECT
      {{ dbt_utils.generate_surrogate_key(['tmp.customer_id','event_type','event_date']) }} AS event_dw_id, 
      customer_dw_id,
      tmp.customer_id,
      tmp.device_usage_violation_id,
      tmp.device_usage_event_violation_id,
      tmp.customer_transaction_id,
      tmp.device_usage_id,
      tmp.event_type,
      tmp.violation_reporting_approval_cd,
      tmp.violation_reporting_approval_user,
      tmp.create_date,
      tmp.create_user,
      tmp.modify_date,
      tmp.modify_user,
      tmp.log_entry_time,
      tmp.event_date,
      tmp.vin,
      tmp.new_vin,
      tmp.created_at,
      tmp.modification_date,
      0 AS is_inconsistent,
      'N/A' AS type_inconsistent,
      tmp.num_duplicates
  FROM tmp
  INNER JOIN {{ ref('customer_cleaned') }} AS c
    ON  tmp.customer_id = c.customer_id
  WHERE 
    tmp.num_duplicates = 1
    AND tmp.customer_id IS NOT NULL
    AND tmp.event_date IS NOT NULL 
    AND c.is_inconsistent = 0
)

SELECT
    event_dw_id, 
    customer_dw_id,
    customer_id,
    device_usage_violation_id,
    device_usage_event_violation_id,
    customer_transaction_id,
    device_usage_id,
    event_type,
    violation_reporting_approval_cd,
    violation_reporting_approval_user,
    create_date,
    create_user,
    modify_date,
    modify_user,
    log_entry_time,
    event_date,
    vin,
    new_vin,
    created_at,
    modification_date,
    is_inconsistent,
    type_inconsistent,
    num_duplicates
  FROM cleaned_data
  {% if is_incremental() %}
    WHERE event_dw_id NOT IN (SELECT event_dw_id FROM {{ this }})
{% endif %}




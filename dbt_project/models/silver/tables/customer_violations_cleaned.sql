{{ config(
		materialized='incremental',
    unique_key='violation_dw_id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY customer_id, violation_type, violation_date;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
) }}

with tmp as(
SELECT
    CustomerId AS customer_id,
    DeviceUsageViolationID AS device_usage_violation_id,
    DeviceUsageEventViolationID AS device_usage_event_violation_id,
    DeviceUsageID AS device_usage_id,
    ViolationType AS violation_type,
    ViolationReportingApprovalCd AS violation_reporting_approval_cd,
    ViolationReportingApprovalUser AS violation_reporting_approval_user,
    --ViolationReportingApprovalDate AS violation_reporting_approval_date,
    Comments AS comments,
    CreateDate AS create_date,
    CreateUser AS create_user,
    ModifyDate AS modify_date,
    ModifyUser AS modify_user,
    LogEntryTime AS log_entry_time,
    ViolationDate AS violation_date,
    current_timestamp() as created_at,
    row_number() over (partition by CustomerId,ViolationType,ViolationDate order by ViolationDate) as num_duplicates
FROM
  {{ source('BRONZE', 'customer_violations') }}


),
cleaned_data AS(

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id','violation_type','violation_date']) }} as violation_dw_id, 
    'N/A' as customer_dw_id,
    customer_id,
    device_usage_violation_id,
    device_usage_event_violation_id,
    device_usage_id,
    violation_type,
    violation_reporting_approval_cd,
    violation_reporting_approval_user,
    comments,
    create_date,
    create_user,
    modify_date,
    modify_user,
    log_entry_time,
    violation_date,
    created_at,
    1 as is_inconsistent,
    'duplicates' as type_inconsistent,
    num_duplicates
FROM tmp
WHERE num_duplicates > 1

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id','violation_type','violation_date']) }} as violation_dw_id, 
    'N/A' as customer_dw_id,
    customer_id,
    device_usage_violation_id,
    device_usage_event_violation_id,
    device_usage_id,
    violation_type,
    violation_reporting_approval_cd,
    violation_reporting_approval_user,
    comments,
    create_date,
    tmp.create_user,
    tmp.modify_date,
    tmp.modify_user,
    log_entry_time,
    violation_date,
    created_at,
  1 as is_inconsistent,
  'NULL values' as type_inconsistent,
  num_duplicates
FROM tmp
WHERE num_duplicates = 1 AND 
  (customer_id IS NULL OR violation_date IS NULL )

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(['tmp.customer_id','violation_type','violation_date']) }} as violation_dw_id, 
    'N/A' as customer_dw_id,
    tmp.customer_id,
    device_usage_violation_id,
    device_usage_event_violation_id,
    device_usage_id,
    violation_type,
    violation_reporting_approval_cd,
    violation_reporting_approval_user,
    comments,
    create_date,
    tmp.create_user,
    tmp.modify_date,
    tmp.modify_user,
    log_entry_time,
    violation_date,
    tmp.created_at,
    1 as is_inconsistent,
    'Without reference entity' as type_inconsistent,
    tmp.num_duplicates
FROM tmp
LEFT JOIN {{ ref('customer_cleaned') }} as c
ON  tmp.customer_id = c.customer_id
WHERE tmp.num_duplicates = 1 AND tmp.customer_id IS NOT NULL AND tmp.violation_date IS NOT NULL 
AND c.customer_id IS NULL
  
UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(['tmp.customer_id','violation_type','violation_date']) }} as violation_dw_id, 
    customer_dw_id,
    tmp.customer_id,
    device_usage_violation_id,
    device_usage_event_violation_id,
    device_usage_id,
    violation_type,
    violation_reporting_approval_cd,
    violation_reporting_approval_user,
    comments,
    create_date,
    tmp.create_user,
    tmp.modify_date,
    tmp.modify_user,
    log_entry_time,
    violation_date,
    tmp.created_at,
    0 as is_inconsistent,
    'N/A' as type_inconsistent,
    tmp.num_duplicates
FROM tmp
INNER JOIN {{ ref('customer_cleaned') }} as c
ON  tmp.customer_id = c.customer_id
WHERE tmp.num_duplicates = 1 AND tmp.customer_id IS NOT NULL AND tmp.violation_date IS NOT NULL 
AND c.is_inconsistent = 0

)

SELECT
    violation_dw_id, 
    customer_dw_id,
    customer_id,
    device_usage_violation_id,
    device_usage_event_violation_id,
    device_usage_id,
    violation_type,
    violation_reporting_approval_cd,
    violation_reporting_approval_user,
    comments,
    create_date,
    create_user,
    modify_date,
    modify_user,
    log_entry_time,
    violation_date,
    created_at,
    is_inconsistent,
    type_inconsistent,
    num_duplicates
  FROM cleaned_data
  {% if is_incremental() %}
    where violation_dw_id not in (select violation_dw_id from {{ this }})
{% endif %}





-- models/current_inconsistencies.sql
{{ config(
    materialized='incremental',
    database='compliance_' ~ var('DEPLOYMENT_ENVIRONMENT'),
    unique_key='id',
    post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY id, business_key;",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
    ]
) }}

WITH batch_customer_inconsistencies AS (
    -- Inconsistencias en batch_customer_cleaned
    SELECT
        audit.audit_inconsistent_dw_id,
        audit.source_table_id as business_key,
        audit.source_table_name,
        audit.inconsistency_id,
        audit.created_at as detection_date,
        ityp.inconsistency_type,
        ityp.inconsistency_description as description,
        'batch_file' as entity,
        ityp.inconsistencies_category as source,
        'PENDING' as status,
        TO_JSON(STRUCT(
            bcc.batch_customer_dw_id as batch_customer_dw_id,
            bcc.drivers_license_number,
            bcc.created_at,
            audit.source_table_name as table_name,
            audit.inconsistency_id as error_code,
            ityp.inconsistency_description as error_message,
            current_timestamp() as processed_timestamp
        )) as technical_details,
        -- Extraer batch_id del batch_customer_dw_id
        REGEXP_EXTRACT(bcc.batch_customer_dw_id, '^BATCH-(\\d+)-.*$', 1) as batch_id
    FROM {{ ref('audit_inconsistencies') }} as audit
    INNER JOIN compliance_{{ var('DEPLOYMENT_ENVIRONMENT') }}.compliance.type_inconsistencies as ityp 
        ON audit.inconsistency_id = ityp.inconsistency_id
    INNER JOIN state_reporting_{{ var('DEPLOYMENT_ENVIRONMENT') }}.silver.batch_customer_cleaned as bcc
        ON audit.source_table_id = bcc.batch_customer_dw_id
    WHERE source_table_name = 'batch_customer_cleaned'
    {% if is_incremental() %}
        AND audit.created_at > (SELECT COALESCE(max(detection_date), '1970-01-01') FROM {{ this }})
    {% endif %}
),

customer_inconsistencies AS (
    -- Inconsistencias en customer_cleaned
    SELECT
        audit.audit_inconsistent_dw_id,
        audit.source_table_id as business_key,
        audit.source_table_name,
        audit.inconsistency_id,
        audit.created_at as detection_date,
        ityp.inconsistency_type,
        ityp.inconsistency_description as description,
        'customer' as entity,
        ityp.inconsistencies_category as source,
        'PENDING' as status,
        TO_JSON(STRUCT(
            cc.customer_dw_id as customer_dw_id,
            cc.drivers_license_number,
            cc.created_at,
            audit.source_table_name as table_name,
            audit.inconsistency_id as error_code,
            ityp.inconsistency_description as error_message,
            current_timestamp() as processed_timestamp
        )) as technical_details,
        NULL as batch_id
    FROM {{ ref('audit_inconsistencies') }} as audit
    INNER JOIN compliance_{{ var('DEPLOYMENT_ENVIRONMENT') }}.compliance.type_inconsistencies as ityp 
        ON audit.inconsistency_id = ityp.inconsistency_id
    INNER JOIN state_reporting_{{ var('DEPLOYMENT_ENVIRONMENT') }}.silver.customer_cleaned as cc
        ON audit.source_table_id = cc.customer_dw_id
    WHERE source_table_name = 'customer_cleaned'
    {% if is_incremental() %}
        AND audit.created_at > (SELECT COALESCE(max(detection_date), '1970-01-01') FROM {{ this }})
    {% endif %}
),

events_inconsistencies AS (
    -- Inconsistencias en customer_events_cleaned
    SELECT
        audit.audit_inconsistent_dw_id,
        audit.source_table_id as business_key,
        audit.source_table_name,
        audit.inconsistency_id,
        audit.created_at as detection_date,
        ityp.inconsistency_type,
        ityp.inconsistency_description as description,
        'events' as entity,
        ityp.inconsistencies_category as source,
        'PENDING' as status,
        TO_JSON(STRUCT(
            cec.event_dw_id as event_dw_id,
            cec.created_at,
            audit.source_table_name as table_name,
            audit.inconsistency_id as error_code,
            ityp.inconsistency_description as error_message,
            current_timestamp() as processed_timestamp
        )) as technical_details,
        NULL as batch_id
    FROM {{ ref('audit_inconsistencies') }} as audit
    INNER JOIN compliance_{{ var('DEPLOYMENT_ENVIRONMENT') }}.compliance.type_inconsistencies as ityp 
        ON audit.inconsistency_id = ityp.inconsistency_id
    INNER JOIN state_reporting_{{ var('DEPLOYMENT_ENVIRONMENT') }}.silver.customer_events_cleaned as cec
        ON audit.source_table_id = cec.event_dw_id
    WHERE source_table_name = 'customer_events_cleaned'
    {% if is_incremental() %}
        AND audit.created_at > (SELECT COALESCE(max(detection_date), '1970-01-01') FROM {{ this }})
    {% endif %}
),

state_reported_inconsistencies AS (
    -- Inconsistencias en customer_state_reported
    SELECT
        audit.audit_inconsistent_dw_id,
        audit.source_table_id as business_key,
        audit.source_table_name,
        audit.inconsistency_id,
        audit.created_at as detection_date,
        ityp.inconsistency_type,
        ityp.inconsistency_description as description,
        'events' as entity,
        'WS_REJECTION' as source, -- Específico para fallos de reportes estatales
        'PENDING' as status,
        TO_JSON(STRUCT(
            csr.customer_state_dw_id as customer_state_dw_id,
            csr.status,
            csr.action_required,
            audit.source_table_name as table_name,
            audit.inconsistency_id as error_code,
            ityp.inconsistency_description as error_message,
            current_timestamp() as processed_timestamp
        )) as technical_details,
        NULL as batch_id
    FROM {{ ref('audit_inconsistencies') }} as audit
    INNER JOIN compliance_{{ var('DEPLOYMENT_ENVIRONMENT') }}.compliance.type_inconsistencies as ityp 
        ON audit.inconsistency_id = ityp.inconsistency_id
    INNER JOIN state_reporting_{{ var('DEPLOYMENT_ENVIRONMENT') }}.gold.customer_state_reported as csr
        ON audit.source_table_id = csr.customer_state_dw_id
    WHERE source_table_name = 'customer_state_reported'
    {% if is_incremental() %}
        AND audit.created_at > (SELECT COALESCE(max(detection_date), '1970-01-01') FROM {{ this }})
    {% endif %}
),

combined_data AS (
    SELECT * FROM batch_customer_inconsistencies
    UNION ALL
    SELECT * FROM customer_inconsistencies
    UNION ALL
    SELECT * FROM events_inconsistencies
    UNION ALL
    SELECT * FROM state_reported_inconsistencies
)

-- Construir tabla final
SELECT
    {{ dbt_utils.generate_surrogate_key(['cd.audit_inconsistent_dw_id']) }} as id,
    cd.business_key,
    cd.source_table_name,
    cd.inconsistency_id,
    cd.inconsistency_type,
    cd.description,
    cd.detection_date,
    cd.entity,
    cd.status,
    1 as recurrence, -- Inicialmente, la recurrencia es 1. Se actualizará con el tiempo.
    cd.source,
    cd.technical_details,
    cd.batch_id
FROM combined_data cd

{% if is_incremental() %}
    -- Si la tabla historical_inconsistencies existe, excluir las que ya están resueltas
    {% if adapter.get_relation(this.database, 'historical_inconsistencies') is not none %}
        WHERE NOT EXISTS (
            SELECT 1 FROM {{ ref('historical_inconsistencies') }} hist
            WHERE hist.business_key = cd.business_key
            AND hist.inconsistency_id = cd.inconsistency_id
        )
    {% endif %}
{% endif %}
{{config(
    materialized='view',
        post_hook=[
        "alter view {{this}} set tblproperties ('view.schemamode' = 'TYPE EVOLUTION');"
    ]
)}}

SELECT
    csr.customer_state_dw_id,
    psi.error_code,
    psi.message,
    psi.submitted_at,
    c.drivers_license_number AS driversLicenseNumber,
    c.last_name AS lastName,
    c.first_name AS firstName,
    c.middle_name AS middleName,
    CAST(c.date_of_birth AS TIMESTAMP) AS dateOfBirth,
    c.vin AS VIN,
    me.new_vin AS newVIN, 
    CAST(c.install_date AS TIMESTAMP) AS ignitionInterlockDeviceInstalledDate,
    CAST(c.deinstall_date AS TIMESTAMP) AS ignitionInterlockDeviceRemovedDate,
    CAST(dd.datetime_full AS TIMESTAMP) AS violationDate,
    rt.id AS recordType
FROM {{ref('customer_state_reported')}} AS csr
INNER JOIN {{ref('marked_events')}} AS me
    ON csr.record_dw_id = me.record_dw_id
INNER JOIN {{ref('customer')}} AS c
    ON csr.customer_dw_id = c.customer_dw_id
INNER JOIN {{ref('dim_date_time')}} dd
    ON csr.datetime_id = dd.datetime_id
INNER JOIN {{ref('batch_customer')}} bc 
    ON csr.batch_customer_dw_id = bc.batch_customer_dw_id
INNER JOIN {{ref('record_type')}} rt
    ON csr.record_type_dw_id = rt.record_type_dw_id
INNER JOIN {{source('GOLD', 'processed_submissions_ia')}} psi
    ON csr.customer_state_dw_id = psi.customer_state_dw_id;

     

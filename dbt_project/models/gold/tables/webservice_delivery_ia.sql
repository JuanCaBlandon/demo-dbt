{{config(
    materialized='view',
        post_hook=[
        "alter view {{this}} set tblproperties ('view.schemamode' = 'TYPE EVOLUTION');"
    ]
)}}

SELECT
    c.drivers_license_number AS driversLicenseNumber,
    c.last_name AS lastName,
    c.first_name AS firstName,
    c.middle_name AS middleName,
    c.date_of_birth AS dateOfBirth,
    c.vin AS VIN,
    NULL AS newVIN, --NO ESTÁ, MIRAR
    bc.iid_start_date AS ignitionInterlockDeviceInstalledDate,
    bc.iid_end_date AS ignitionInterlockDeviceRemovedDate,
    dd.datetime_full AS violationDate,
    rt.id AS recordType
FROM {{ref('customer_state_reported')}} AS csr
INNER JOIN {{ref('customer')}} AS c
    ON csr.customer_dw_id = c.customer_dw_id
INNER JOIN {{ref('dim_date_time')}} dd
    ON csr.datetime_id = dd.datetime_id
INNER JOIN {{ref('batch_customer')}} bc 
    ON csr.batch_customer_dw_id = bc.batch_customer_dw_id
INNER JOIN {{ref('record_type')}} rt
    ON csr.record_type_dw_id = rt.record_type_dw_id
     

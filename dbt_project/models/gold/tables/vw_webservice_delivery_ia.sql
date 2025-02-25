{{config(
    materialized='view',
        post_hook=[
        "alter view {{this}} set tblproperties ('view.schemamode' = 'TYPE EVOLUTION');"
    ]
)}}

SELECT
    csr.customer_state_dw_id,
    c.drivers_license_number AS driversLicenseNumber,
    c.last_name AS lastName,
    c.first_name AS firstName,
    c.middle_name AS middleName,
    date_format(c.date_of_birth , 'yyyy-MM-dd') AS dateOfBirth,
    c.vin AS VIN,
    me.new_vin AS newVIN, 
    date_format(c.install_date , 'yyyy-MM-dd') AS ignitionInterlockDeviceInstalledDate,
    date_format(c.deinstall_date , 'yyyy-MM-dd') AS ignitionInterlockDeviceRemovedDate,
    date_format(dd.datetime_full , 'yyyy-MM-dd') AS violationDate,
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
    AND bc.created_at = (SELECT MAX(created_at) FROM {{ ref('batch_customer') }})
INNER JOIN {{ref('record_type')}} rt
    ON csr.record_type_dw_id = rt.record_type_dw_id
WHERE csr.status IS NULL OR csr.status = 2;
     

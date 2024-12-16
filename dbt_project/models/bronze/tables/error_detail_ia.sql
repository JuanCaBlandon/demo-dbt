 {{ config(
		materialized = 'table',
		post_hook=["ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"]
) }}

WITH error_codes AS (
    SELECT code, error_message
    FROM
    VALUES
        (1, 'Authentication Failed: Invalid credentials'),
        (2, 'You are not authorized to use service'),
        (3, 'Drivers License Number field is blank or Null or exceeds the maximum field length of 50'),
        (4, 'Last Name is blank, null, or exceeds the maximum field length of 80'),
        (5, 'First Name is blank, null, or exceeds the maximum field length of 80'),
        (6, 'Date of Birth is blank, null, or in invalid format'),
        (7, 'VIN field is blank or Null or exceeds maximum field length of 30'),
        (8, 'IID Installed Date is blank, null, or invalid format'),
        (9, 'Record Type does not contain a valid value'),
        (10, 'No match found on entered DL Number'),
        (11, 'No match found on entered VIN'),
        (12, 'No IID match found on entered DL Number and VIN'),
        (13, 'IID information was submitted successfully'),
        (14, 'An error occurred while submitting the IID log information'),
        (15, 'New VIN field is blank or Null or exceeds maximum field length of 30'),
        (16, 'Violation Date field is blank, Null or invalid format'),
        (17, 'Authorized Uninstalls not accepted through the service at this time'),
        (18, 'IID Violations/Final Compliance are not applicable for customers with OWI-related offenses occurring prior to {0}'),
        (19, 'No match found on entered New VIN'),
        (20, 'IID Removed Date is blank, null, or in invalid format'),
        (21, 'Middle Name exceeds the maximum field length of 80'),
        (22, 'New VIN must be different from VIN'),
        (23, 'IID Violations/Final Compliance are not applicable for customers with first offenses'),
        (24, 'Final Compliance Report not accepted prior to the IID Ending Date'),
        (25, 'Removed Date is in invalid format') AS DATA (code, error_message)
)
SELECT 
    code,
    error_message,
    CURRENT_TIMESTAMP() as created_at
FROM error_codes;
CREATE TABLE IF NOT EXISTS int_sr_dev.bronze.aditional_field_customer_ia
COMMENT 'Ingested from batch file'
AS
SELECT *, current_timestamp() AS created_at
FROM read_files(
    '/Volumes/int_sr_dev/bronze/inbound_volume/ftp/',
    FORMAT => 'csv',
    HEADER => TRUE,
    DELIMITER => ',',
    MODE => 'FAILFAST'
);
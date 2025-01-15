CREATE SCHEMA IF NOT EXISTS int_sr_dev.bronze
WITH DBPROPERTIES (
    'purpose' = 'Raw data ingestion and storage',
    'environment' = 'dev',
    'layer' = 'bronze'
);
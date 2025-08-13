CREATE SCHEMA IF NOT EXISTS laboratory_dev.bronze
WITH DBPROPERTIES (
    'purpose' = 'Raw data ingestion and storage',
    'environment' = 'dev',
    'layer' = 'bronze'
);
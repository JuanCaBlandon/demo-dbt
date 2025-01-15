CREATE SCHEMA IF NOT EXISTS int_sr_dev.silver
WITH DBPROPERTIES (
    'purpose' = 'Cleansed and enriched data',
    'environment' = 'dev',
    'layer' = 'silver'
);
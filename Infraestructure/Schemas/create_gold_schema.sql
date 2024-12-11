CREATE SCHEMA IF NOT EXISTS int_sr_dev.gold
WITH DBPROPERTIES (
    'purpose' = 'Aggregated data for business intelligence',
    'environment' = 'dev',
    'layer' = 'gold'
);
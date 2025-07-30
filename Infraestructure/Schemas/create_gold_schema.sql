CREATE SCHEMA IF NOT EXISTS laboratory_dev.gold
WITH DBPROPERTIES (
    'purpose' = 'Aggregated data for business intelligence',
    'environment' = 'dev',
    'layer' = 'gold'
);
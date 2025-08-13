CREATE SCHEMA IF NOT EXISTS laboratory_dev.silver
WITH DBPROPERTIES (
    'purpose' = 'Cleansed and enriched data',
    'environment' = 'dev',
    'layer' = 'silver'
);

CREATE EXTERNAL VOLUME int_sr_dev.bronze.inbound_volume
    LOCATION 's3://databricks-test-workspace-stack-0d464-bucket/inbound_ia'
    COMMENT 'This is ia ftp external volume on S3'
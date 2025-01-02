
# Define connection parameters
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
database_host = "172.16.1.161" 
instanceName = "dev"
database_name = "StateReporting"
user = dbutils.secrets.get(scope="sql-credentials", key="username")
password = dbutils.secrets.get(scope="sql-credentials", key="password")


# Build connection URL with SSL parameters
url = (f"jdbc:sqlserver://{database_host};instanceName={instanceName};"
         f"database={database_name};"
         "encrypt=true;"
         "trustServerCertificate=true;"
         f"user={user};"
         f"password={password}") 


query = "SELECT * FROM CustomerViolations"

# Try to establish connection
try:
    df = (spark.read
        .format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("query", query)
        .option("user", user)
        .option("password", password)
        .load())
    
    df.createOrReplaceTempView("CustomerViolations")

    # Insert the results into delta table bronze layer 
    spark.sql("""
        INSERT INTO state_reporting_dev.bronze.customer_violations (
            ViolationType,CustomerID,DeviceUsageViolationID,DeviceUsageID,ViolationID,StartingDeviceLogEntryID,EndingDeviceLogEntryID,ViolationReportingApprovalCd,ViolationReportingApprovalUser,
            ViolationReportingApprovalDate,ViolationReported,RegulatoryRptgDt,Comments,CreateDate,CreateUser,ModifyDate,ModifyUser
            )
        SELECT 
            ViolationType,CustomerID,DeviceUsageViolationID,DeviceUsageID,ViolationID,StartingDeviceLogEntryID,EndingDeviceLogEntryID,ViolationReportingApprovalCd,ViolationReportingApprovalUser,
            ViolationReportingApprovalDate,ViolationReported,RegulatoryRptgDt,Comments,CreateDate,CreateUser,ModifyDate,ModifyUser
        FROM CustomerViolations
        """)

    print("ingestion successful!")

except Exception as e:
    print(f"Connection failed with error:\n{str(e)}")
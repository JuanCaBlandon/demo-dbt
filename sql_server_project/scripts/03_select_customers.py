
# Define connection parameters
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
database_host = "172.16.1.161" 
instanceName = "dev"
database_name = "StateReporting"
user = dbutils.secrets.get(scope="sql-credentials", key="username")
password = dbutils.secrets.get(scope="sql-credentials", key="password")

var = "prod" # Environment variable

# Build connection URL with SSL parameters
url = (f"jdbc:sqlserver://{database_host};instanceName={instanceName};"
         f"database={database_name};"
         "encrypt=true;"
         "trustServerCertificate=true;"
         f"user={user};"
         f"password={password}") 

if var == "prod":
    sql_where = " WHERE OffenseDate >= '2025-01-01'"
else:
    sql_where = " WHERE OffenseDate >= '2024-10-01'"

query = f"SELECT * FROM TpmStateReportedCustomer{sql_where}"

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
    
    df.createOrReplaceTempView("CustomersIA")

    # Insert the results into delta table bronze layer 
    spark.sql("""
        INSERT INTO state_reporting_dev.bronze.state_reported_customer (
            CustomerReportingStateID, CustomerID, DriversLicenseNumber, FirstName, LastName, MiddleName, DateOfBirth, VIN, InstallDate,
            DeInstallDate, StateCode, ActiveStatus, EffectiveStartDate, EffectiveEndDate, DeviceLogRptgClassCd, CreateDate, CreateUser,
            ModifyDate, ModifyUser, CreationDate, ReportStatusCd,CustomerStatus,ActiveStatusStartDate,OffenseDate,IIDStartDate,IIDEndDate,RepeatOffender
        ) 
        SELECT 
            CustomerReportingStateID, CustomerID, DriversLicenseNumber, FirstName, LastName, MiddleName, try_cast(DateOfBirth AS TIMESTAMP), VIN, try_cast(InstallDate AS TIMESTAMP),
            try_cast(DeInstallDate AS TIMESTAMP), StateCode, ActiveStatus, try_cast(EffectiveStartDate AS TIMESTAMP), try_cast(EffectiveEndDate AS TIMESTAMP), DeviceLogRptgClassCd, try_cast(CreateDate AS TIMESTAMP), CreateUser,
            try_cast(ModifyDate AS TIMESTAMP), ModifyUser, try_cast(CreationDate AS TIMESTAMP), ReportStatusCd,CustomerStatus,ActiveStatusStartDate,OffenseDate,IIDStartDate,IIDEndDate,RepeatOffender
        FROM CustomersIA
        """)

    spark.sql(""" 
        MERGE INTO state_reporting_dev.bronze.state_reported_customer AS ST
        USING CustomersIA AS CU
        ON ST.CustomerID = CU.CustomerID
            AND CU.ActiveStatus = 0
        WHEN MATCHED THEN
        UPDATE SET ST.ActiveStatusEndDate = current_date()
        """)

    print("ingestion successful!")

except Exception as e:
    print(f"Connection failed with error:\n{str(e)}")
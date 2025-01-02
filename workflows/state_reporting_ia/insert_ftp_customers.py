
from pyspark.sql.functions import col

# SQL Server Connection Parameters
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
database_host = "172.16.1.161"
instanceName = "dev"
database_name = "StateReporting"
user = dbutils.secrets.get(scope="state_reporting", key="sql_server_user")
password = dbutils.secrets.get(scope="state_reporting", key="sql_server_pass")


# Build connection URL with SSL parameters
url = (f"jdbc:sqlserver://{database_host};instanceName={instanceName};"
         f"database={database_name};"
         "encrypt=true;"
         "trustServerCertificate=true;"
         f"user={user};"
         f"password={password}")  

# Table details
table_name = "dbo.FTPCustomerData"


try:
    result_df = spark.read.table("state_reporting_dev.bronze.state_batch_customer_data_ia").where("created_at=CAST(CURRENT_DATE() AS DATE)")

    result_df = result_df.select(col("vendor_name").alias("VendorName"), col("DriversLicenseNumber"),col("LastName"), col("FirstName"), col("MiddleName"), \
        col("DateOfBirth").cast("timestamp").alias("DateOfBirth"),col("VIN"), col("offense_date").alias("OffenseDate"), col("repeat_offender").alias("RepeatOffender"),\
        col("IID_Start_Date").cast("timestamp").alias("IIDStartDate"), col("IID_End_Date").cast("timestamp").alias("IIDEndDate"), col("created_at").cast("date").alias("CreationDate"))
    
    # Write DataFrame to SQL Server
    result_df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .mode("append") \
        .save()
    
    print(f"Successfully inserted {result_df.count()} rows into {table_name}")

except Exception as e:
    print(f"Error inserting DataFrame: {str(e)}")


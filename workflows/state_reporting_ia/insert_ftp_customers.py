
from pyspark.sql.functions import col, to_date
from args_parser import get_parser

# Get the parser
parser = get_parser()
args = parser.parse_args()

# Access parameters
execution_date = args.execution_date
print(execution_date)


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
table_name = "databricks.FTPCustomerData"


try:
    result_df = spark.read.table("state_reporting_dev.bronze.state_batch_customer_data_ia").where(f"CAST(created_at AS DATE) = '{execution_date}'")

    batch_data = result_df.select(
        col("vendor_name").alias("VendorName"),
        col("DriversLicenseNumber"),
        col("LastName"),
        col("FirstName"),
        col("MiddleName"),
        col("DateOfBirth").cast("timestamp").alias("DateOfBirth"),
        col("VIN"),
        col("offense_date").alias("OffenseDate"),
        col("repeat_offender").alias("RepeatOffender"),
        to_date(col("IID_Start_Date"), "M/d/yy").alias("IIDStartDate"),
        to_date(col("IID_End_Date"), "M/d/yy").alias("IIDEndDate"),
        col("created_at").cast("date").alias("CreationDate")
    )
    
    if batch_data.count() == 0:
        raise ValueError("No rows to insert")

    # Write DataFrame to SQL Server
    batch_data.write \
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
    raise
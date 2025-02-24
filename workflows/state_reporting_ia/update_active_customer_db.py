from pyspark.sql.functions import col, lit, to_date
from args_parser import get_parser

# Get the parser
parser = get_parser()
args = parser.parse_args()

# Access parameters
env = args.environment

# SQL Server Connection Parameters
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
instanceName = env
database_name = "StateReporting"
database_host = dbutils.secrets.get(scope="state_reporting", key=f"sql_server_host_{env}")
username = dbutils.secrets.get(scope="state_reporting", key=f"sql_server_user_{env}")
password = dbutils.secrets.get(scope="state_reporting", key=f"sql_server_pass_{env}")

# Build connection URL with SSL parameters
url = (f"jdbc:sqlserver://{database_host};instanceName={instanceName};"
       f"database={database_name};"
       "encrypt=true;"
       "trustServerCertificate=true;"
       f"user={username};"
       f"password={password}")

# Table details
table_name = "databricks.StateReportedCustomer"

try:
    result_df = spark.read.table(f"state_reporting_{env}.gold.customer")
    active_customers = result_df.select(
        col("customer_reporting_state_id").alias("CustomerReportingStateID"),
        col("customer_id").alias("CustomerID"),
        col("state_code").substr(0,2).alias("StateCode"),  # Ensure max length = 2
        col("active_status").cast("int").alias("ActiveStatus"),
        col("customer_status").cast("int").alias("CustomerStatus"),
        to_date(col("active_status_start_date")).alias("ActiveStatusStartDate"),
        to_date(col("active_status_end_date")).alias("ActiveStatusEndDate "),
        col("report_status_cd").substr(0,30).alias("ReportStatusCD"),  # Ensure max length = 30
        lit(None).cast("date").alias("FirstReportDate"),
        lit(None).cast("date").alias("StopReportDate"),
        to_date(col("install_date")).alias("InstallDate"),
        to_date(col("deinstall_date")).alias("DeInstallDate"),
        to_date(col("create_date")).alias("CreateDate"),
        col("create_user").substr(0,50).alias("CreateUser"),  # Ensure max length = 50
        to_date(col("modify_date")).alias("ModifyDate"),
        col("modify_user").substr(0,50).alias("ModifyUser"),  # Ensure max length = 50
        col("repeat_offender").substr(0,1).alias("RepeatOffender"),  # Ensure max length = 1
        to_date(col("offense_date")).alias("OffenseDate"),
        to_date(col("iid_start_date")).alias("IIDStartDate"),
        to_date(col("iid_end_date")).alias("IIDEndDate"),
        to_date(col("created_at")).alias("CreationDate")
    )

    active_customers = active_customers.fillna({"ActiveStatus": 0, "CustomerStatus": 0})

    if result_df.count() == 0:
        raise ValueError("No rows to insert")

    # Truncate table before inserting
    truncate_query = f"DELETE FROM {table_name};"

    spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("query", truncate_query) \
        .option("driver", driver) \
        .option("user", username) \
        .option("password", password) \
        .load()

    # Insert new data
    active_customers.write \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password) \
        .mode("append") \
        .save()

    print(f"Successfully replaced data in {table_name}")

except Exception as e:
    print(f"Error processing DataFrame: {str(e)}")
    raise
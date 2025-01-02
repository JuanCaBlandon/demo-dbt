
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
database_host = "172.16.1.161\dev"  # Note the escaped backslash
database_port = "1433"
database_name = "statereporting"
table = "TpmStateReportedCustomer"
username = dbutils.secrets.get(scope="state_reporting", key="sql_server_user")
password = dbutils.secrets.get(scope="state_reporting", key="sql_server_pass")

url = f"jdbc:sqlserver://{database_host};instanceName=dev;databaseName={database_name};encrypt=true;trustServerCertificate=true"

query = """
SELECT * FROM CustomerViolations
"""

result_df = (spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("query", query)
    .option("user", username)
    .option("password", password)
    .load())

result_df = result_df.withColumn("ViolationReported", result_df["ViolationReported"].cast("int"))

result_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("state_reporting_dev.bronze.customer_violations")

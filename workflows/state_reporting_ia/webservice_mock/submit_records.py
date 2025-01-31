from pyspark.sql import SparkSession
import pandas as pd
from test_runner_2 import process_multiple_records

# Initialize Spark session
spark = SparkSession.builder.appName("Databricks Webservice Integration").getOrCreate()

# Define the source table
source_table = "state_reporting_dev.gold.vw_webservice_delivery_ia"
df = spark.read.table(source_table)
df = df.drop('customer_state_dw_id')
records_pd = df.toPandas()


successful_submissions, failed_submissions = process_multiple_records(records_pd.to_dict(orient="records"))


if successful_submissions:
    spark_df = spark.createDataFrame(pd.DataFrame(successful_submissions))
    spark_df.write.mode("append").saveAsTable("state_reporting_dev.gold.processed_submissions")
    print(f"✅ Successfully saved {len(successful_submissions)} valid records to Databricks!")


if failed_submissions:
    spark_df_failures = spark.createDataFrame(pd.DataFrame(failed_submissions))
    spark_df_failures.write.mode("append").saveAsTable("state_reporting_dev.gold.failed_submissions")
    print(f"⚠️ Saved {len(failed_submissions)} failed records to `failed_submissions` table.")

else:
    print("⚠️ No failed records found.")

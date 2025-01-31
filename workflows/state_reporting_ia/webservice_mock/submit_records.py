from pyspark.sql import SparkSession
import pandas as pd
from test_runner_2 import process_record

spark = SparkSession.builder.appName("Databricks Webservice Integration").getOrCreate()

source_table = "state_reporting_dev.gold.vw_webservice_delivery_ia"
df = spark.read.table(source_table)
df = df.drop('customer_state_dw_id')
records_pd = df.toPandas()

submissions = []  # Track previous submissions dynamically

for index, record in records_pd.iterrows():
    customer_state_dw_id = record['customer_state_dw_id']
    record.drop('customer_state_dw_id', inplace=True)
    response_json = process_record(record.to_dict(), customer_state_dw_id, submissions, )
    if response_json:
        submissions.append(response_json)  # Store in-memory instead of JSON file

print(submissions)
# Convert to Spark DataFrame and save
# spark_df = spark.createDataFrame(pd.DataFrame(submissions))
# spark_df.write.mode("append").saveAsTable("state_reporting_dev.gold.processed_submissions")

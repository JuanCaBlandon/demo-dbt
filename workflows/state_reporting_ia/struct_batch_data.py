import logging
from args_parser import get_parser
from pyspark.sql.functions import col, substring, regexp_replace, to_timestamp, to_date, lit, when


# Get the parser
parser = get_parser()
args = parser.parse_args()

# Access parameters
env = args.environment
execution_date = args.execution_date

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source_path = f"/Volumes/ia_batch_files_raw_{env}/default/ia_batch_files_raw_{env}/extracted_files/Intoxalock_{execution_date}.txt"
destination_path = f"state_reporting_{env}.bronze.state_batch_customer_data_ia_test"

try:
    df = spark.read.text(source_path)
    if df.count() == 0:
        raise ValueError("Empty file detected")
except Exception as e:
    logger.error(f"Error reading file: {str(e)}")
    raise

batch_file = df.select(
    regexp_replace(substring(col("value"), 1, 30), r"\s{3,}", "").alias("VendorName"),
    regexp_replace(substring(col("value"), 31, 50), r"\s{3,}", "").alias("DriversLicenseNumber"),
    regexp_replace(substring(col("value"), 81, 80), r"\s{3,}", "").alias("LastName"),
    regexp_replace(substring(col("value"), 161, 80), r"\s{3,}", "").alias("FirstName"),
    regexp_replace(substring(col("value"), 241, 80), r"\s{3,}", "").alias("MiddleName"),
    regexp_replace(substring(col("value"), 321, 10), r"\s{3,}", "").alias("DateOfBirth"),
    regexp_replace(substring(col("value"), 331, 30), r"\s{3,}", "").alias("VIN"),
    regexp_replace(substring(col("value"), 361, 10), r"\s{3,}", "").alias("OffenseDate"),
    regexp_replace(substring(col("value"), 371, 1), r"\s{3,}", "").alias("RepeatOffender"),
    regexp_replace(substring(col("value"), 372, 10), r"\s{3,}", "").alias("IIDStartDate"),
    regexp_replace(substring(col("value"), 382, 10), r"\s{3,}", "").alias("IIDEndDate")
)
batch_file = batch_file.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in batch_file.columns])

date_columns = ["DateOfBirth", "OffenseDate", "IIDStartDate", "IIDEndDate"]
batch_file = batch_file.select(
    *[to_date(col(c), "yyyy-MM-dd").alias(c) if c in date_columns else col(c) for c in batch_file.columns]
)

#Created At
batch_file = batch_file.withColumn("CreatedAt", to_timestamp(lit(execution_date)))

# Ensure the schema of the DataFrame matches the schema of the Delta table
existing_table_schema = spark.table("state_reporting_dev.bronze.state_batch_customer_data_ia_test").schema

batch_file = batch_file.select([col(field.name).cast(field.dataType) for field in existing_table_schema])

# Write to delta table

try:
    row_count = batch_file.count()
    batch_file.write.format("delta").mode("append").saveAsTable(destination_path)
    logger.info(f"Successfully added {row_count} rows to {destination_path}")
except Exception as e:
    logger.error(f"Error writing to Delta table: {str(e)}")
    raise

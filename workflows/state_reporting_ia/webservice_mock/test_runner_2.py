from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import pandas as pd
import json
from datetime import datetime
from iid_service_mock_2 import IIDService, IgnitionInterlockDeviceServiceLog, ErrorCodes
from typing import List

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle datetime objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def process_record(record: dict, record_id: str, previous_submissions: List[dict]):
    """Processes a single record dynamically without file dependencies."""
    service = IIDService()

    # Prepare the base submission JSON
    submission_json = {
        "record_id": record_id,
        "submission_date": datetime.now().isoformat(),
        "service_response": []
    }

    try:
        log = IgnitionInterlockDeviceServiceLog(**record)
        result = service.SubmitIgnitionInterlockDevice(log, previous_submissions)

        print("\nService Response:")
        print("-----------------")
        for rv in result:
            print(f"Error Code: {rv.ErrorCode}")
            print(f"Message: {rv.Message}")

        submission_json["service_response"] = [{"ErrorCode": rv.ErrorCode, "Message": rv.Message} for rv in result]

    except ValueError as e:
        print(f"\nValidation Error: {str(e)}")
        submission_json["service_response"] = [{
            "ErrorCode": ErrorCodes.VALIDATION_ERROR,
            "Message": str(e)
        }]
    except Exception as e:
        print(f"\nUnexpected Error: {str(e)}")
        submission_json["service_response"] = [{
            "ErrorCode": ErrorCodes.VALIDATION_ERROR,
            "Message": str(e)
        }]

    return submission_json

def save_responses(spark, submissions: list):  # Pass spark explicitly
    print('Starting to save')
    rows = []

    schema = StructType([
        StructField("record_id", StringType(), False),
        StructField("error_code", IntegerType(), False),  # Ensure it's an integer
        StructField("error_message", StringType(), False),
        StructField("submission_date", TimestampType(), False)
    ])

    for record in submissions:
        record_id = record['record_id']
        submission_date = record['submission_date']
        responses = record['service_response']
        print('Adding record', record_id)
        for response in responses:
            rows.append(
                Row(
                    record_id=record_id,
                    error_code=int(response['ErrorCode']),
                    error_message=response['Message'],
                    submission_date=submission_date
                )
            )
    
    print(json.dumps(rows, indent=2, cls=DateTimeEncoder))
    processed_submissions = spark.createDataFrame(rows, schema=schema)
    processed_submissions.write.format("delta").mode("append").saveAsTable("state_reporting_dev.gold.proccessed_sumbissions_ia")

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Databricks Webservice Integration") \
        .getOrCreate()

    try:
        # Read from source table
        source_table = "state_reporting_dev.gold.vw_webservice_delivery_ia"
        print(f"\nReading from source table: {source_table}")
        df = spark.read.table(source_table)
        
        # Convert to pandas for easier processing
        print("Converting to pandas dataframe...")
        records_pd = df.toPandas()
        
        # Track submissions in memory
        submissions = []
        
        # Process each record
        total_records = len(records_pd)
        print(f"\nProcessing {total_records} records...")
        
        for index, record in records_pd.iterrows():
            print(f"\nProcessing record {index + 1} of {total_records}")
            
            # Extract and remove customer_state_dw_id
            customer_state_dw_id = record['customer_state_dw_id']
            record = record.drop('customer_state_dw_id')
            
            # Convert record to dict and process
            response_json = process_record(record.to_dict(), customer_state_dw_id, submissions)
            submissions.append(response_json)
        
        print(json.dumps(submissions, indent=2, cls=DateTimeEncoder))
        save_responses(spark, submissions)
    except Exception as e:
        print(f"\nError in main processing: {str(e)}")
        raise


if __name__ == "__main__":
    main()
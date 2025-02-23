import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Initialize Spark session
def model(dbt, session):
    dbt.config(materialized="incremental",submission_method="all_purpose_cluster",cluster_id="0204-173204-ojxrab09")
    start_date = dbt.config.get("start_date", "2025-01-01")
    execution_date = dbt.config.get("execution_date")

    customer_events_cleaned = dbt.ref("customer_events_cleaned")
    customer_cleaned = dbt.ref("customer_cleaned")
    batch_customer_cleaned = dbt.ref("batch_customer_cleaned")

    
    customer_events_cleaned.createOrReplaceTempView("customer_events_cleaned")
    customer_cleaned.createOrReplaceTempView("customer_cleaned")
    batch_customer_cleaned.createOrReplaceTempView("batch_customer_cleaned")

    # Handle incremental logic
    if dbt.is_incremental:
        # Fetch previous events for incremental processing
        previous_events_df = session.sql(f"""
            SELECT
                drivers_license_number, 
                CAST(MAX(event_date) AS TIMESTAMP) AS event_date
            FROM {dbt.this}
            WHERE record_type = 2
            GROUP BY drivers_license_number
        """).toPandas()
 

    else:
        # Initialize an empty DataFrame on first run
        previous_events_df = pd.DataFrame(columns=["drivers_license_number", "event_date"])

    base_df = session.sql(f"""
        WITH base_data AS (
            SELECT
                cec.event_dw_id,
                cec.customer_id,
                cc.drivers_license_number,
                cec.device_usage_violation_id,
                cec.device_usage_event_violation_id,
                cec.customer_transaction_id,
                cec.event_type,
                CAST(cec.event_date AS TIMESTAMP) AS event_date
            FROM customer_events_cleaned cec
            INNER JOIN customer_cleaned cc 
                ON cc.customer_id = cec.customer_id
                AND cc.is_inconsistent = 0
            INNER JOIN batch_customer_cleaned AS bcc
                ON bcc.drivers_license_number = cc.drivers_license_number
                AND RIGHT(bcc.vin,6) = RIGHT(cc.vin,6)
                AND bcc.created_at = '{execution_date}'
                AND bcc.is_inconsistent = 0
                AND bcc.repeat_offender = 1
                AND bcc.offense_date >= '{start_date}'
            WHERE cec.is_inconsistent = 0
            AND cec.event_type = 'TYPE 1-2'
        )
        SELECT
            event_dw_id,
            customer_id,
            drivers_license_number,
            device_usage_violation_id,
            CAST(LAG(event_date, 9) OVER (
                PARTITION BY drivers_license_number
                ORDER BY event_date
            ) AS TIMESTAMP) AS event_start_date,
            CAST(event_date AS TIMESTAMP) AS event_date
        FROM base_data
        QUALIFY COUNT(*) OVER (
            PARTITION BY drivers_license_number
            ORDER BY event_date
            RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
        ) >= 10
    """)


    # Convert to Pandas for processing
    events30 = base_df.toPandas()

    result_schema = StructType([
        StructField("event_dw_id", StringType(), True),
        StructField("drivers_license_number", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("event_id_type", StringType(), True),
        StructField("event_id", IntegerType(), True),
        StructField("event_date", TimestampType(), True),
        StructField("record_type", IntegerType(), True),
        StructField("record_description", StringType(), True)
    ])

    marked_violations30 = pd.DataFrame(columns=[
        "event_dw_id", "drivers_license_number", "customer_id",
        "event_id_type", "event_id", "event_date", "record_type", "record_description"
    ])
    if not events30.empty:
        # Create a dictionary for faster lookups
        last_events_dict = dict(zip(
            previous_events_df['drivers_license_number'],
            previous_events_df['event_date'].apply(pd.Timestamp)
        ))

        for row in events30.itertuples(index=False):
            # Get the last event date, defaulting to 2025-01-01 if not found
            last_event_date = last_events_dict.get(
                row.drivers_license_number, 
                pd.Timestamp(start_date)
            )
            
            # Convert dates to pandas Timestamp objects
            current_event_start_date = pd.Timestamp(row.event_start_date)
            current_event_date = pd.Timestamp(row.event_date)

            if current_event_start_date > last_event_date:
                new_row = {
                    "event_dw_id": str(row.event_dw_id),
                    "drivers_license_number": str(row.drivers_license_number),
                    "customer_id": int(row.customer_id),
                    "event_id_type": "device_usage_violation_id",
                    "event_id": int(row.device_usage_violation_id),
                    "event_date": current_event_date,
                    "record_type": 2,
                    "record_description": "30 days"
                }
                
                marked_violations30 = pd.concat([marked_violations30, pd.DataFrame([new_row])], ignore_index=True)
                
                # Update the last event date in our dictionary
                last_events_dict[row.drivers_license_number] = current_event_date
    else:
        marked_violations30 = session.createDataFrame(marked_violations30, schema=result_schema)

    return marked_violations30
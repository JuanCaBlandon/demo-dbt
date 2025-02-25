import pandas as pd
import logging
from args_parser import get_parser
from pyspark.sql.functions import col, lit, coalesce, md5, concat_ws


# Get the parser
parser = get_parser()
args = parser.parse_args()

# Access parameters
env = args.environment
start_date = args.start_date
execution_date = args.execution_date

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

destination_path = f"state_reporting_{env}.silver.rt_1_2"


def get_previous_events():
    """Get previous events for both record types"""
    return spark.sql(f"""
        SELECT
            drivers_license_number,
            record_type,
            CAST(MAX(event_date) AS TIMESTAMP) as event_date
        FROM state_reporting_{env}.silver.rt_1_2
        WHERE record_type IN (1, 2)
        GROUP BY drivers_license_number, record_type
    """)

def get_violation_events(record_type):
    """Get violation events based on record type"""
    lookback_count = 4 if record_type == 1 else 9
    time_window = "24 HOURS" if record_type == 1 else "30 DAYS"
    min_violations = 5 if record_type == 1 else 10
    
    return spark.sql(f"""
        WITH base_data AS (
            SELECT
                cec.event_dw_id,
                cec.customer_id,
                cc.drivers_license_number,
                cec.device_usage_violation_id,
                cec.device_usage_event_violation_id,
                cec.customer_transaction_id,
                cec.event_type,
                CAST(cec.event_date AS TIMESTAMP) as event_date
            FROM state_reporting_{env}.silver.customer_events_cleaned cec
            INNER JOIN state_reporting_{env}.silver.customer_cleaned cc 
                ON cc.customer_id = cec.customer_id
                AND cc.is_inconsistent = 0
            INNER JOIN state_reporting_{env}.silver.batch_customer_cleaned AS bcc
                ON cc.drivers_license_number = bcc.drivers_license_number
                AND RIGHT(bcc.vin,6) = RIGHT(cc.vin,6)
                AND bcc.created_at = '{execution_date}'
                AND bcc.is_inconsistent = 0
                AND bcc.repeat_offender = 1
                AND bcc.offense_date >= '{start_date}'
            WHERE
                cec.is_inconsistent = 0
                AND cec.event_date >= bcc.iid_start_date
                AND cec.event_type = 'TYPE 1-2'
        )
        SELECT
            event_dw_id,
            customer_id,
            drivers_license_number,
            device_usage_violation_id,
            CAST(LAG(event_date, {lookback_count}) OVER (
                PARTITION BY drivers_license_number
                ORDER BY event_date
            ) AS TIMESTAMP) AS event_start_date,
            CAST(event_date AS TIMESTAMP) as event_date
        FROM base_data
        QUALIFY COUNT(*) OVER (
            PARTITION BY drivers_license_number
            ORDER BY event_date
            RANGE BETWEEN INTERVAL {time_window} PRECEDING AND CURRENT ROW
        ) >= {min_violations}
    """)

def process_violations(events_df, previous_events_df, record_type):
    """Process violations for a specific record type"""
    # Filter previous events for current record type
    prev_events_filtered = previous_events_df[previous_events_df['record_type'] == record_type]
    
    # Create lookup dictionary for the current record type
    last_events_dict = dict(zip(
        prev_events_filtered['drivers_license_number'],
        prev_events_filtered['event_date'].apply(pd.Timestamp)
    ))
    
    result_schema = {
        "record_dw_id": str,
        "event_dw_id": str,
        "drivers_license_number": str,
        "customer_id": int,
        "event_id_type": str,
        "event_id": int,
        "event_date": "datetime64[ns]",
        "record_type": int,
        "record_description": str
    }
    
    marked_violations = pd.DataFrame(columns=result_schema.keys())
    
    for row in events_df.itertuples(index=False):
        # Get last event date, default to 2025-01-01 if not found
        last_event_date = last_events_dict.get(
            row.drivers_license_number, 
            pd.Timestamp("2025-01-01")
        )
        
        current_event_start_date = pd.Timestamp(row.event_start_date)
        current_event_date = pd.Timestamp(row.event_date)
        
        if current_event_start_date > last_event_date:
            record_description = "24 hrs" if record_type == 1 else "30 days"
            
            new_row = {
                "record_dw_id": str(row.record_dw_id),
                "event_dw_id": str(row.event_dw_id),
                "drivers_license_number": str(row.drivers_license_number),
                "customer_id": int(row.customer_id),
                "event_id_type": "device_usage_violation_id",
                "event_id": int(row.device_usage_violation_id),
                "event_date": current_event_date,
                "record_type": record_type,
                "record_description": record_description
            }
            
            marked_violations = pd.concat([marked_violations, pd.DataFrame([new_row])], ignore_index=True)
            last_events_dict[row.drivers_license_number] = current_event_date
    
    return marked_violations

def save_violations(violations_df):
    print("saving")
    existing_table_schema = spark.table(f"state_reporting_{env}.silver.rt_1_2").schema
    final_df = violations_df.select([col(field.name).cast(field.dataType) for field in existing_table_schema])
    display(final_df)
    try:
        row_count = final_df.count()
        final_df.write.format("delta").mode("append").saveAsTable(destination_path)
        logger.info(f"Successfully added {row_count} rows to {destination_path}")
    except Exception as e:
        logger.error(f"Error writing to Delta table: {str(e)}")
        raise

def main():
    # Get previous events for both record types
    previous_events = get_previous_events()
    previous_events_df = previous_events.toPandas()
    
    # Process both types of violations
    all_violations = []
    
    for record_type in [1, 2]:  # Process both record types
        # Get events for current record type
        base_df = get_violation_events(record_type)
        base_df = base_df.withColumn(
            "record_dw_id",
            md5(concat_ws("_", col("event_dw_id"), lit(str(record_type)))).cast("string")
        )
        
        # Convert to pandas and process
        events_df = base_df.toPandas()
        
        if not events_df.empty:
            violations = process_violations(events_df, previous_events_df, record_type)
            if not violations.empty:
                all_violations.append(violations)
    
    # Combine all violations
    if all_violations:
        final_violations = pd.concat(all_violations, ignore_index=True)
        final_violations_df = spark.createDataFrame(final_violations)
        save_violations(final_violations_df)
    else:
        print("No violations found")

if __name__ == "__main__":
    main()
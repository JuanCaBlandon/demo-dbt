from args_parser import get_parser
from pyspark.sql.functions import col
import json

def updateResultsTable(results, env):
    """
    Updates the results table in Databricks with the submission responses.
    Ensures that the data types match the table schema.
    """
    table_name = f"state_reporting_{env}.gold.processed_submissions_ia"

    df_results = spark.createDataFrame(results)

    df_results = df_results.withColumn("error_code", col("error_code").cast("int"))
    df_results = df_results.withColumn("submitted_at", col("submitted_at").cast("timestamp"))


    try:
        row_count = df_results.count()
        df_results.write.format("delta").mode("append").saveAsTable(table_name)
        print(f"Successfully added {row_count} rows to {table_name}")
    except Exception as e:
        print(f"Error writing to Delta table {table_name}: {str(e)}")
        raise

def updateCustomerStateReported(results, env):
    """
    Updates the customer_state_reported table in Databricks based on submission responses.
    If submission response error code = 13, then status = 1; otherwise, status = 0.
    """
    submision_entity = 'Databricks Workflow'
    status_dict = {}

    for result in results:
        customer_id = result['customer_state_dw_id']
        error_code = result['error_code']
        submitted_at = result['submitted_at']
        
        status = 1 if error_code == 13 else 0
        action_required = 0 if status == 1 else 1

        if customer_id not in status_dict or status_dict[customer_id]['status'] < status:
            status_dict[customer_id] = {
                'status': status,
                'action_required': action_required,
                'submitted_at': submitted_at
            }

    df_status = spark.createDataFrame(
        [(key, value['status'], value['action_required'], value['submitted_at'], submision_entity) 
         for key, value in status_dict.items()],
        ['customer_state_dw_id', 'status', 'action_required', 'submitted_at', 'submitted_by']
    )

    try:
        df_status.createOrReplaceTempView("temp_status")
        spark.sql(f"""
            MERGE INTO state_reporting_{env}.gold.customer_state_reported as target
            USING temp_status as source
            ON target.customer_state_dw_id = source.customer_state_dw_id
            WHEN MATCHED THEN
              UPDATE SET 
                target.status = source.status,
                target.submitted_at = source.submitted_at,
                target.submitted_by = source.submitted_by,
                target.action_required = source.action_required
        """)
        print(f"Successfully updated customer_state_reported table with {len(status_dict)} entries.")
    except Exception as e:
        print(f"Error updating customer_state_reported table: {str(e)}")
        raise

def setInactiveCustomers(id_list, execution_date, env):
    """
    Sets customers as inactive based on tracked records.
    
    Args:
        tracked_records (list): List of dictionaries containing tracked submission data
        execution_date (datetime): The date when the execution occurred
        env (str): Environment identifier for the database
    """
    active_status_end_type = 'Record type 4 or 5 submitted'
    inactive_customers_df = spark.createDataFrame(
        [(id, 0, 'Inactive', execution_date, active_status_end_type) for id in id_list],
        ['customer_state_dw_id', 'active_status', 'report_status_cd', 'active_status_end_date', 'active_status_end_type']
    )

    try:
        inactive_customers_df.createOrReplaceTempView("inactive_customers_temp")

        spark.sql(f"""
            MERGE INTO state_reporting_{env}.gold.customer AS target
            USING (
                SELECT 
                    src.customer_dw_id,
                    temp.active_status,
                    temp.report_status_cd,
                    temp.active_status_end_date,
                    temp.active_status_end_type
                FROM inactive_customers_temp temp
                JOIN state_reporting_{env}.gold.customer_state_reported src
                    ON temp.customer_state_dw_id = src.customer_state_dw_id
            ) AS source
            ON target.customer_dw_id = source.customer_dw_id
            WHEN MATCHED THEN
                UPDATE SET
                    target.active_status = source.active_status,
                    target.report_status_cd = source.report_status_cd,
                    target.active_status_end_date = source.active_status_end_date,
                    target.active_status_end_type = source.active_status_end_type'
        """)
        print(f"Successfully marked {len(id_list)} customers as inactive")
    except Exception as e:
        print(f"Error updating customer table: {str(e)}")
        raise
    finally:
        # Clean up the temporary view
        spark.sql("DROP VIEW IF EXISTS inactive_customers_temp")


def updateActionRequired(env):
    """
    Updates action_required = 0 for all records that are not the latest
    submitted_at entry per record_dw_id in customer_state_reported.
    """
    query = f"""
        WITH ranked_records AS (
            SELECT 
                record_dw_id, 
                submitted_at,
                ROW_NUMBER() OVER (PARTITION BY record_dw_id ORDER BY submitted_at DESC) AS rn
            FROM state_reporting_{env}.gold.customer_state_reported
            WHERE submitted_at IS NOT NULL
        )

        MERGE INTO state_reporting_{env}.gold.customer_state_reported AS target
        USING ranked_records AS source
        ON target.record_dw_id = source.record_dw_id
        AND target.submitted_at = source.submitted_at
        WHEN MATCHED AND source.rn > 1 THEN
        UPDATE SET action_required = 0;
    """

    try:
        spark.sql(query)
        print(f"Successfully updated action_required column in state_reporting_{env}.gold.customer_state_reported.")
    except Exception as e:
        print(f"Error updating action_required column: {str(e)}")
        raise


def main():
    print("Starting process")
    # Get the parser
    parser = get_parser()
    args = parser.parse_args()

    # Access parameters
    env = args.environment
    execution_date = args.execution_date
    submission_results = json.loads(dbutils.jobs.taskValues.get(taskKey="webservice_delivery", key="submission_results", default="[]"))
    tracked_records = json.loads(dbutils.jobs.taskValues.get(taskKey="webservice_delivery", key="tracked_records", default="[]"))


    if submission_results:
        print(f"submission_results {submission_results}")
        # Store results in DB
        updateResultsTable(submission_results, env)

        # Update customer_state_reported table
        updateCustomerStateReported(submission_results, env)
            # Set inactive clients

        if tracked_records:
            print(f"tracked results {tracked_records}")
            setInactiveCustomers(tracked_records, execution_date, env)
        else:
            print("No RT 4 and 5 successfully reported")

        # Update action_required field
        updateActionRequired(env)
    else:
        print("No Web Service Responses provided")


if __name__ == "__main__":
    main()

from args_parser import get_parser

# Get the parser
parser = get_parser()
args = parser.parse_args()

# Access parameters
env = args.environment
start_date = args.start_date
end_date = args.end_date

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

database_host = dbutils.secrets.get(scope = "state_reporting", key = f"sql_server_host_{env}")
database_port = dbutils.secrets.get(scope = "state_reporting", key = f"sql_server_port_{env}")
username = dbutils.secrets.get(scope="state_reporting", key=f"sql_server_user_{env}")
password = dbutils.secrets.get(scope="state_reporting", key=f"sql_server_pass_{env}")
database_name = "statereporting"

url = f"jdbc:sqlserver://{database_host}:{database_port};instanceName=dev;databaseName={database_name};encrypt=true;trustServerCertificate=true"

query = f"""
SELECT * FROM databricks.CustomerEvents WHERE EventDate BETWEEN '{start_date}' AND '{end_date}'
"""

result_df = (spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("query", query)
    .option("user", username)
    .option("password", password)
    .load())

print(f"DF count {result_df.count()}-2")

result_df.createOrReplaceTempView("CustomerEvents")

spark.sql(""" 
    MERGE INTO state_reporting_dev.bronze.customer_events AS CED
    USING CustomerEvents AS CESQL
    ON COALESCE(CED.DeviceUsageViolationID,CED.DeviceUsageEventViolationID,CED.CustomerTransactionID) = COALESCE(CESQL.DeviceUsageViolationID,CESQL.DeviceUsageEventViolationID,CESQL.CustomerTransactionID)
    WHEN MATCHED AND CESQL.ModificationDate = current_date() THEN
        UPDATE SET
          	CED.CustomerID = CESQL.CustomerID,
            CED.DeviceUsageID = CESQL.DeviceUsageID,
            CED.ViolationReportingApprovalCd = CESQL.ViolationReportingApprovalCd,
            CED.ViolationReportingApprovalUser = CESQL.ViolationReportingApprovalUser,
            CED.CreateDate = CESQL.CreateDate,
            CED.CreateUser = CESQL.CreateUser,
            CED.ModifyDate = CESQL.ModifyDate,
            CED.ModifyUser = CESQL.ModifyUser,
            CED.LogEntryTime = CESQL.LogEntryTime,
            CED.Eventdate = CESQL.Eventdate,
            CED.VIN = CESQL.VIN,
            CED.ModificationDate = CESQL.ModificationDate
    WHEN NOT MATCHED THEN
        INSERT (
            EventType,
            CustomerID,
            DeviceUsageViolationID,
            DeviceUsageEventViolationID,
            CustomerTransactionID,
            DeviceUsageID,
            ViolationReportingApprovalCd,
            ViolationReportingApprovalUser,
            CreateDate,
            CreateUser,
            ModifyDate,
            ModifyUser,
            LogEntryTime,
            Eventdate,
            VIN,
            NewVIN,
            CreationDate
        )
        VALUES (
            CESQL.EventType,
            CESQL.CustomerID,
            CESQL.DeviceUsageViolationID,
            CESQL.DeviceUsageEventViolationID,
            CESQL.CustomerTransactionID,
            CESQL.DeviceUsageID,
            CESQL.ViolationReportingApprovalCd,
            CESQL.ViolationReportingApprovalUser,
            CESQL.CreateDate,
            CESQL.CreateUser,
            CESQL.ModifyDate,
            CESQL.ModifyUser,
            CESQL.LogEntryTime,
            CESQL.Eventdate,
            CESQL.VIN,
            CESQL.NewVIN,
            CESQL.CreationDate
        )
""")

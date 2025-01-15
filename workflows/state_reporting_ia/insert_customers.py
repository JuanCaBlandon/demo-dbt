from pyspark.sql.functions import col
from args_parser import get_parser


# Get the parser
parser = get_parser()
args = parser.parse_args()

# Access parameters
env = args.environment

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
database_host = "172.16.1.161\dev"  # Note the escaped backslash
database_port = "1433"
database_name = "statereporting"
table = "TpmStateReportedCustomer"
username = dbutils.secrets.get(scope="state_reporting", key="sql_server_user")
password = dbutils.secrets.get(scope="state_reporting", key="sql_server_pass")

url = f"jdbc:sqlserver://{database_host};instanceName=dev;databaseName={database_name};encrypt=true;trustServerCertificate=true"


sql_where = "WHERE OffenseDate >= '2025-01-01'" if env == "prod" else "WHERE OffenseDate >= '2024-01-01'"
query = f"""
SELECT * FROM {database_name}.databricks.TmpStateReportedCustomer {sql_where}
"""

result_df = (spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("query", query)
    .option("user", username)
    .option("password", password)
    .load())

result_df = result_df.select(
    [col(column).alias(column.replace(' ', '_').replace(',', '_').replace(';', '_')
                      .replace('{', '_').replace('}', '_').replace('(', '_')
                      .replace(')', '_').replace('\n', '_').replace('\t', '_')
                      .replace('=', '_')) for column in result_df.columns]
)

result_df.createOrReplaceTempView("CustomersIA")

spark.sql(""" 
    MERGE INTO state_reporting_dev.bronze.state_reported_customer AS ST
    USING CustomersIA AS CU ON ST.CustomerID = CU.CustomerID
    WHEN MATCHED AND CU.ActiveStatus = 0 THEN
        UPDATE SET ST.ActiveStatusEndDate = current_date()
    WHEN NOT MATCHED THEN
        INSERT (
            CustomerReportingStateID, CustomerID, DriversLicenseNumber, FirstName, LastName, MiddleName, DateOfBirth, VIN, InstallDate,
            DeInstallDate, StateCode, ActiveStatus, EffectiveStartDate, EffectiveEndDate, DeviceLogRptgClassCd, CreateDate, CreateUser,
            ModifyDate, ModifyUser, CreationDate, ReportStatusCd, CustomerStatus, ActiveStatusStartDate, OffenseDate, IIDStartDate, IIDEndDate, RepeatOffender
        )
        VALUES (
            CU.CustomerReportingStateID, CU.CustomerID, CU.DriversLicenseNumber, CU.FirstName, CU.LastName, CU.MiddleName, try_cast(CU.DateOfBirth AS TIMESTAMP), CU.VIN, try_cast(CU.InstallDate AS TIMESTAMP),
            try_cast(CU.DeInstallDate AS TIMESTAMP), CU.StateCode, CU.ActiveStatus, try_cast(CU.EffectiveStartDate AS TIMESTAMP), try_cast(CU.EffectiveEndDate AS TIMESTAMP), CU.DeviceLogRptgClassCd, try_cast(CU.CreateDate AS TIMESTAMP), CU.CreateUser,
            try_cast(CU.ModifyDate AS TIMESTAMP), CU.ModifyUser, try_cast(CU.CreationDate AS TIMESTAMP), CU.ReportStatusCd, CU.CustomerStatus, CU.ActiveStatusStartDate, CU.OffenseDate, CU.IIDStartDate, CU.IIDEndDate, CU.RepeatOffender
        )
""")

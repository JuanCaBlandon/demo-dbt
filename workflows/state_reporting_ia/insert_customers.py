from pyspark.sql.functions import col
from args_parser import get_parser


# Get the parser
parser = get_parser()
args = parser.parse_args()

# Access parameters
env = args.environment
execution_date = args.execution_date

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
database_name = "statereporting"
table = "TpmStateReportedCustomer"
database_host = dbutils.secrets.get(scope = "state_reporting", key = f"sql_server_host_{env}")
username = dbutils.secrets.get(scope="state_reporting", key=f"sql_server_user_{env}")
password = dbutils.secrets.get(scope="state_reporting", key=f"sql_server_pass_{env}")

url = f"jdbc:sqlserver://{database_host};instanceName={env};databaseName={database_name};encrypt=true;trustServerCertificate=true"

query = f"""
SELECT * FROM {database_name}.databricks.TmpStateReportedCustomer
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

spark.sql(f""" 
    MERGE INTO state_reporting_{env}.bronze.customer_raw AS ST
    USING CustomersIA AS CU ON ST.CustomerID = CU.CustomerID
    WHEN MATCHED
        UPDATE SET
            ST.CustomerReportingStateID = CU.CustomerReportingStateID,
            ST.DriversLicenseNumber = CU.DriversLicenseNumber,
            ST.FirstName = CU.FirstName,
            ST.LastName = CU.LastName,
            ST.MiddleName = CU.MiddleName,
            ST.DateOfBirth = CU.DateOfBirth,
            ST.VIN = CU.VIN,
            ST.InstallDate = CU.InstallDate,
            ST.DeInstallDate = CU.DeInstallDate,
            ST.StateCode = CU.StateCode,
            ST.ActiveStatus = CU.ActiveStatus,
            ST.ReportStatusCD = CU.ReportStatusCD,
            ST.CustomerStatus = CU.CustomerStatus,
            ST.EffectiveStartDate = CU.EffectiveStartDate,
            ST.EffectiveEndDate = CU.EffectiveEndDate,
            ST.DeviceLogRptgClassCd = CU.DeviceLogRptgClassCd,
            ST.CreateDate = CU.CreateDate,
            ST.CreateUser = CU.CreateUser,
            ST.ModifyDate = CU.ModifyDate,
            ST.ModifyUser = CU.ModifyUser,
            ST.RepeatOffender = CU.RepeatOffender,
            ST.OffenseDate = CU.OffenseDate,
            ST.IIDStartDate = CU.IIDStartDate,
            ST.IIDEndDate = CU.IIDEndDate,
            ST.ModifyDate = {execution_date}
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            CustomerReportingStateID,
            CustomerID,
            DriversLicenseNumber,
            FirstName,
            LastName,
            MiddleName,
            DateOfBirth,
            VIN,
            InstallDate,
            DeInstallDate,
            StateCode,
            ActiveStatus,
            ReportStatusCD,
            CustomerStatus,
            ActiveStatusStartDate,
            EffectiveStartDate,
            EffectiveEndDate,
            DeviceLogRptgClassCd,
            CreateDate,
            CreateUser,
            ModifyDate,
            ModifyUser,
            RepeatOffender,
            OffenseDate,
            IIDStartDate,
            IIDEndDate,
            CreationDate
        )
        VALUES (
            CU.CustomerReportingStateID,
            CU.CustomerID,
            CU.DriversLicenseNumber,
            CU.FirstName,
            CU.LastName,
            CU.MiddleName,
            try_cast(CU.DateOfBirth AS TIMESTAMP),
            CU.VIN,
            try_cast(CU.InstallDate AS TIMESTAMP),
            try_cast(CU.DeInstallDate AS TIMESTAMP),
            CU.StateCode,
            CU.ActiveStatus,
            CU.ReportStatusCD,
            CU.CustomerStatus,
            CU.ActiveStatusStartDate,
            try_cast(CU.EffectiveStartDate AS TIMESTAMP),
            try_cast(CU.EffectiveEndDate AS TIMESTAMP),
            CU.DeviceLogRptgClassCd,
            try_cast(CU.CreateDate AS TIMESTAMP),
            CU.CreateUser,
            try_cast(CU.ModifyDate AS TIMESTAMP),
            CU.ModifyUser,
            CU.RepeatOffender,
            CU.OffenseDate,
            CU.IIDStartDate,
            CU.IIDEndDate,
            try_cast(CU.CreationDate AS TIMESTAMP)
        )
        WHEN NOT MATCHED BY SOURCE THEN
            UPDATE SET
                ST.ActiveStatus = 0,
                ST.ReportStatusCD = 'Inactive',
                ST.ActiveStatusEndDate = {execution_date},
                ST.ModifyDate = {execution_date}
""")


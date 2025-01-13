from pyspark.sql import SparkSession
from args_parser import get_parser

# Get the parser
parser = get_parser()
args = parser.parse_args()

# Access parameters
sp_name = args.sp_name

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
database_host = "172.16.1.161\dev"  # Note the escaped backslash
database_port = "1433"
database_name = "statereporting"
schema = "databricks"
table = "TpmStateReportedCustomer"
username = dbutils.secrets.get(scope="state_reporting", key="sql_server_user")
password = dbutils.secrets.get(scope="state_reporting", key="sql_server_pass")


url = f"jdbc:sqlserver://{database_host};instanceName=dev;databaseName={database_name};encrypt=true;trustServerCertificate=true"
print(url)

spark = SparkSession.builder.appName("StoredProcedureExecution").getOrCreate()
try:
    sc = spark.sparkContext
    driver_manager = sc._gateway.jvm.java.sql.DriverManager
    connection = driver_manager.getConnection(url, username, password)
    callable_statement = connection.prepareCall(f"{{CALL {schema}.{sp_name}}}")
    result_set = callable_statement.executeQuery()
    print(f"SP {sp_name} executed successfully")
except Exception as e:
    print(f"Execution failed with error:\n{str(e)}")


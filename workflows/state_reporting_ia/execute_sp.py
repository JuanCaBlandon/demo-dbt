print("test reconnect repo")  # TODO: Delete this
from pyspark.sql import SparkSession
from args_parser import get_parser
import sys

# Get the parser
parser = get_parser()
args = parser.parse_args()

# Access parameters
sp_name = args.sp_name

# Database connection details
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
database_host = "172.16.1.161"  # Remove the instance name from the URL
database_port = "1433"
database_name = "statereporting"
schema = "databricks"
table = "TpmStateReportedCustomer"
username = dbutils.secrets.get(scope="state_reporting", key="sql_server_user")
password = dbutils.secrets.get(scope="state_reporting", key="sql_server_pass")

# JDBC URL
url = f"jdbc:sqlserver://{database_host}:{database_port};databaseName={database_name};encrypt=true;trustServerCertificate=true"

print(url)

# Create a Spark session
spark = SparkSession.builder.appName("StoredProcedureExecution").getOrCreate()

# Define the query to call the stored procedure
query = f"EXEC {schema}.{sp_name}"

# Use Spark to execute the stored procedure via JDBC
try:
    df = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", f"({query}) AS tmp") \
        .option("user", username) \
        .option("password", password) \
        .option("driver", driver) \
        .load()

    print(f"SP {sp_name} executed successfully")
    df.show()  # Display the result if needed
except Exception as e:
    print(f"Execution failed with error:\n{str(e)}")
    sys.exit(1)

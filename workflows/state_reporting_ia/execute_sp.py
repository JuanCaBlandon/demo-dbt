from args_parser import get_parser

# Get the parser
parser = get_parser()
args = parser.parse_args()

# Access parameters
param1 = args.sp_name

# Database details
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
database_host = "172.16.1.161\dev"  # Note the escaped backslash
database_port = "1433"
database_name = "statereporting"
table = "TpmStateReportedCustomer"
user = "sourcemeridian"
password = "p%MQuwu4pC@!cp8o"

# Construct JDBC URL
url = f"jdbc:sqlserver://{database_host};instanceName=dev;databaseName={database_name};encrypt=true;trustServerCertificate=true"

# Print output
print(url, param1)
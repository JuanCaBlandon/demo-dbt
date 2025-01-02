
# Define connection parameters
database_host = "172.16.1.161" 
instanceName = "dev"
database_name = "StateReporting"
user = dbutils.secrets.get(scope="sql-credentials", key="username")
password = dbutils.secrets.get(scope="sql-credentials", key="password")

# Build connection URL with SSL parameters
url = (f"jdbc:sqlserver://{database_host};instanceName={instanceName};"
         f"database={database_name};"
         "encrypt=true;"
         "trustServerCertificate=true;"
         f"user={user};"
         f"password={password}") 

try:
    # Access the underlying Java DriverManager
    driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager

    # Establish the connection
    connection = driver_manager.getConnection(url, user, password)

    # Prepare and execute the stored procedure
    callable_statement = connection.prepareCall("{CALL dbo.GetCustomersViolationsIA}")
    result_set = callable_statement.executeQuery()


except Exception as e:
    print(f"Execution failed with error:\n{str(e)}")
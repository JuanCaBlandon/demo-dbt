from pyspark.sql import SparkSession,DataFrame
from datetime import datetime
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from args_parser import get_parser

# Get the parser
parser = get_parser()
args = parser.parse_args()

# Access parameters
env = args.environment
execution_date = args.execution_date
email_from = args.email_from
email_to = args.email_to

database_host = dbutils.secrets.get(scope = "state_reporting", key = f"sql_server_host_{env}")
username = dbutils.secrets.get(scope="state_reporting", key=f"sql_server_user_{env}")
password = dbutils.secrets.get(scope="state_reporting", key=f"sql_server_pass_{env}")
database_name = "CustSrv"


spark = SparkSession.builder \
    .appName("Insert notification data into SQL CommunicationQueue table ") \
    .getOrCreate()

jdbc_url = f"jdbc:sqlserver://{database_host};instanceName={env};databaseName={database_name};encrypt=true;trustServerCertificate=true"

connection_properties = {
    "user": username,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

schema = StructType([
    StructField("To", StringType(), True),
    StructField("From", StringType(), True),
    StructField("CC", StringType(), True),
    StructField("BCC", StringType(), True),
    StructField("Subject", StringType(), True),
    StructField("Body", StringType(), True),
    StructField("ResendCount", IntegerType(), True),
    StructField("SourceObject", StringType(), True),
    StructField("StatusCd", IntegerType(), True),
    StructField("StatusDate", TimestampType(), True),
    StructField("StatusMsg", StringType(), True),
    StructField("DeliveryStatusCd", IntegerType(), True),
    StructField("DeliveryConfirmationDate", TimestampType(), True),
    StructField("BatchIDCreate", IntegerType(), True),
    StructField("CreateDate", TimestampType(), True),
    StructField("CreateUser", StringType(), True),
    StructField("CreateSystemUserID", IntegerType(), True),
    StructField("ModifyUser", StringType(), True),
    StructField("ModifySystemUserID", IntegerType(), True),
    StructField("SendDate", TimestampType(), True)
])

def insert_data(schema, data, jdbc_url, connection_properties) -> DataFrame:
    try:
        df = spark.createDataFrame(data, schema)

        df.write \
            .jdbc(url=jdbc_url, 
                table="CommunicationQueue", 
                mode="append", 
                properties=connection_properties
                )

        return df
    except Exception as e:
        print(f"Error: {e}")
        return None


## Inconsistencies data #########################
audit_df = spark.read.table(f"compliance_{env}.compliance.audit_inconsistencies").where(f"inconsistency_id in (9,7) and created_at = {execution_date}")

## Batch data #########################
cleaned_df = spark.read.table(f"state_reporting_{env}.silver.batch_customer_cleaned").where(f'created_at = {execution_date}')
batch_df = cleaned_df.join(audit_df, (cleaned_df.batch_customer_dw_id == audit_df.source_table_id) & (audit_df.inconsistency_id == 9),"inner")\
            .select('vendor_name','drivers_license_number','first_name','last_name','middle_name','date_of_birth','vin','offense_date','repeat_offender','iid_start_date','iid_end_date')

pandas_df = batch_df.toPandas()
html_batch = pandas_df.to_html()
batch_count = batch_df.count()

## Batch data notification
insert_batch = [(email_to,email_from,"","","Iowa DataFeed",html_batch,0,"IA DataFeed do not match batch file",\
                 369, datetime.now(),"Pending",590,None,None, datetime.now(),"DatafeedUser",6254,"DatafeedUser",6254,None)]

## Customers data #####################
cus_cleaned_df = spark.read.table(f"state_reporting_{env}.silver.customer_cleaned").where(f'created_at = {execution_date}')
customer_df = cus_cleaned_df.join(audit_df, (cus_cleaned_df.customer_dw_id == audit_df.source_table_id) & (audit_df.inconsistency_id == 7),"inner")\
                .select('customer_id','drivers_license_number','first_name','last_name','middle_name','date_of_birth','vin','install_date','deinstall_date','state_code')


pandas_customer_df = customer_df.toPandas()
html_customer = pandas_customer_df.to_html()
customer_count = customer_df.count()

## Customer data notification
insert_customer = [(email_to,email_from,"","","Iowa DataFeed",html_customer,0,"IA DataFeed dont match with customer table",\
                    369,datetime.now(),"Pending",590,None,None, datetime.now(),"DatafeedUser",6254,"DatafeedUser",6254,None)]

data = []

if customer_count == 0 and batch_count == 0:
    print("No data from batch and customer cleaned tables")

elif customer_count == 0 and batch_count > 0:
    data = insert_batch
    
elif customer_count > 0 and batch_count == 0:
    data = insert_customer

else:
    for record in insert_customer:
        data.append(record)
    for record in insert_batch:
        data.append(record)


# Insert data if we have any
if data:
    df = insert_data(schema, data, jdbc_url, connection_properties)
    print('The data was ingested successfully into SQL CommunicationQueue table')
else:
    print("No data to insert")

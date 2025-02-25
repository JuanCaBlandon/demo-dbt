from zeep import Client
from zeep.transports import Transport
from requests import Session
from args_parser import get_parser
import xml.etree.ElementTree as ET
import logging.config


logging.config.dictConfig({
    'version': 1,
    'formatters': {
        'verbose': {
            'format': '%(name)s: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'verbose',
        },
    },
    'loggers': {
        'zeep.transports': {
            'level': 'DEBUG',
            'propagate': True,
            'handlers': ['console'],
        },
    }
})

class SOAPAuthClient:
    def __init__(self, wsdl_url, service_url=None):
        session = Session()
        transport = Transport(session=session)
        self.client = Client(wsdl_url, transport=transport)
        
        if service_url:
            service = self.client.service
            service._binding_options['address'] = service_url
        
        self.session_id = None

    def _extract_session_id(self, response):
        """
        Extract the session ID from the XML response
        """
        try:
            xml_str = response.replace('&lt;', '<').replace('&gt;', '>')
            root = ET.fromstring(xml_str)
            message = root.find('.//Message')
            if message is not None and message.text:
                return message.text
            raise ValueError("Could not find session ID in response")
        except Exception as e:
            print(f"Error parsing session ID: {str(e)}")
            raise

    def authenticate(self, username, password):
        """
        Authenticate with the SOAP service and get a session ID
        Returns the session ID if successful
        """
        try:
            result = self.client.service.Authenticate(userName=username, password=password)
            self.session_id = self._extract_session_id(result)
            return self.session_id
        except Exception as e:
            print(f"Authentication failed: {str(e)}")
            raise

class IgnitionInterlockServiceClient:
    def __init__(self, wsdl_url, service_url=None):
        """
        Initialize the Ignition Interlock Device Service client
        """
        session = Session()
        transport = Transport(session=session)
        self.client = Client(wsdl_url, transport=transport)
        
        if service_url:
            service = self.client.service
            service._binding_options['address'] = service_url

    def submit_device_log(self, session_id, log_data):
        """
        Submit an Ignition Interlock Device Service Log
        
        Args:
            session_id (str): Valid session ID from authentication
            log_data (dict): Dictionary containing the log data
        
        Returns:
            list: Array of ReturnValue objects
        """
        try:
            # Create the SOAP header with session ID
            header = {
                'AuthenticationSoapHeader': {
                    'SessionID': session_id
                }
            }
            
            # Create the device log object
            device_log = {
                'driversLicenseNumber': log_data.get('driversLicenseNumber'),
                'lastName': log_data.get('lastName'),
                'firstName': log_data.get('firstName'),
                'middleName': log_data.get('middleName'),
                'dateOfBirth': log_data.get('dateOfBirth'),
                'VIN': log_data.get('VIN'),
                'newVIN': log_data.get('newVIN'),
                'ignitionInterlockDeviceInstalledDate': log_data.get('ignitionInterlockDeviceInstalledDate'),
                'ignitionInterlockDeviceRemovedDate': log_data.get('ignitionInterlockDeviceRemovedDate'),
                'violationDate': log_data.get('violationDate'),
                'recordType': log_data.get('recordType')
            }
            
            # Submit the device log
            result = self.client.service.SubmitIgnitionInterlockDevice(
                IgnitionInterlockDeviceLog=device_log,
                _soapheaders=header
            )
            
            return result
        except Exception as e:
            print(f"Failed to submit device log: {str(e)}")
            raise

def getReportableEvents(env):
    """
    Extracts data from the Databricks table and returns a list of reportable events.
    Returns an empty list if no records are found.
    """
    df_revents = spark.read.table(f"state_reporting_{env}.gold.vw_webservice_delivery_ia")
    
    # Check if there are any records to process
    if df_revents.count() == 0:
        print(f"No reportable events found in table state_reporting_{env}.gold.vw_webservice_delivery_ia")
        return []
    
    reportable_events = [
        {
            'customer_state_dw_id': row['customer_state_dw_id'],
            'driversLicenseNumber': row['driversLicenseNumber'],
            'lastName': row['lastName'],
            'firstName': row['firstName'],
            'middleName': row['middleName'],
            'dateOfBirth': row['dateOfBirth'],
            'VIN': row['VIN'],
            'newVIN': row['newVIN'],
            'ignitionInterlockDeviceInstalledDate': row['ignitionInterlockDeviceInstalledDate'],
            'ignitionInterlockDeviceRemovedDate': row['ignitionInterlockDeviceRemovedDate'],
            'violationDate': row['violationDate'],
            'recordType': row['recordType']
        }
        for row in df_revents.collect()
    ]
    return reportable_events

def submitRecords(events_list, iid_client, session_id):
    """
    Submits device logs for a list of events and stores the responses along with customer_state_dw_id.
    Tracks submissions of record types 4 or 5 that receive error code 13.
    
    Returns:
        tuple: (submission_results, tracked_records)
            - submission_results: list of all submission results
            - tracked_records: list of submissions with record types 4 or 5 and error code 13
    """

    submission_results = []
    tracked_records = []

    for event in events_list:
        print(f"Submitting: {event['customer_state_dw_id']} - Drivers License: {event['driversLicenseNumber']}")

        # Remove 'customer_state_dw_id' before submitting
        log_data = {key: value for key, value in event.items() if key != 'customer_state_dw_id'}
        print(f"Current log data {log_data}")
        try:
            # Submit the device log using the session ID obtained earlier
            result = iid_client.submit_device_log(session_id, log_data)
            # Process the result
            for return_value in result:
                print(f"Error Code: {return_value.ErrorCode}")
                print(f"Message: {return_value.Message}")
                
                # Store the response along with the customer_state_dw_id
                submission_results.append({
                    'customer_state_dw_id': event['customer_state_dw_id'],
                    'error_code': return_value.ErrorCode,
                    'message': return_value.Message
                })
                                
                # Track record types 4 and 5 submitted successfully
                if event['recordType'] in [4, 5] and return_value.ErrorCode == 13:
                    tracked_records.append(event['customer_state_dw_id'])


        except Exception as e:
            print(f"Error: {str(e)}")
            submission_results.append({
                'customer_state_dw_id': event['customer_state_dw_id'],
                'error_code': 99,
                'message': str(e)
            })

    # Update the results table in Databricks
    return submission_results, tracked_records


def updateResultsTable(results, env):
    """
    Updates the results table in Databricks with the submission responses.
    Ensures that the data types match the table schema.
    """
    table_name = f"state_reporting_{env}.gold.processed_submissions_ia"

    table_schema = spark.table(table_name).schema
    df_results = spark.createDataFrame(results)

    # Cast columns to match the table schema
    for field in table_schema:
        if field.name in df_results.columns:
            df_results = df_results.withColumn(field.name, df_results[field.name].cast(field.dataType))

    try:
        row_count = df_results.count()
        df_results.write.format("delta").mode("append").saveAsTable(table_name)
        print(f"Successfully added {row_count} rows to {table_name}")
    except Exception as e:
        print(f"Error writing to Delta table: {str(e)}")
        raise

def updateCustomerStateReported(results, env, execution_date):
    """
    Updates the customer_state_reported table in Databricks based on submission responses.
    If submission response error code = 13, then status = 1; otherwise, status = 0.
    """
    
    status_dict = {}

    for result in results:
        customer_id = result['customer_state_dw_id']
        error_code = result['error_code']
        
        status = 1 if error_code == 13 else 0

        if customer_id not in status_dict or status_dict[customer_id] < status:
            status_dict[customer_id] = status

    df_status = spark.createDataFrame(
        [(key, value, execution_date) for key, value in status_dict.items()],
        ['customer_state_dw_id', 'status', 'submitted_at']
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
                target.submitted_at = source.submitted_at
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
    inactive_customers_df = spark.createDataFrame(
        [(id, 0, 'Inactive', execution_date, 'Webservice') for id in id_list],
        ['customer_state_dw_id', 'active_status', 'report_status_cd', 'active_status_end_date', 'active_status_end_type']
    )

    try:
        inactive_customers_df.createOrReplaceTempView("inactive_customers_temp")

        spark.sql(f"""
            MERGE INTO state_reporting_{env}.gold.customer AS target
            USING inactive_customers_temp AS source
            ON target.customer_state_dw_id = source.customer_state_dw_id
            WHEN MATCHED THEN
                UPDATE SET
                    target.active_status = source.active_status,
                    target.report_status_cd = source.report_status_cd,
                    target.active_status_end_date = source.active_status_end_date
        """)
        print(f"Successfully marked {len(id_list)} customers as inactive")
    except Exception as e:
        print(f"Error updating customer table: {str(e)}")
        raise
    finally:
        # Clean up the temporary view
        spark.sql("DROP VIEW IF EXISTS inactive_customers_temp")


def main():
    print("We shouldn't do anything yet")
    # Get the parser
    parser = get_parser()
    args = parser.parse_args()

    # Access parameters
    env = args.environment
    execution_date = args.execution_date
    
    # Get events to report from Databricks
    reportable_events = getReportableEvents(env)
    
    if not reportable_events:
        print("No events to report. Exiting.")
        return

    base_url = "https://arts.iowadot.gov"
    
    # Retrieve username and password from Databricks secrets
    username = dbutils.secrets.get(scope="state_reporting", key="iowa_ws_user_prd")
    password = dbutils.secrets.get(scope="state_reporting", key="iowa_ws_password_prd ")

    # Initialize the authentication client
    wsdl_url = f"{base_url}/Security/Session.asmx?wsdl"
    service_url = f"{base_url}/Security/Session.asmx"
    auth_client = SOAPAuthClient(wsdl_url, service_url)

    # Authenticate and retrieve session ID
    session_id = auth_client.authenticate(username=username, password=password)
    print(f"Successfully authenticated. Session ID: {session_id}")

    # TODO: Make sure the session ID is authenticated

    # Initialize the Ignition Interlock Service client
    iid_client = IgnitionInterlockServiceClient(
        wsdl_url=f"{base_url}/IgnitionInterlockDeviceService/IgnitionInterlockDeviceService.asmx?WSDL",
        service_url=f"{base_url}/IgnitionInterlockDeviceService/IgnitionInterlockDeviceService.asmx"
    )

    # Submit records
    submission_results, tracked_records  = submitRecords(reportable_events, iid_client, session_id)
    
    # Store results in DB
    updateResultsTable(submission_results, env)

    # Update customer_state_reported table
    updateCustomerStateReported(submission_results, env, execution_date)

    # Set inactive clients
    if tracked_records:
        setInactiveCustomers(tracked_records, execution_date, env)

if __name__ == "__main__":
    main()
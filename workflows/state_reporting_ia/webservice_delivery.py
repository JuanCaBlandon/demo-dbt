from zeep import Client
from zeep.transports import Transport
from requests import Session
from args_parser import get_parser
from datetime import datetime
import xml.etree.ElementTree as ET
import logging.config
import json


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
                    'message': return_value.Message,
                    'submitted_at': datetime.now()
                })
                                
                # Track record types 4 and 5 submitted successfully
                if event['recordType'] in [4, 5] and return_value.ErrorCode == 13:
                    tracked_records.append(event['customer_state_dw_id'])


        except Exception as e:
            print(f"Error: {str(e)}")
            submission_results.append({
                'customer_state_dw_id': event['customer_state_dw_id'],
                'error_code': 99,
                'message': str(e),
                'submitted_at': datetime.now()
            })

    # Update the results table in Databricks
    return submission_results, tracked_records


def main():
    print("Starting process")
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
    username = dbutils.secrets.get(scope="state_reporting", key=f"iowa_ws_user_{env}")
    password = dbutils.secrets.get(scope="state_reporting", key=f"iowa_ws_password_{env}")

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
    
    if submission_results:
        dbutils.jobs.taskValues.set(key="save_results", value=1)
        dbutils.jobs.taskValues.set(key="submission_results", value=json.dumps(submission_results))
        dbutils.jobs.taskValues.set(key="tracked_records", value=json.dumps(tracked_records))
    else:
        dbutils.jobs.taskValues.set(key="save_results", value=0)


if __name__ == "__main__":
    main()
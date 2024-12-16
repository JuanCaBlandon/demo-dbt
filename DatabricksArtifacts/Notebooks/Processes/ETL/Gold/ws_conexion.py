from zeep import Client
from zeep.transports import Transport
from requests import Session
import logging.config

# Set up logging to see the SOAP messages (optional but helpful for debugging)
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
    def __init__(self, wsdl_url):
        # Create a session to manage connections
        session = Session()
        
        # Create a transport with the session
        transport = Transport(session=session)
        
        # Initialize the SOAP client
        self.client = Client(wsdl_url, transport=transport)
        
        # Store the session ID once authenticated
        self.session_id = None

    def authenticate(self, username, password):
        """
        Authenticate with the SOAP service and get a session ID
        Returns the session ID if successful
        """
        try:
            # Call the Authenticate operation
            result = self.client.service.Authenticate(userName=username, password=password)
            
            # Store the session ID
            self.session_id = result
            
            return self.session_id
        except Exception as e:
            print(f"Authentication failed: {str(e)}")
            raise

    def get_session_timeout(self):
        """
        Get the session timeout value
        Requires a valid session ID
        """
        if not self.session_id:
            raise ValueError("Not authenticated. Call authenticate() first.")

        try:
            # Create the SOAP header with the session ID
            header = {
                'AuthenticationSoapHeader': {
                    'SessionID': self.session_id
                }
            }
            
            # Call the GetSessionTimeout operation with the header
            result = self.client.service.GetSessionTimeout(_soapheaders=header)
            
            return result
        except Exception as e:
            print(f"Failed to get session timeout: {str(e)}")
            raise

def main():
    # Service URLs
    wsdl_url = "https://artsdev.iowadot.gov/Security/Session.asmx?wsdl"
    
    # Create the client
    auth_client = SOAPAuthClient(wsdl_url)
    
    try:
        # Authenticate and get session ID
        session_id = auth_client.authenticate(username="your_username", password="your_password")
        print(f"Successfully authenticated. Session ID: {session_id}")
        
        # Get session timeout
        timeout = auth_client.get_session_timeout()
        print(f"Session timeout: {timeout} seconds")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
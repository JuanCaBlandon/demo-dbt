import os
from ftplib import FTP_TLS
import pyzipper
from args_parser import get_parser

# Get the parser
parser = get_parser()
args = parser.parse_args()

# Access parameters
env = args.environment
execution_date = args.execution_date
str_execution_date = execution_date.replace("-", "")

# Get secrets
host = dbutils.secrets.get(scope="state_reporting", key=f"iowa_ftp_host_{env}")
username = dbutils.secrets.get(scope="state_reporting", key=f"iowa_ftp_user_{env}")
password = dbutils.secrets.get(scope="state_reporting", key=f"iowa_ftp_password_{env}")
passkey = dbutils.secrets.get(scope="state_reporting", key=f"iowa_ftp_file_encryption_{env}")

# Paths
zip_folder = f"/Volumes/ia_batch_files_raw_{env}/default/ia_batch_files_raw_{env}/"
destination_folder = f"{zip_folder}/extracted_files"

# Ensure directories exist
os.makedirs(zip_folder, exist_ok=True)
os.makedirs(destination_folder, exist_ok=True)

filename = f"Intoxalock_{str_execution_date}.zip"
file_path = os.path.join(zip_folder, filename)

# Download file from FTP
try:
    with FTP_TLS(host) as ftp:
        ftp.login(username, password)
        ftp.prot_p()
        
        if filename in ftp.nlst():
            with open(file_path, 'wb') as f:
                ftp.retrbinary(f'RETR {filename}', f.write)
            print(f"Successfully downloaded {filename}")
        else:
            raise Exception(f"File {filename} not found on FTP server")
except Exception as e:
    raise Exception(f"FTP error: {str(e)}")

# Validate and extract zip file
try:
    # Check if file is valid and not empty
    with pyzipper.AESZipFile(file_path) as zf:
        file_list = zf.namelist()
        if not file_list:
            raise Exception("ZIP file is empty")
            
        # Extract files
        for name in file_list:
            data = zf.read(name, pwd=bytes(passkey, 'utf-8'))
            target_path = os.path.join(destination_folder, name)
            
            # Create directories if needed
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            
            with open(target_path, 'wb') as f:
                f.write(data)
                
    print(f"Successfully extracted files to {destination_folder}")
    
except Exception as e:
    raise Exception(f"Extraction error: {str(e)}")
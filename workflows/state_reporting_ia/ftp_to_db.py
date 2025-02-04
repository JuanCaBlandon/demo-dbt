# from ftplib import FTP, error_perm
# import os

# # FTP server details
# ftp_host = "ftp.dlptest.com"
# ftp_user = "dlpuser"
# ftp_pass = "rNrKYTX9g7z3RgJRmxWuGHbeu"
# ftp_file_path = "/actorstoday.txt"
# local_file_path = "/dbfs/tmp/actorstoday.txt"


# os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

# try:
#     # Connect to the FTP server
#     ftp = FTP(ftp_host)
#     ftp.login(ftp_user, ftp_pass)
#     ftp.set_pasv(True)
#     ftp.retrlines('LIST')
    
#     # Download the file to DBFS
#     with open(local_file_path, "wb") as local_file:
#         ftp.retrbinary(f"RETR {ftp_file_path}", local_file.write)
    
#     ftp.quit()

#     print(f"File downloaded to {local_file_path}")

# except error_perm as e:
#     print(f"Permission error: {e}")
# except Exception as e:
#     print(f"An error occurred: {e}")
#TEST
from ftplib import FTP, error_perm
import os

# FTP server details
ftp_host = "ftp.dlptest.com"
ftp_user = "dlpuser"
ftp_pass = "rNrKYTX9g7z3RgJRmxWuGHbeu"
ftp_file_path = "/actorstoday.txt"
local_file_path = "/dbfs/tmp/actorstoday.txt"

# Ensure the directory exists in DBFS using dbutils
dbutils.fs.mkdirs("dbfs:/tmp")

try:
    # Connect to the FTP server
    ftp = FTP(ftp_host)
    ftp.login(ftp_user, ftp_pass)
    ftp.set_pasv(True)
    ftp.retrlines('LIST')
    
    # Download the file to DBFS
    with open(local_file_path, "wb") as local_file:
        ftp.retrbinary(f"RETR {ftp_file_path}", local_file.write)
    
    ftp.quit()

    print(f"File downloaded to {local_file_path}")

except error_perm as e:
    print(f"Permission error: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
from datetime import datetime
from typing import Dict, List

# Test data scenarios
TEST_CASES = {
"test_case_1": {
        "driversLicenseNumber": "DL123456789",
        "lastName": "Johnson",
        "firstName": "Michael",
        "middleName": "James",
        "dateOfBirth": datetime(1985, 6, 15),
        "VIN": "5XYZU3LB4DG1234",
        "newVIN": None,
        "ignitionInterlockDeviceInstalledDate": datetime(2024, 1, 15),
        "ignitionInterlockDeviceRemovedDate": None,
        "violationDate": None,
        "recordType": 1,
        "description": "Valid installation record"
    },
    
    # First submit the type 7 record
    "test_case_type_7": {
        "driversLicenseNumber": "DL777777777",
        "lastName": "Anderson",
        "firstName": "Thomas",
        "middleName": None,
        "dateOfBirth": datetime(1992, 3, 15),
        "VIN": "1HGCM82633A1234",
        "newVIN": None,
        "ignitionInterlockDeviceInstalledDate": datetime(2024, 1, 1),
        "ignitionInterlockDeviceRemovedDate": None,
        "violationDate": None,
        "recordType": 7,
        "description": "Type 7 record needed for valid type 5"
    },
    
    # Then submit the matching type 5 record
    "test_case_valid_type_5": {
        "driversLicenseNumber": "DL777777777",
        "lastName": "Anderson",
        "firstName": "Thomas",
        "middleName": None,
        "dateOfBirth": datetime(1992, 3, 15),
        "VIN": "2HGES16575H1234",
        "newVIN": None,
        "ignitionInterlockDeviceInstalledDate": datetime(2024, 1, 1),
        "ignitionInterlockDeviceRemovedDate": None,
        "violationDate": None,
        "recordType": 5,
        "description": "Valid type 5 record with matching type 7"
    },
    
    # Invalid type 5 record without matching type 7
    "test_case_invalid_type_5": {
        "driversLicenseNumber": "DL888888888",
        "lastName": "Wilson",
        "firstName": "Sarah",
        "middleName": None,
        "dateOfBirth": datetime(1990, 5, 20),
        "VIN": "3VWLL7AJ3BM1234",
        "newVIN": None,
        "ignitionInterlockDeviceInstalledDate": datetime(2024, 1, 1),
        "ignitionInterlockDeviceRemovedDate": None,
        "violationDate": None,
        "recordType": 5,
        "description": "Invalid type 5 record - no matching type 7"
    },
    
    # Valid type 6 record with newVIN
    "test_case_valid_type_6": {
        "driversLicenseNumber": "DL666666666",
        "lastName": "Wilson",
        "firstName": "Jane",
        "middleName": None,
        "dateOfBirth": datetime(1988, 7, 15),
        "VIN": "9ABCD12345E1234",
        "newVIN": "8EFGH12345F1234",
        "ignitionInterlockDeviceInstalledDate": datetime(2024, 1, 1),
        "ignitionInterlockDeviceRemovedDate": None,
        "violationDate": None,
        "recordType": 6,
        "description": "Valid type 6 record with newVIN"
    },
    
    # Invalid type 6 record without newVIN
    "test_case_invalid_type_6": {
        "driversLicenseNumber": "DL666666667",
        "lastName": "Wilson",
        "firstName": "Jane",
        "middleName": None,
        "dateOfBirth": datetime(1988, 7, 15),
        "VIN": "9ABCD12345E1234",
        "newVIN": None,
        "ignitionInterlockDeviceInstalledDate": datetime(2024, 1, 1),
        "ignitionInterlockDeviceRemovedDate": None,
        "violationDate": None,
        "recordType": 6,
        "description": "Invalid type 6 record - missing newVIN"
    }
}
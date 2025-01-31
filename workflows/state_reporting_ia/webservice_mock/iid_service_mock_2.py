from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional
from enum import IntEnum
import json
import os

class ErrorCodes(IntEnum):
    SUCCESS = 0
    VALIDATION_ERROR = 1
    BUSINESS_RULE_VIOLATION = 2

@dataclass
class IgnitionInterlockDeviceServiceLog:
    driversLicenseNumber: str
    lastName: str
    firstName: str
    middleName: Optional[str]
    dateOfBirth: datetime
    VIN: str
    newVIN: Optional[str]
    ignitionInterlockDeviceInstalledDate: datetime
    ignitionInterlockDeviceRemovedDate: Optional[datetime]
    violationDate: Optional[datetime]
    recordType: int

    def __post_init__(self):
        # Validate required fields
        if not self.driversLicenseNumber or len(self.driversLicenseNumber) > 50:
            raise ValueError("Invalid driver's license number")
        if not self.lastName or len(self.lastName) > 80:
            raise ValueError("Invalid last name")
        if not self.firstName or len(self.firstName) > 80:
            raise ValueError("Invalid first name")
        if self.middleName and len(self.middleName) > 80:
            raise ValueError("Invalid middle name")
        if not self.VIN or len(self.VIN) > 16:
            raise ValueError("Invalid VIN")
        if self.newVIN and len(self.newVIN) > 16:
            raise ValueError("Invalid new VIN")

@dataclass
class ReturnValue:
    ErrorCode: int
    Message: str

class IIDService:

    def __init__(self):
        self.submissions_file = "successful_submissions.json"
        if not os.path.exists(self.submissions_file):
            self._initialize_submissions_file()
    
    def _initialize_submissions_file(self):
        try:
            with open(self.submissions_file, 'w') as f:
                json.dump([], f)
        except Exception as e:
            print(f"Warning: Could not initialize submissions file: {e}")

    def load_all_submissions(self) -> List[dict]:
        """Load all historical submissions"""
        if not os.path.exists(self.submissions_file):
            # Create the file if it doesn't exist
            with open(self.submissions_file, 'w') as f:
                json.dump([], f)
            return []
            
        try:
            with open(self.submissions_file, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: Submissions file {self.submissions_file} contains invalid JSON")
            # Backup corrupted file and create new one
            backup_file = f"{self.submissions_file}.backup"
            os.rename(self.submissions_file, backup_file)
            print(f"Corrupted file backed up to {backup_file}")
            with open(self.submissions_file, 'w') as f:
                json.dump([], f)
            return []
        except Exception as e:
            print(f"Warning: Error loading submissions: {e}")
            return []

    def has_matching_type_7_submission(self, log: IgnitionInterlockDeviceServiceLog) -> bool:
            """Check if there's a matching type 7 submission for the given record"""
            submissions = self.load_all_submissions()
            print(f"Sumissions{submissions}")
            
            for submission in submissions:
                test_data = submission["test_data"]
                service_response = submission["service_response"]

                if (test_data["recordType"] == 7 and
                    test_data["driversLicenseNumber"] == log.driversLicenseNumber and
                    all(rv["ErrorCode"] == 0 for rv in service_response)):
                    return True
            
            return False

    def validate_business_rules(self, log: IgnitionInterlockDeviceServiceLog) -> List[ReturnValue]:
        """Validate business rules and return any errors"""
        errors = []
        
        # Validate record type 6 requires newVIN
        if log.recordType == 6 and not log.newVIN:
            errors.append(
                ReturnValue(
                    ErrorCode=ErrorCodes.BUSINESS_RULE_VIOLATION,
                    Message="Record type 6 requires a new VIN"
                )
            )
            
        # Validate record type 5 requires matching type 7
        if log.recordType == 5:
            if not self.has_matching_type_7_submission(log):
                errors.append(
                    ReturnValue(
                        ErrorCode=ErrorCodes.BUSINESS_RULE_VIOLATION,
                        Message="Record type 5 requires a matching record type 7 submission with the same driversLicenseNumber"
                    )
                )
            
        return errors

def SubmitIgnitionInterlockDevice(self, log: IgnitionInterlockDeviceServiceLog) -> dict:
    """
    Submit a record and return a JSON-formatted response instead of a list of ReturnValue objects.
    """
    try:
        # Validate business rules
        validation_errors = self.validate_business_rules(log)
        if validation_errors:
            return {
                "ErrorCode": ErrorCodes.BUSINESS_RULE_VIOLATION,
                "Message": [rv.Message for rv in validation_errors]
            }

        # If validation passes, return success
        return {
            "ErrorCode": ErrorCodes.SUCCESS,
            "Message": "Successfully submitted"
        }
    
    except ValueError as e:
        return {"ErrorCode": ErrorCodes.VALIDATION_ERROR, "Message": str(e)}
    
    except Exception as e:
        return {"ErrorCode": ErrorCodes.VALIDATION_ERROR, "Message": str(e)}


# Example usage:
def example_usage():
    # Create service instance
    service = IIDService()

    # Create a sample log entry
    log = IgnitionInterlockDeviceServiceLog(
        driversLicenseNumber="DL123456",
        lastName="Smith",
        firstName="John",
        middleName="Robert",
        dateOfBirth=datetime(1990, 1, 1),
        VIN="1HGCM82633A123456",
        newVIN=None,
        ignitionInterlockDeviceInstalledDate=datetime(2024, 1, 1),
        ignitionInterlockDeviceRemovedDate=None,
        violationDate=None,
        recordType=1
    )

    # Submit to service
    result = service.SubmitIgnitionInterlockDevice(log)
    
    # Print results
    for rv in result:
        print(f"Error Code: {rv.ErrorCode}")
        print(f"Message: {rv.Message}")

if __name__ == "__main__":
    example_usage()
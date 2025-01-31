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

    def has_matching_type_7_submission(self, log: IgnitionInterlockDeviceServiceLog, previous_submissions: List[dict]) -> bool:
        """Check if there's a matching type 7 submission for the given record in-memory."""
        
        for submission in previous_submissions:
            test_data = submission["test_data"]
            service_response = submission["service_response"]

            if (test_data["recordType"] == 7 and
                test_data["driversLicenseNumber"] == log.driversLicenseNumber and
                all(rv["ErrorCode"] == 0 for rv in service_response)):
                return True

        return False

    def validate_business_rules(self, log: IgnitionInterlockDeviceServiceLog, previous_submissions: List[dict]) -> List[ReturnValue]:
        """Validate business rules dynamically using in-memory submissions."""
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
            if not self.has_matching_type_7_submission(log, previous_submissions):
                errors.append(
                    ReturnValue(
                        ErrorCode=ErrorCodes.BUSINESS_RULE_VIOLATION,
                        Message="Record type 5 requires a matching record type 7 submission with the same driversLicenseNumber"
                    )
                )
        
        return errors


    def SubmitIgnitionInterlockDevice(self, log: IgnitionInterlockDeviceServiceLog, previous_submissions: List[dict]) -> List[ReturnValue]:
        """
        Processes the submission dynamically without file operations.
        """
        try:
            # Validate business rules
            validation_errors = self.validate_business_rules(log, previous_submissions)
            if validation_errors:
                return validation_errors

            # If validation passes, return success response
            return [ReturnValue(ErrorCode=ErrorCodes.SUCCESS, Message="Successfully submitted")]
        
        except ValueError as e:
            return [ReturnValue(ErrorCode=ErrorCodes.VALIDATION_ERROR, Message=str(e))]
        except Exception as e:
            return [ReturnValue(ErrorCode=ErrorCodes.VALIDATION_ERROR, Message=str(e))]


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
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional
from enum import IntEnum

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

    def validate(self) -> List[tuple]:
        """Validates all fields and returns list of errors"""
        errors = []
        
        # Validate required fields
        if not self.driversLicenseNumber:
            errors.append((ErrorCodes.VALIDATION_ERROR, "Driver's license number is required"))
        elif len(self.driversLicenseNumber) > 50:
            errors.append((ErrorCodes.VALIDATION_ERROR, "Invalid driver's license number"))
        if not self.lastName or len(self.lastName) > 80:
            errors.append((ErrorCodes.VALIDATION_ERROR, "Invalid last name"))
        if not self.firstName or len(self.firstName) > 80:
            errors.append((ErrorCodes.VALIDATION_ERROR, "Invalid first name"))
        if self.middleName and len(self.middleName) > 80:
            errors.append((ErrorCodes.VALIDATION_ERROR, "Invalid middle name"))
        if not self.VIN or len(self.VIN) > 17:
            errors.append((ErrorCodes.VALIDATION_ERROR, "Invalid VIN"))
        if self.newVIN and len(self.newVIN) > 17:
            errors.append((ErrorCodes.VALIDATION_ERROR, "Invalid new VIN"))
            
        return errors

@dataclass
class ReturnValue:
    ErrorCode: int
    Message: str

class IIDService:
    def has_matching_type_7_submission(self, log: IgnitionInterlockDeviceServiceLog, previous_submissions: List[dict]) -> bool:
        """Check if there's a matching type 7 submission for the given record"""
        submissions = previous_submissions
        
        for submission in submissions:
            test_data = submission["test_data"]
            service_response = submission["service_response"]

            if (test_data["recordType"] == 7 and
                test_data["driversLicenseNumber"] == log.driversLicenseNumber and
                all(rv["ErrorCode"] == 0 for rv in service_response)):
                return True
        
        return False

    def validate_business_rules(self, log: IgnitionInterlockDeviceServiceLog, previous_submissions: List[dict]) -> List[ReturnValue]:
        """Validate all business rules and return list of errors"""
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
        Process submission and return all validation errors or success
        """
        try:
            # Collect all validation errors
            all_errors = []
            
            # Field validation errors
            field_errors = log.validate()
            all_errors.extend([ReturnValue(code, msg) for code, msg in field_errors])
            
            # Always check business rules to get all possible errors
            business_errors = self.validate_business_rules(log, previous_submissions)
            all_errors.extend(business_errors)
            
            # Return errors if any, otherwise success
            if all_errors:
                return all_errors
            return [ReturnValue(ErrorCode=ErrorCodes.SUCCESS, Message="Successfully submitted")]
            
        except Exception as e:
            return [ReturnValue(ErrorCode=ErrorCodes.VALIDATION_ERROR, Message=str(e))]
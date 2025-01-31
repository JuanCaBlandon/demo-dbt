import json
from datetime import datetime
from iid_service_mock_2 import IIDService, IgnitionInterlockDeviceServiceLog
import os

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle datetime objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def generate_successful_case_json(test_name: str, test_data: dict, result: list) -> dict:
    """Generate a JSON object for a successful test case instead of saving it to a file."""
    
    submission_data = {
        "test_name": test_name,
        "submission_date": datetime.now().isoformat(),
        "test_data": test_data,
        "service_response": [{"ErrorCode": rv.ErrorCode, "Message": rv.Message} for rv in result]
    }
    
    return submission_data  # Return JSON instead of writing to a file


def process_record(record: dict, record_id: int, previous_submissions: List[dict]):
    """Processes a single record dynamically without file dependencies."""
    service = IIDService()

    try:
        log = IgnitionInterlockDeviceServiceLog(**record)
        result = service.SubmitIgnitionInterlockDevice(log, previous_submissions)

        print("\nService Response:")
        print("-----------------")
        for rv in result:
            print(f"Error Code: {rv.ErrorCode}")
            print(f"Message: {rv.Message}")

        # Convert result to JSON format
        submission_json = {
            "test_name": f"Record_{record_id}",
            "submission_date": datetime.now().isoformat(),
            "test_data": record,
            "service_response": [{"ErrorCode": rv.ErrorCode, "Message": rv.Message} for rv in result]
        }

        return submission_json  # Return JSON instead of writing to a file

    except ValueError as e:
        print(f"\nValidation Error: {str(e)}")
    except Exception as e:
        print(f"\nUnexpected Error: {str(e)}")

    return None  # Return None if processing failed



def process_multiple_records(records: list):
    """Processes multiple records sequentially"""
    count = 0
    for index, record in enumerate(records):
        print("\n" + "=" * 50)
        print(f"Processing record {index + 1}")
        print("-" * 50)
        
        process_record(record, index + 1)
        count += 1

    print(f"\nProcess completed! Processed {count} records.")

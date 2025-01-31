import json
from datetime import datetime
from iid_service_mock import IIDService, IgnitionInterlockDeviceServiceLog
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


def process_record(record: dict, record_id: int):
    """Processes a single record and returns the response JSON instead of writing it to a file."""
    service = IIDService()
    
    try:
        log = IgnitionInterlockDeviceServiceLog(**record)
        result_json = service.SubmitIgnitionInterlockDevice(log)  # Now returning a JSON dict

        print("\nService Response:")
        print("-----------------")
        print(json.dumps(result_json, indent=2))  # Pretty-print response

        return result_json  # Return JSON instead of saving to a file

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

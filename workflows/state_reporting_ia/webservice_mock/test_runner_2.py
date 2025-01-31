import json
from datetime import datetime
from iid_service_mock_2 import IIDService, IgnitionInterlockDeviceServiceLog
from typing import List


def process_record(record: dict, record_id: str, previous_submissions: List[dict]):
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
            "record_id": record_id,
            "submission_date": datetime.now().isoformat(),
            "response_code": result,
            "service_response": [{"ErrorCode": rv.ErrorCode, "Message": rv.Message} for rv in result]
        }

        return submission_json  # Return JSON instead of writing to a file

    except ValueError as e:
        print(f"\nValidation Error: {str(e)}")
    except Exception as e:
        print(f"\nUnexpected Error: {str(e)}")

    return None  # Return None if processing failed

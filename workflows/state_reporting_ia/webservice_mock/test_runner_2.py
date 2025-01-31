import json
from datetime import datetime
from iid_service_mock_2 import IIDService, IgnitionInterlockDeviceServiceLog
from typing import List


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


def process_record(record: dict, record_id: int, previous_submissions: List[dict], successful_submissions: List[dict], failed_submissions: List[dict]):
    """Processes a single record dynamically and stores both success & failure responses."""
    service = IIDService()

    try:
        log = IgnitionInterlockDeviceServiceLog(**record)
        result = service.SubmitIgnitionInterlockDevice(log, previous_submissions)

        print(f"\n📝 Processed Record {record_id}: {record}")
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

        # ✅ Store failed records separately
        if any(rv["ErrorCode"] != 0 for rv in result):
            print(f"⚠️ Record {record_id} failed validation.")
            failed_submissions.append(submission_json)  # Store in failed list
        else:
            successful_submissions.append(submission_json)  # ✅ Store if no errors
            previous_submissions.append(submission_json)  # Add to history for future rule checks

        return submission_json  # ✅ Always return the processed record

    except ValueError as e:
        print(f"\nValidation Error: {str(e)}")
    except Exception as e:
        print(f"\nUnexpected Error: {str(e)}")

    return None  # Return None if processing completely failed



def process_multiple_records(records: list):
    """Processes multiple records sequentially and tracks both successful & failed submissions."""
    previous_submissions = []
    successful_submissions = []
    failed_submissions = []

    count = 0

    for index, record in enumerate(records):
        print("\n" + "=" * 50)
        print(f"Processing record {index + 1}")
        print("-" * 50)

        submission_json = process_record(record, index + 1, previous_submissions, successful_submissions, failed_submissions)

        if submission_json:
            count += 1

    print(f"\nProcess completed! Processed {count} records.")
    
    # ✅ Save both successful and failed records separately
    return successful_submissions, failed_submissions  # Return for saving

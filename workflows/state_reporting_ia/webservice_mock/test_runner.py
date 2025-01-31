import json
from datetime import datetime
from iid_service_mock import IIDService, IgnitionInterlockDeviceServiceLog
from test_data import TEST_CASES
import os

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle datetime objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# def clean_submissions_file():
#     """Clean the submissions file by creating a new empty one"""
#     filename = "successful_submissions.json"
#     with open(filename, 'w') as f:
#         json.dump([], f)
#     print("Cleaned previous submissions file")

def save_successful_case(test_name: str, test_data: dict, result: list):
    """Save successful test case to a JSON file"""
    filename = "successful_submissions.json"
    
    # Prepare the data to save
    submission_data = {
        "test_name": test_name,
        "submission_date": datetime.now().isoformat(),
        "test_data": test_data,
        "service_response": [{"ErrorCode": rv.ErrorCode, "Message": rv.Message} for rv in result]
    }
    
    # Load existing data
    with open(filename, 'r') as f:
        try:
            existing_data = json.load(f)
        except json.JSONDecodeError:
            existing_data = []
    
    # Add new submission
    existing_data.append(submission_data)
    
    # Save updated data
    with open(filename, 'w') as f:
        json.dump(existing_data, f, cls=DateTimeEncoder, indent=2)
    
    print(f"\nSuccessful submission saved to {filename}")

def run_test_cases():
    # Clean previous submissions
    # clean_submissions_file()
    
    # Create service instance
    service = IIDService()
    
    # Iterate through all test cases
    for test_name, test_data in TEST_CASES.items():
        print("\n" + "="*50)
        print(f"Running test: {test_name}")
        print(f"Description: {test_data['description']}")
        print("-"*50)
        
        try:
            # Create log entry from test data
            # Remove 'description' as it's not part of the actual log object
            test_data_copy = test_data.copy()
            test_data_copy.pop('description')
            
            log = IgnitionInterlockDeviceServiceLog(**test_data_copy)
            
            # Submit to service
            result = service.SubmitIgnitionInterlockDevice(log)
            
            # Print results
            print("\nService Response:")
            print("-----------------")
            for rv in result:
                print(f"Error Code: {rv.ErrorCode}")
                print(f"Message: {rv.Message}")
                
            # If all return values have ErrorCode 0 (success), save the case
            # if all(rv.ErrorCode == 0 for rv in result):
            save_successful_case(test_name, test_data_copy, result)
                
        except ValueError as e:
            print(f"\nValidation Error: {str(e)}")
        except Exception as e:
            print(f"\nUnexpected Error: {str(e)}")

def read_successful_submissions():
    """Utility function to read and display saved submissions"""
    filename = "successful_submissions.json"
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            try:
                submissions = json.load(f)
                if not submissions:
                    print("\nNo successful submissions found")
                    return
                    
                print("\nSuccessful Submissions Summary:")
                print("="*30)
                for submission in submissions:
                    print(f"\nTest Name: {submission['test_name']}")
                    print(f"Submission Date: {submission['submission_date']}")
                    print("Service Response:")
                    for response in submission['service_response']:
                        print(f"  Error Code: {response['ErrorCode']}")
                        print(f"  Message: {response['Message']}")
                    print("-"*30)
            except json.JSONDecodeError:
                print("Error reading submissions file")
    else:
        print("No successful submissions file found")

if __name__ == "__main__":
    run_test_cases()
    print("\nShowing saved submissions:")
    read_successful_submissions()
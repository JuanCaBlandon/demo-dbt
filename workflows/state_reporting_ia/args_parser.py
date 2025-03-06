import argparse

def get_parser():
    """Returns a configured argument parser."""
    parser = argparse.ArgumentParser(description="Process parameters for the Databricks job.")
    parser.add_argument("--environment", required=False, help="Execution Environment")
    parser.add_argument("--sp_name", required=False, help="Name of the SP to execute")
    parser.add_argument("--start_date", required=False, help="Start date for Events processing")
    parser.add_argument("--end_date", required=False, help="End date for Events processing")
    parser.add_argument("--execution_date", required=False, help="Date when the process is executed")
    parser.add_argument("--email_from", required=False, help="Email from, to send compliance notification")
    parser.add_argument("--email_to", required=False, help="Email to, to receive compliance notification")


    return parser
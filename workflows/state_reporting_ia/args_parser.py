import argparse

def get_parser():
    """Returns a configured argument parser."""
    parser = argparse.ArgumentParser(description="Process parameters for the Databricks job.")
    parser.add_argument("--sp_name", required=False, help="Name of the SP to execute")
    return parser
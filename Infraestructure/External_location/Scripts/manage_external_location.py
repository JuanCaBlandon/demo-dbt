import argparse
import json
import requests
import os

# Load configuration from JSON file
def load_config(config_path):
    with open(config_path, 'r') as config_file:
        return json.load(config_file)

# API Request Function for External Location
def api_request(url, method, headers, payload=None):
    if method == 'POST':
        response = requests.post(url, headers=headers, json=payload)
    elif method == 'GET':
        response = requests.get(url, headers=headers)
    elif method == 'PATCH':
        response = requests.patch(url, headers=headers, json=payload)
    elif method == 'DELETE':
        response = requests.delete(url, headers=headers)
    return response

# External Location CRUD Operations
def external_location_operations(config, operation):
    DATABRICKS_HOST = os.getenv('DATABRICKS_HOST')
    DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')
    headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}', 'Content-Type': 'application/json'}
    
    url = f"https://{DATABRICKS_HOST}/api/2.0/unity-catalog/external-locations"

    # Perform CRUD operation
    if operation == 'create':
        response = api_request(url, 'POST', headers, config)
    elif operation == 'delete':
        location_name = config.get('name')
        response = api_request(f"{url}/{location_name}", 'DELETE', headers)
    elif operation == 'update':
        location_name = config.get('name')
        response = api_request(f"{url}/{location_name}", 'PATCH', headers, config)
    elif operation == 'read':
        response = api_request(url, 'GET', headers)

    if response.status_code == 200:
        print(f"External location {operation} successful.")
    else:
        print(f"Error with external location {operation}: {response.text}")

# Main Argument Parsing and Execution
parser = argparse.ArgumentParser(description='Manage external locations in Databricks.')
parser.add_argument('--operation', type=str, choices=['create', 'read', 'update', 'delete'], required=True,
                    help='CRUD operation to perform: create, read, update, delete')
parser.add_argument('--config-path', type=str, default="external_location_config.json", help='Path to JSON config file')
parser.add_argument('--resource-name', type=str, required=True, help='Name of the external location to manage')

args = parser.parse_args()

# Load the external location configuration based on the resource name
config_data = load_config(args.config_path)
location_config = config_data.get("external_locations", {}).get(args.resource_name, {})

# Perform the selected operation
external_location_operations(location_config, args.operation)

import argparse
import json
import requests
import os

# Load configuration from JSON file
def load_config(config_path):
    with open(config_path, 'r') as config_file:
        return json.load(config_file)

# API Request Function for Cluster
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

# Cluster CRUD Operations
def cluster_operations(config, operation):
    DATABRICKS_HOST = os.getenv('DATABRICKS_HOST')
    DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')
    headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}', 'Content-Type': 'application/json'}
    
    url = f"https://{DATABRICKS_HOST}/api/2.0/clusters"

    # Perform CRUD operation
    if operation == 'create':
        response = api_request(f"{url}/create", 'POST', headers, config)
    elif operation == 'delete':
        cluster_id = config.get('cluster_id')
        response = api_request(f"{url}/delete", 'POST', headers, {"cluster_id": cluster_id})
    elif operation == 'update':
        cluster_id = config.get('cluster_id')
        response = api_request(f"{url}/edit", 'POST', headers, {"cluster_id": cluster_id, **config})
    elif operation == 'read':
        response = api_request(f"{url}/list", 'GET', headers)

    if response.status_code == 200:
        print(f"Cluster {operation} successful.")
    else:
        print(f"Error with cluster {operation}: {response.text}")

# Main Argument Parsing and Execution
parser = argparse.ArgumentParser(description='Manage clusters in Databricks.')
parser.add_argument('--operation', type=str, choices=['create', 'read', 'update', 'delete'], required=True,
                    help='CRUD operation to perform: create, read, update, delete')
parser.add_argument('--config-path', type=str, default="cluster_config.json", help='Path to JSON config file')
parser.add_argument('--resource-name', type=str, required=True, help='Name of the cluster to manage')

args = parser.parse_args()

# Load the cluster configuration based on the resource name
config_data = load_config(args.config_path)
cluster_config = config_data.get("clusters", {}).get(args.resource_name, {})

# Perform the selected operation
cluster_operations(cluster_config, args.operation)

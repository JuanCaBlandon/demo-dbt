import argparse
import json
import requests
import os

# Cargar la configuración del clúster desde el archivo JSON
def load_cluster_config(config_path):
    with open(config_path, 'r') as config_file:
        return json.load(config_file)

# Crear el clúster en Databricks
def create_cluster(config):
    DATABRICKS_HOST = os.getenv('DATABRICKS_HOST')
    DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')

    # URL para la API de Databricks
    url = f"https://{DATABRICKS_HOST}/api/2.0/clusters/create"

    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'application/json'
    }

    # Convertir la configuración del clúster en el formato esperado por la API
    cluster_payload = {
        "cluster_name": config["cluster_name"],
        "spark_version": config["spark_version"],
        "node_type_id": config["node_type_id"],
        "num_workers": config["num_workers"],
        "autotermination_minutes": config["autotermination_minutes"],
        "custom_tags": config["tags"]
    }

    # Hacer la solicitud a la API de Databricks
    response = requests.post(url, headers=headers, json=cluster_payload)

    if response.status_code == 200:
        print("Cluster creado exitosamente.")
    else:
        print(f"Error al crear el cluster: {response.text}")

# Configurar el parser de argumentos
parser = argparse.ArgumentParser(description='Crear un clúster en Databricks.')
parser.add_argument('--cluster-name', type=str, help='Nombre del clúster')
parser.add_argument('--node-type-id', type=str, help='Tipo de nodo (por ejemplo, i3.xlarge)')
parser.add_argument('--tags', type=str, help='Etiquetas personalizadas en formato key=value,key=value')

args = parser.parse_args()

# Ruta al archivo de configuración del clúster
config_path = "Infrastructure/Clusters/Parameters/cluster_config.json"

# Cargar la configuración base
cluster_config = load_cluster_config(config_path)

# Si se pasan parámetros opcionales, sobrescribir los valores en la configuración
if args.cluster_name:
    cluster_config["cluster_name"] = args.cluster_name

if args.node_type_id:
    cluster_config["node_type_id"] = args.node_type_id

if args.tags:
    # Parsear las etiquetas desde un formato key=value,key=value
    tags = dict(tag.split('=') for tag in args.tags.split(','))
    cluster_config["tags"].update(tags)

# Crear el clúster con la configuración final
create_cluster(cluster_config)

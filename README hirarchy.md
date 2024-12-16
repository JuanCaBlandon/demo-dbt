/root
│
├── /ARMTemplates
│   ├── /Infrastructure
│   │   ├── databricks_workspace.json
│   │   ├── storage_account.json
│   │   └── networking.json
│   ├── /Clusters
│   │   ├── cluster_configuration.json
│   │   └── autoscaling.json
│   └── /Parameters
│       ├── dev.parameters.json
│       ├── staging.parameters.json
│       └── prod.parameters.json
│
├── /DatabricksArtifacts
│   ├── /Notebooks
│   │   ├── /Processes
│   │   │   ├── /ETL
│   │   │   │   ├── /Bronze
│   │   │   │   │   ├── ingest_raw_data_wisconsin.py                  # **WIDOT Ingesta Inicial (Python)**
│   │   │   │   │   └── ingest_config_by_state.py                    # **WIDOT: Ingesta Tabla de Configuración por Estado (Python)**
│   │   │   │   ├── /Silver
│   │   │   │   │   ├── clean_data_wisconsin.sql                      # **WIDOT Limpieza de Datos (SQL)**
│   │   │   │   │   └── transform_data_wisconsin.sql                  # **WIDOT Transformación de Datos (SQL)**
│   │   │   │   └── /Gold
│   │   │   │       ├── create_clean_table_wisconsin.sql             # **WIDOT Creación Tabla Clean (SQL)**
│   │   │   │       └── load_to_ftp.py                                # **WIDOT Carga a FTP (Python)**
│   │   │   ├── /Analytics
│   │   │   │   ├── create_views.sql
│   │   │   │   └── generate_reports.sql
│   │   │   └── /MachineLearning
│   │   │       ├── data_preparation.sql
│   │   │       └── model_training.sql
│   │   ├── /Utilities
│   │   │   ├── helper_functions.py
│   │   │   └── config_loader.py
│   │   └── /Shared
│   │       ├── constants.py
│   │       ├── common_utils.py
│   │       ├── transform_function.sql                               # **Funciones de Transformación (SQL)**
│   │       └── categorize_function.sql                              # **Funciones de Categorización (SQL)**
│   │
│   ├── /SQL
│   │   ├── /Bronze
│   │   │   ├── table_raw_sales.sql
│   │   │   └── table_raw_customers.sql
│   │   ├── /Silver
│   │   │   ├── table_cleaned_sales.sql
│   │   │   └── table_cleaned_customers.sql
│   │   ├── /Gold
│   │   │   ├── table_sales_metrics.sql
│   │   │   └── table_customer_insights.sql
│   │   ├── /Views
│   │   │   ├── view_sales.sql
│   │   │   └── view_customers.sql
│   │   ├── /Functions
│   │   │   ├── function_calculate_discount.sql
│   │   │   └── function_format_date.sql
│   │   └── /Tables
│   │       ├── table_sales.sql
│   │       └── table_customers.sql
│   │
│   └── /Libraries
│       ├── custom_library.jar
│       └── utils_library.py
│
├── /Configurations
│   ├── /Tables
│   │   ├── state_configurations.sql                                     # **Tabla General de Configuración por Estado**
│   │   ├── global_parameters.sql                                        # **Tabla de Parámetros Globales**
│   │   └── ftp_settings.sql                                             # **Tabla de Configuraciones FTP**
│   ├── /Scripts
│   │   ├── create_state_config_table.sh                                 # **Script para Crear Tabla de Configuración por Estado**
│   │   ├── create_global_parameters_table.sh                            # **Script para Crear Tabla de Parámetros Globales**
│   │   └── create_ftp_settings_table.sh                                 # **Script para Crear Tabla de Configuraciones FTP**
│   └── /Data
│       ├── initial_state_configurations.csv                             # **Datos Iniciales para Configuración por Estado**
│       ├── initial_global_parameters.csv                                # **Datos Iniciales para Parámetros Globales**
│       └── initial_ftp_settings.csv                                     # **Datos Iniciales para Configuraciones FTP**
│
├── /Scripts
│   ├── /Deployment
│   │   ├── deploy_arm_templates.sh
│   │   ├── deploy_notebooks.sh
│   │   └── configure_clusters.sh
│   └── /Automation
│       ├── schedule_jobs.py
│       ├── monitor_pipeline.py
│       └── load_to_ftp.py                                               # **Script de Carga a FTP (Python)**
│
├── /Documentation
│   ├── README.md
│   ├── Deployment_Guide.md
│   ├── Best_Practices.md
│   ├── Architecture_Diagram.png
│   ├── WIDOT_Process_Guide.md                                        # **Documentación Específica para WIDOT**
│   └── Medallion_Data_Guide.md
│
└── /Tests
    ├── /UnitTests
    │   ├── test_ingest_raw_data.py
    │   ├── test_ingest_config_by_state.py
    │   ├── test_clean_data_wisconsin.sql
    │   ├── test_transform_data_wisconsin.sql
    │   ├── test_transform_function.sql
    │   └── test_categorize_function.sql
    └── /IntegrationTests
        ├── test_etl_pipeline.py
        └── test_load_to_ftp.py

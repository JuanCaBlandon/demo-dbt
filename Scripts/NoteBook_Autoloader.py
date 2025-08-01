from pyspark.sql.functions import col, regexp_extract, current_timestamp
import re

def clean_column_name(name):
    return re.sub(r'[^a-zA-Z0-9_]', '_', name.strip()).lower()

# Define paths for both detail and header files
path_detail = "/Volumes/laboratory_dev/bronze/sales/SalesOrderDetail*.csv" # Path específico para detail
path_header = "/Volumes/laboratory_dev/bronze/sales/SalesOrderHeader*.csv" # Path específico para header

# Define schema and checkpoint locations for detail
schema_loc_detail = "/Volumes/laboratory_dev/bronze/sales/_schema/detail4"
checkpoint_detail = "/Volumes/laboratory_dev/bronze/sales/_checkpoints/detail4"

# Define schema and checkpoint locations for header
schema_loc_header = "/Volumes/laboratory_dev/bronze/sales/_schema/header4"
checkpoint_header = "/Volumes/laboratory_dev/bronze/sales/_checkpoints/header4"

# ---------- DETAIL ----------
# Infiero el esquema solo de los archivos SalesOrderDetail
schema_detail = (spark.read
                 .option("header", "true")
                 .option("sep", "\t")
                 .csv(path_detail) # Leo solo los archivos detail para inferir el esquema
                 .schema)

df_detail = (spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("header", "true")
             .option("sep", "\t")
             .option("cloudFiles.schemaLocation", schema_loc_detail)
             .schema(schema_detail) # Uso el esquema inferido para detail
             .load(path_detail)) # <-- Cargo solo los archivos detail

df_detail = df_detail.toDF(*[clean_column_name(c) for c in df_detail.columns])
df_detail = df_detail.withColumn("fecha_archivo", regexp_extract(col("_metadata.file_path"), r'_(\d{8})', 1))
df_detail = df_detail.withColumn("process_date", current_timestamp())
df_detail = df_detail.withColumn("file_name", col("_metadata.file_path"))

(df_detail.writeStream
   .format("delta")
   .option("checkpointLocation", checkpoint_detail)
   .option("mergeSchema", "true")
   .outputMode("append")
   .partitionBy("fecha_archivo")
   .trigger(availableNow=True)
   .toTable("laboratory_dev.bronze.sales_order_detail_raw")) # Tabla para detail

# --- Horizontal Rule for Clarity ---

# ---------- HEADER ----------
# Infiero el esquema solo de los archivos SalesOrderHeader
schema_header = (spark.read
                 .option("header", "true")
                 .option("sep", "\t")
                 .csv(path_header) # Leo solo los archivos header para inferir el esquema
                 .schema)

df_header = (spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("header", "true")
             .option("sep", "\t")
             .option("cloudFiles.schemaLocation", schema_loc_header)
             .schema(schema_header) # Uso el esquema inferido para header
             .load(path_header)) # <-- Cargo solo los archivos header

df_header = df_header.toDF(*[clean_column_name(c) for c in df_header.columns])
df_header = df_header.withColumn("fecha_archivo", regexp_extract(col("_metadata.file_path"), r'_(\d{8})', 1))
df_header = df_header.withColumn("process_date", current_timestamp())
df_header = df_header.withColumn("file_name", col("_metadata.file_path"))

(df_header.writeStream
   .format("delta")
   .option("checkpointLocation", checkpoint_header)
   .option("mergeSchema", "true")
   .outputMode("append")
   .partitionBy("fecha_archivo")
   .trigger(availableNow=True)
   .toTable("laboratory_dev.bronze.sales_order_header_raw")) # <--- ¡Cambiado a su propia tabla!
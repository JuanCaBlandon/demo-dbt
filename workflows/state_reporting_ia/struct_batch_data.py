print("test reconnect repo")
# Databricks notebook source
from pyspark.sql.functions import col, length, substring, regexp_replace, split, when, regexp_extract, current_timestamp, to_timestamp


file_path = "/FileStore/Intoxalock_20241218.txt"

df = spark.read.text(file_path)

#Remove spaces and separate by comma when possible
df = df.select(
    regexp_replace(col("value"), r"\s{3,}", "  ").alias("value")
)
df = df.select(
    regexp_replace(col("value"), r"\s{2,}", ",").alias("value")
)


#Vendor Name and License Number
part_1 = df.select(
    substring(col("value"), 1, 14).alias("vendor_name"),
    substring(col("value"), 16, 9).alias("DriversLicenseNumber"),
    substring(col("value"), 26, 1000).alias("rest"),
)


#First, Middle and Last Name
part_2 = part_1.select(
    col("vendor_name"),
    col("DriversLicenseNumber"),
    split(col("rest"), ",", 2).getItem(0).alias("LastName"),
    split(col("rest"), ",", 3).getItem(1).alias("FirstName"),
    split(col("rest"), ",", 3).getItem(2).alias("remaining_rest")
).withColumn(
    "MiddleName",
    when(
        regexp_extract(col("remaining_rest"), r"^[A-Za-z]+", 0) != "",
        split(col("remaining_rest"), ",").getItem(0)
    ).otherwise(None)
).withColumn(
    "rest",
    when(
        regexp_extract(col("remaining_rest"), r"^[A-Za-z]+", 0) != "",
        split(col("remaining_rest"), ",",2).getItem(1)
    ).otherwise(col("remaining_rest"))
).drop("remaining_rest")


#DOB, VIN
part_3 = part_2.select(
    col("vendor_name"),
    col("DriversLicenseNumber"),
    col("LastName"),
    col("FirstName"),
    col("MiddleName"),
    substring(col("rest"), 1, 10).alias("DateOfBirth"),
    substring(col("rest"), 11, 10000).alias("remaining_rest"),
).withColumn(
    "VIN",
    split(col("remaining_rest"), ",", 2).getItem(0)
).withColumn(
    "rest",
    split(col("remaining_rest"), ",", 2).getItem(1)
)


#Offense Date, Repeat Offender,
part_4 = part_3.select(
    col("vendor_name"),
    col("DriversLicenseNumber"),
    col("LastName"),
    col("FirstName"),
    col("MiddleName"),
    col("DateOfBirth"),
    col("VIN"),
    substring(col("rest"), 1, 10).alias("offense_date"),
    substring(col("rest"), 11, 1).alias("repeat_offender").cast("int"),
    substring(col("rest"), 12, 10000).alias("remaining_rest")
).withColumn(
    "rest",
    regexp_replace(col("remaining_rest"), r"^,", "")
).drop("remaining_rest")

part_4 = part_4.withColumn("offense_date", to_timestamp(col("offense_date"), "yyyy-MM-dd"))


#IID start and end Date
part_5 = part_4.select(
    col("vendor_name"),
    col("DriversLicenseNumber"),
    col("LastName"),
    col("FirstName"),
    col("MiddleName"),
    col("DateOfBirth"),
    col("VIN"),
    col("offense_date"),
    col("repeat_offender"),
    col("rest")
).withColumn(
    "IID_Start_Date",
    when(
        length(col("rest")) > 10,
        substring(col("rest"), 1, 10)
    ).otherwise(None)
).withColumn(
    "IID_End_Date",
    when(
        length(col("rest")) > 10,
        substring(col("rest"), 11, 10)
    ).otherwise(col("rest"))
).drop("rest")


#Created At
part_5 = part_5.withColumn("created_at", current_timestamp())


#Write to delta table
part_5.write.format("delta").mode("append").saveAsTable("state_reporting_dev.bronze.state_batch_customer_data_ia_test")

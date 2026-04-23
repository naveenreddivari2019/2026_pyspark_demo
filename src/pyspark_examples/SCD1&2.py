"""
Slowly Changing Dimensions (SCD) Type 1 & Type 2 using Delta Lake

SCD Type 1: Overwrite changes (no history)
SCD Type 2: Track history with start_date, end_date, is_current flags
"""

import os
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, lit
from delta.tables import DeltaTable
import tempfile
import shutil

# Create Spark session with Delta Lake configuration
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("SCD Type 1 and 2 Example") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[1]")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create temporary directories for Delta tables
scd1_path = tempfile.mkdtemp(prefix="scd1_")
scd2_path = tempfile.mkdtemp(prefix="scd2_")

print("=" * 80)
print("SCD TYPE 1: Overwrite Changes (No History)")
print("=" * 80)

# Initial customer data
initial_data = [
    (1, "Alice", "New York"),
    (2, "Bob", "Los Angeles"),
    (3, "Charlie", "Chicago")
]
initial_df = spark.createDataFrame(initial_data, ["customer_id", "name", "city"])

print("\n1. Initial Data:")
initial_df.show()

# Write initial data as Delta table
initial_df.write.format("delta").mode("overwrite").save(scd1_path)

# Updated customer data (Alice moved, Bob's name typo fixed)
updated_data = [
    (1, "Alice", "Boston"),      # City changed
    (2, "Robert", "Los Angeles"), # Name changed
    (3, "Charlie", "Chicago"),    # No change
    (4, "Diana", "Seattle")       # New customer
]
source_df = spark.createDataFrame(updated_data, ["customer_id", "name", "city"])

print("\n2. New/Updated Data:")
source_df.show()

# SCD Type 1: Merge with overwrite
target = DeltaTable.forPath(spark, scd1_path)

target.alias("t").merge(
    source_df.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedUpdate(
    set={
        "name": "s.name",
        "city": "s.city"
    }
).whenNotMatchedInsert(
    values={
        "customer_id": "s.customer_id",
        "name": "s.name",
        "city": "s.city"
    }
).execute()

print("\n3. After SCD Type 1 Merge (History Overwritten):")
spark.read.format("delta").load(scd1_path).orderBy("customer_id").show()

print("\n" + "=" * 80)
print("SCD TYPE 2: Track History with Versioning")
print("=" * 80)

# Initial data with SCD2 columns using proper schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, BooleanType
from datetime import date

scd2_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("city", StringType(), False),
    StructField("start_date", DateType(), False),
    StructField("end_date", DateType(), True),
    StructField("is_current", BooleanType(), False)
])

initial_data_scd2 = [
    (1, "Alice", "New York", date(2024, 1, 1), None, True),
    (2, "Bob", "Los Angeles", date(2024, 1, 1), None, True),
    (3, "Charlie", "Chicago", date(2024, 1, 1), None, True)
]
initial_df_scd2 = spark.createDataFrame(initial_data_scd2, scd2_schema)

print("\n1. Initial Data with SCD2 columns:")
initial_df_scd2.show()

# Write initial data
initial_df_scd2.write.format("delta").mode("overwrite").save(scd2_path)

# New updates
updated_data_scd2 = [
    (1, "Alice", "Boston"),       # City changed
    (2, "Robert", "Los Angeles"),  # Name changed
    (3, "Charlie", "Chicago"),     # No change
    (4, "Diana", "Seattle")        # New customer
]
source_df_scd2 = spark.createDataFrame(updated_data_scd2, ["customer_id", "name", "city"])

print("\n2. New/Updated Data:")
source_df_scd2.show()

# SCD Type 2: Expire old records and insert new versions
target_scd2 = DeltaTable.forPath(spark, scd2_path)

# Step 1: Expire old records that have changes
target_scd2.alias("t").merge(
    source_df_scd2.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true"
).whenMatchedUpdate(
    condition="t.name <> s.name OR t.city <> s.city",
    set={
        "end_date": "current_date()",
        "is_current": "false"
    }
).execute()

# Step 2: Insert new versions for changed records and new records
new_records_df = source_df_scd2.withColumn("start_date", current_date()) \
    .withColumn("end_date", lit(None).cast("date")) \
    .withColumn("is_current", lit(True))

target_scd2.alias("t").merge(
    new_records_df.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true"
).whenNotMatchedInsert(
    values={
        "customer_id": "s.customer_id",
        "name": "s.name",
        "city": "s.city",
        "start_date": "s.start_date",
        "end_date": "s.end_date",
        "is_current": "s.is_current"
    }
).execute()

print("\n3. After SCD Type 2 Merge (History Preserved):")
result_df = spark.read.format("delta").load(scd2_path) \
    .orderBy("customer_id", col("start_date").desc())
result_df.show(truncate=False)

print("\n4. Current Records Only (is_current = true):")
result_df.filter("is_current = true").show(truncate=False)

print("\n5. Historical Records (is_current = false):")
result_df.filter("is_current = false").show(truncate=False)

# Cleanup
spark.stop()
shutil.rmtree(scd1_path, ignore_errors=True)
shutil.rmtree(scd2_path, ignore_errors=True)

print("\nDemo completed successfully!")

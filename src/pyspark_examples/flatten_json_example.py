from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json, schema_of_json
import os
import json

# Set environment variables to force localhost
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Create SparkSession
spark = SparkSession.builder \
    .appName("FlattenJSONExample") \
    .master("local[1]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.shuffle.service.enabled", "false") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

# Read the entire JSON file as text
json_path = "/Users/naveenkumarreddyreddivari/Git_2026_personal/2026_pyspark_demo/2026_pyspark_demo/src/data1.json"

# Read entire file content
with open(json_path, 'r') as f:
    json_data = json.load(f)

# Create DataFrame from the parsed JSON
df = spark.createDataFrame([json_data])

print("Original nested structure:")
df.show(truncate=False)
df.printSchema()

# Flatten address nested object
df_flattened = df.select(
    col("id"),
    col("name"),
    col("address.city").alias("city"),
    col("address.zip").alias("zip"),
    col("contacts")
)

print("\nFlattened address:")
df_flattened.show(truncate=False)

# Explode contacts array
df_contacts_exploded = df_flattened.select(
    col("id"),
    col("name"),
    col("city"),
    col("zip"),
    explode(col("contacts")).alias("contact")
)

# Further flatten contact details
df_fully_flattened = df_contacts_exploded.select(
    col("id"),
    col("name"),
    col("city"),
    col("zip"),
    col("contact.type").alias("contact_type"),
    col("contact.value").alias("contact_value")
)

print("\nFully flattened data:")
df_fully_flattened.show(truncate=False)

df_fully_flattened.write.mode("overwrite").csv("/Users/naveenkumarreddyreddivari/Git_2026_personal/2026_pyspark_demo/2026_pyspark_demo/src/flattened_output")

spark.stop()
import os
from pyspark.sql import SparkSession

# Set environment variables to force localhost
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Create a SparkSession with Delta Lake support in local mode
spark = SparkSession.builder \
    .appName("Pivot Example") \
    .master("local[1]") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.shuffle.service.enabled", "false") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

# Sample data
data = [
    ("Banana", 1500, "USA"),
    ("Carrots", 1500, "USA"),
    ("Beans", 1600, "USA"),
    ("Orange", 2000, "USA"),
    ("Orange", 2000, "USA"),
    ("Banana", 400, "China"),
    ("Carrots", 1200, "China"),
    ("Beans", 1500, "China"),
    ("Orange", 4000, "China"),
    ("Banana", 2000, "Canada"),
    ("Carrots", 2000, "Canada"),
    ("Beans", 2000, "Mexico"),
]

# Define the schema
columns = ["Product", "Amount", "Country"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

df.show()

# Perform pivot operation
pivot_df = df.groupBy("Product").pivot("Country").sum("Amount")

# Show the result
pivot_df.show()

pivot_df.write.format("delta").mode("overwrite").save("/Users/naveenkumarreddyreddivari/Git_2026_personal/2026_pyspark_demo/2026_pyspark_demo/src/delta_tables/pivot_table")
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number,dense_rank,rank

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Second Highest Sales") \
    .getOrCreate()

# Sample data
data = [
    ("Banana", 1000, "USA"),
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

# Define a window specification
window_spec = Window.partitionBy("Product").orderBy(col("Amount").desc())

# Add a row number column to rank sales for each product
ranked_df = df.withColumn("rank", dense_rank().over(window_spec))

# Filter for the second highest sales (rank = 2)
second_highest_sales_df = ranked_df.filter(col("rank") == 2).select("Product", "Amount", "Country")

# Show the result
second_highest_sales_df.show()

import time
time.sleep(600)

print("Done")
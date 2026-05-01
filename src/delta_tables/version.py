import os
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

# Set environment variables to force localhost
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Create SparkSession with Delta Lake support
spark = SparkSession.builder \
    .appName("DeltaTableVersionExample") \
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

table_path = "/Users/naveenkumarreddyreddivari/Git_2026_personal/2026_pyspark_demo/2026_pyspark_demo/src/delta_tables/pivot_table"
d_table= DeltaTable.forPath(spark, table_path)
d_table.history().show(truncate=False)
df = d_table.toDF()
df.show(truncate=False)

# Show specific version of data
version_number = 0
df_version = spark.read.format("delta").option("versionAsOf", version_number).load(table_path)
print(f"Data at version {version_number}:")
df_version.show(truncate=False)

# Show specific timestamp of data
timestamp = "2026-04-24 15:25:31.709"
df_timestamp = spark.read.format("delta").option("timestampAsOf", timestamp).load(table_path)
print(f"Data at timestamp {timestamp}:")
df_timestamp.show(truncate=False)

#restore table to version 0
restoreversion=3
d_table.restoreToVersion(restoreversion)
# Show data after restore
print("Data after restore to version 0:")
df_restored = d_table.toDF()
df_restored.show(truncate=False)
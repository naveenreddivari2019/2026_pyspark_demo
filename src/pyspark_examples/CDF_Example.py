"""
Change Data Feed (CDF) Example in Delta Lake

This example demonstrates how to:
1. Enable CDF on a Delta table
2. Track INSERT, UPDATE, and DELETE operations
3. Read change data using different methods (version, timestamp, streaming)
4. Query specific change types and versions
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from delta import configure_spark_with_delta_pip
import shutil
import os

# Configure Spark with Delta Lake
builder = SparkSession.builder \
    .appName("CDF_Delta_Example") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Define table path
delta_table_path = "/tmp/delta_cdf_example"

# Clean up existing table
if os.path.exists(delta_table_path):
    shutil.rmtree(delta_table_path)

print("=" * 80)
print("STEP 1: Create Initial Delta Table with CDF Enabled")
print("=" * 80)

# Create initial data
initial_data = [
    (1, "Alice", "Engineering", 75000),
    (2, "Bob", "Sales", 65000),
    (3, "Charlie", "Engineering", 80000),
    (4, "Diana", "HR", 60000)
]

df_initial = spark.createDataFrame(initial_data, ["id", "name", "department", "salary"])

# Write to Delta with CDF enabled
df_initial.write.format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .save(delta_table_path)

print("\nInitial data (Version 0):")
spark.read.format("delta").load(delta_table_path).show()

print("\n" + "=" * 80)
print("STEP 2: Perform UPDATE Operations")
print("=" * 80)

# Update salaries for Engineering department (Version 1)
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, delta_table_path)

delta_table.update(
    condition="department = 'Engineering'",
    set={"salary": "salary * 1.10"}
)

print("\nAfter UPDATE (Version 1) - Engineering salaries increased by 10%:")
spark.read.format("delta").load(delta_table_path).show()

print("\n" + "=" * 80)
print("STEP 3: Perform INSERT Operations")
print("=" * 80)

# Insert new employees (Version 2)
new_employees = [
    (5, "Eve", "Sales", 70000),
    (6, "Frank", "Engineering", 85000)
]

df_new = spark.createDataFrame(new_employees, ["id", "name", "department", "salary"])
df_new.write.format("delta").mode("append").save(delta_table_path)

print("\nAfter INSERT (Version 2) - Added 2 new employees:")
spark.read.format("delta").load(delta_table_path).show()

print("\n" + "=" * 80)
print("STEP 4: Perform DELETE Operations")
print("=" * 80)

# Delete HR department (Version 3)
delta_table.delete("department = 'HR'")

print("\nAfter DELETE (Version 3) - Removed HR department:")
spark.read.format("delta").load(delta_table_path).show()

print("\n" + "=" * 80)
print("STEP 5: Read Change Data Feed - All Changes")
print("=" * 80)

# Read all changes from version 0 to latest
cdf_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .load(delta_table_path)

print("\nAll changes captured by CDF:")
print("Columns: _change_type (insert/update_preimage/update_postimage/delete), _commit_version, _commit_timestamp")
cdf_df.orderBy("_commit_version", "_change_type", "id").show(50, truncate=False)

print("\n" + "=" * 80)
print("STEP 6: Query Specific Change Types")
print("=" * 80)

# Show only INSERT operations
print("\nOnly INSERT operations:")
cdf_df.filter(col("_change_type") == "insert").show(truncate=False)

# Show only UPDATE operations (pre and post images)
print("\nOnly UPDATE operations (preimage and postimage):")
cdf_df.filter(col("_change_type").contains("update")).show(truncate=False)

# Show only DELETE operations
print("\nOnly DELETE operations:")
cdf_df.filter(col("_change_type") == "delete").show(truncate=False)

print("\n" + "=" * 80)
print("STEP 7: Read Changes Between Specific Versions")
print("=" * 80)

# Read changes from version 1 to version 2
cdf_v1_v2 = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 1) \
    .option("endingVersion", 2) \
    .load(delta_table_path)

print("\nChanges between version 1 and 2:")
cdf_v1_v2.orderBy("_commit_version", "id").show(truncate=False)

print("\n" + "=" * 80)
print("STEP 8: Show Table History")
print("=" * 80)

history_df = delta_table.history()
print("\nDelta Table History:")
history_df.select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

print("\n" + "=" * 80)
print("STEP 9: CDF for Incremental Processing Pattern")
print("=" * 80)

print("\nCommon pattern: Process only new changes since last checkpoint")
print("Example: Get changes from version 2 onwards (for incremental ETL)")

latest_changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 2) \
    .load(delta_table_path)

print("\nLatest changes (from version 2):")
latest_changes.orderBy("_commit_version", "id").show(truncate=False)

print("\n" + "=" * 80)
print("STEP 10: CDF Schema Information")
print("=" * 80)

print("\nCDF DataFrame Schema:")
cdf_df.printSchema()

print("\n" + "=" * 80)
print("Key CDF Columns Explained:")
print("=" * 80)
print("""
_change_type: Type of change operation
  - 'insert': New row added
  - 'update_preimage': Row state BEFORE update
  - 'update_postimage': Row state AFTER update
  - 'delete': Row was deleted

_commit_version: Delta table version when change occurred
_commit_timestamp: Timestamp when change was committed

Original columns (id, name, department, salary): The actual data
""")

print("\n" + "=" * 80)
print("STEP 11: Business Use Cases")
print("=" * 80)

print("\n1. Audit Trail - Track all salary changes:")
salary_changes = cdf_df.filter(
    (col("_change_type").contains("update")) &
    (col("department") == "Engineering")
).select("id", "name", "_change_type", "salary", "_commit_version", "_commit_timestamp")
salary_changes.show(truncate=False)

print("\n2. CDC Pattern - Get net changes (latest state per ID):")
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("id").orderBy(col("_commit_version").desc())
net_changes = cdf_df.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn") \
    .select("id", "name", "department", "salary", "_change_type", "_commit_version")

print("Net changes (one row per ID showing final state):")
net_changes.show(truncate=False)

print("\n" + "=" * 80)
print("CDF Example Complete!")
print("=" * 80)

# Clean up
spark.stop()

"""
Change Data Capture (CDC) in PySpark

CDC tracks changes (inserts, updates, deletes) in source data and applies them to target.

Demonstrates:
1. CDC basics - Insert, Update, Delete operations
2. Merge/Upsert operations
3. Timestamp-based CDC
4. Full vs Incremental load
5. Delta Lake merge operations for CDC
6. CDC with change tracking columns
7. Slowly Changing Dimension Type 2 with CDC
"""

import os
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, max as max_func, coalesce
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType, BooleanType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from datetime import datetime, date
import tempfile
import shutil

# Create Spark session with Delta Lake
builder = SparkSession.builder \
    .appName("CDC - Change Data Capture") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[1]")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create temporary directories
target_path = tempfile.mkdtemp(prefix="cdc_target_")
cdc_log_path = tempfile.mkdtemp(prefix="cdc_log_")
scd2_path = tempfile.mkdtemp(prefix="cdc_scd2_")

print("=" * 80)
print("CHANGE DATA CAPTURE (CDC) IN PYSPARK")
print("=" * 80)

# ============================================================================
# 1. CDC Basics - Understanding Change Operations
# ============================================================================
print("\n1. CDC BASICS - Understanding Change Types")
print("-" * 80)

print("""
CDC Operation Types:
- INSERT (I): New records added to source
- UPDATE (U): Existing records modified in source
- DELETE (D): Records removed from source

CDC Approaches:
1. Timestamp-based: Track last_modified_time
2. Version-based: Track version numbers
3. Log-based: Read database transaction logs
4. Trigger-based: Database triggers capture changes
""")

# ============================================================================
# 2. Initial Load - Setting up Target Table
# ============================================================================
print("\n2. INITIAL LOAD - Setting up Target Table")
print("-" * 80)

# Initial customer data
initial_data = [
    (1, "Alice Johnson", "alice@email.com", "New York", "2024-01-01 10:00:00"),
    (2, "Bob Smith", "bob@email.com", "Los Angeles", "2024-01-01 10:00:00"),
    (3, "Charlie Brown", "charlie@email.com", "Chicago", "2024-01-01 10:00:00"),
    (4, "Diana Prince", "diana@email.com", "Seattle", "2024-01-01 10:00:00"),
    (5, "Eve Davis", "eve@email.com", "Boston", "2024-01-01 10:00:00")
]

schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("last_updated", StringType(), True)
])

initial_df = spark.createDataFrame(initial_data, schema)
initial_df = initial_df.withColumn("last_updated", col("last_updated").cast(TimestampType()))

print("\nInitial data loaded into target:")
initial_df.show()

# Write to Delta table
initial_df.write.format("delta").mode("overwrite").save(target_path)

print(f"Target table created at: {target_path}")

# ============================================================================
# 3. CDC with Change Data - Incremental Updates
# ============================================================================
print("\n3. CDC INCREMENTAL UPDATES - Processing Change Data")
print("-" * 80)

# CDC change data with operation types
cdc_changes = [
    (2, "Robert Smith", "robert@email.com", "Los Angeles", "2024-01-02 11:00:00", "U"),  # Update
    (3, None, None, None, "2024-01-02 11:00:00", "D"),  # Delete
    (6, "Frank Wilson", "frank@email.com", "Miami", "2024-01-02 11:00:00", "I"),  # Insert
    (7, "Grace Lee", "grace@email.com", "Austin", "2024-01-02 11:00:00", "I"),  # Insert
    (1, "Alice Johnson", "alice.new@email.com", "New York", "2024-01-02 11:30:00", "U")  # Update
]

cdc_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("operation", StringType(), False)
])

cdc_df = spark.createDataFrame(cdc_changes, cdc_schema)
cdc_df = cdc_df.withColumn("last_updated", col("last_updated").cast(TimestampType()))

print("\nCDC Change Data (I=Insert, U=Update, D=Delete):")
cdc_df.show(truncate=False)

# ============================================================================
# 4. Apply CDC Changes using Delta Merge
# ============================================================================
print("\n4. APPLYING CDC CHANGES - Delta Merge Operation")
print("-" * 80)

# Load target table
target_table = DeltaTable.forPath(spark, target_path)

# Separate deletes from inserts/updates
deletes_df = cdc_df.filter(col("operation") == "D")
upserts_df = cdc_df.filter(col("operation").isin(["I", "U"]))

print("\nStep 1: Processing DELETES")
if deletes_df.count() > 0:
    print(f"Deleting {deletes_df.count()} record(s)...")
    deletes_df.show()

    # Delete matching records
    target_table.alias("target").merge(
        deletes_df.alias("deletes"),
        "target.customer_id = deletes.customer_id"
    ).whenMatchedDelete().execute()

print("\nStep 2: Processing INSERTS and UPDATES")
if upserts_df.count() > 0:
    print(f"Upserting {upserts_df.count()} record(s)...")
    upserts_df.drop("operation").show()

    # Merge (upsert) operation
    target_table.alias("target").merge(
        upserts_df.drop("operation").alias("updates"),
        "target.customer_id = updates.customer_id"
    ).whenMatchedUpdate(
        set={
            "name": "updates.name",
            "email": "updates.email",
            "city": "updates.city",
            "last_updated": "updates.last_updated"
        }
    ).whenNotMatchedInsert(
        values={
            "customer_id": "updates.customer_id",
            "name": "updates.name",
            "email": "updates.email",
            "city": "updates.city",
            "last_updated": "updates.last_updated"
        }
    ).execute()

print("\nTarget table after CDC changes applied:")
result_df = spark.read.format("delta").load(target_path)
result_df.orderBy("customer_id").show(truncate=False)

print(f"\nSummary: Original={initial_df.count()}, After CDC={result_df.count()}")

# ============================================================================
# 5. Timestamp-based CDC - Incremental Load
# ============================================================================
print("\n5. TIMESTAMP-BASED CDC - Incremental Processing")
print("-" * 80)

# Create source table with timestamps
source_data = [
    (1, "Product A", 100.0, "2024-01-01 08:00:00"),
    (2, "Product B", 200.0, "2024-01-01 08:00:00"),
    (3, "Product C", 150.0, "2024-01-01 08:00:00"),
    (4, "Product D", 250.0, "2024-01-01 08:00:00")
]

source_df = spark.createDataFrame(
    source_data,
    ["product_id", "product_name", "price", "modified_time"]
)
source_df = source_df.withColumn("modified_time", col("modified_time").cast(TimestampType()))

print("\nInitial source data:")
source_df.show()

# Track last processed timestamp
last_processed_time = "2024-01-01 08:00:00"
print(f"Last processed timestamp: {last_processed_time}")

# New changes in source
updated_source = [
    (2, "Product B", 220.0, "2024-01-02 09:00:00"),  # Price updated
    (3, "Product C", 160.0, "2024-01-02 10:00:00"),  # Price updated
    (5, "Product E", 300.0, "2024-01-02 11:00:00")   # New product
]

updated_source_df = spark.createDataFrame(
    updated_source,
    ["product_id", "product_name", "price", "modified_time"]
)
updated_source_df = updated_source_df.withColumn("modified_time", col("modified_time").cast(TimestampType()))

print("\nNew/Updated records in source (after last processed time):")
updated_source_df.show()

# Incremental load - only records modified after last_processed_time
incremental_df = updated_source_df.filter(col("modified_time") > last_processed_time)

print(f"\nIncremental records to process: {incremental_df.count()}")
incremental_df.show()

# Update last processed timestamp
new_last_processed = incremental_df.agg(max_func("modified_time")).collect()[0][0]
print(f"New last processed timestamp: {new_last_processed}")

# ============================================================================
# 6. CDC Log Table - Audit Trail
# ============================================================================
print("\n6. CDC LOG TABLE - Maintaining Audit Trail")
print("-" * 80)

# CDC log captures all changes with metadata
cdc_log_data = [
    (1, "customer", 2, "UPDATE", "name changed from 'Bob Smith' to 'Robert Smith'", "2024-01-02 11:00:00"),
    (2, "customer", 2, "UPDATE", "email changed from 'bob@email.com' to 'robert@email.com'", "2024-01-02 11:00:00"),
    (3, "customer", 3, "DELETE", "customer record deleted", "2024-01-02 11:00:00"),
    (4, "customer", 6, "INSERT", "new customer added", "2024-01-02 11:00:00"),
    (5, "customer", 7, "INSERT", "new customer added", "2024-01-02 11:00:00"),
    (6, "customer", 1, "UPDATE", "email changed", "2024-01-02 11:30:00")
]

cdc_log_schema = StructType([
    StructField("log_id", IntegerType(), False),
    StructField("table_name", StringType(), False),
    StructField("record_id", IntegerType(), False),
    StructField("operation", StringType(), False),
    StructField("change_description", StringType(), True),
    StructField("change_timestamp", StringType(), True)
])

cdc_log_df = spark.createDataFrame(cdc_log_data, cdc_log_schema)
cdc_log_df = cdc_log_df.withColumn("change_timestamp", col("change_timestamp").cast(TimestampType()))

print("\nCDC Audit Log:")
cdc_log_df.show(truncate=False)

# Save CDC log
cdc_log_df.write.format("delta").mode("overwrite").save(cdc_log_path)

# Query audit trail
print("\nAudit trail for customer_id = 2:")
cdc_log_df.filter(col("record_id") == 2).show(truncate=False)

print("\nOperation summary:")
cdc_log_df.groupBy("operation").count().show()

# ============================================================================
# 7. CDC with SCD Type 2 - Historical Tracking
# ============================================================================
print("\n7. CDC WITH SCD TYPE 2 - Historical Data Tracking")
print("-" * 80)

# Initial data with SCD2 columns
scd2_initial = [
    (1, "Alice", "alice@email.com", date(2024, 1, 1), None, True, 1),
    (2, "Bob", "bob@email.com", date(2024, 1, 1), None, True, 1),
    (3, "Charlie", "charlie@email.com", date(2024, 1, 1), None, True, 1)
]

scd2_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("effective_date", DateType(), False),
    StructField("end_date", DateType(), True),
    StructField("is_current", BooleanType(), False),
    StructField("version", IntegerType(), False)
])

scd2_df = spark.createDataFrame(scd2_initial, scd2_schema)

print("\nInitial SCD2 table:")
scd2_df.show()

# Write initial data
scd2_df.write.format("delta").mode("overwrite").save(scd2_path)

# CDC changes - Bob's email changed
cdc_change = [
    (2, "Bob", "bob.new@email.com")
]

change_df = spark.createDataFrame(cdc_change, ["customer_id", "name", "email"])

print("\nCDC Change detected - Bob's email changed:")
change_df.show()

# Load SCD2 table
scd2_table = DeltaTable.forPath(spark, scd2_path)

# Step 1: Expire old record
print("\nStep 1: Expiring old version...")
scd2_table.alias("target").merge(
    change_df.alias("updates"),
    "target.customer_id = updates.customer_id AND target.is_current = true"
).whenMatchedUpdate(
    condition="target.name <> updates.name OR target.email <> updates.email",
    set={
        "end_date": "current_date()",
        "is_current": "false"
    }
).execute()

# Step 2: Insert new version
print("Step 2: Inserting new version...")
new_version_df = change_df.withColumn("effective_date", lit(date(2024, 1, 2))) \
    .withColumn("end_date", lit(None).cast(DateType())) \
    .withColumn("is_current", lit(True)) \
    .withColumn("version", lit(2))

scd2_table.alias("target").merge(
    new_version_df.alias("updates"),
    "target.customer_id = updates.customer_id AND target.is_current = true"
).whenNotMatchedInsert(
    values={
        "customer_id": "updates.customer_id",
        "name": "updates.name",
        "email": "updates.email",
        "effective_date": "updates.effective_date",
        "end_date": "updates.end_date",
        "is_current": "updates.is_current",
        "version": "updates.version"
    }
).execute()

print("\nSCD2 table after CDC change (shows history):")
final_scd2 = spark.read.format("delta").load(scd2_path)
final_scd2.orderBy("customer_id", "version").show(truncate=False)

print("\nCurrent records only:")
final_scd2.filter(col("is_current") == True).show(truncate=False)

print("\nHistorical records (Bob's email change history):")
final_scd2.filter(col("customer_id") == 2).show(truncate=False)

# ============================================================================
# 8. CDC Best Practices
# ============================================================================
print("\n8. CDC BEST PRACTICES")
print("-" * 80)
print("""
Best Practices for CDC in PySpark:

1. DELTA LAKE:
   - Use Delta Lake for ACID transactions
   - Supports merge/upsert operations natively
   - Time travel for auditing

2. CHANGE TRACKING:
   - Always track last_modified timestamp
   - Use version numbers for tracking changes
   - Maintain CDC audit log table

3. INCREMENTAL PROCESSING:
   - Process only changed records (timestamp-based)
   - Use checkpoints to track progress
   - Handle late-arriving data

4. OPERATION ORDERING:
   - Process DELETEs first, then UPSERTs
   - Handle duplicate change records
   - Validate data before applying changes

5. PERFORMANCE:
   - Partition target tables appropriately
   - Use merge conditions efficiently
   - Batch CDC operations when possible

6. DATA QUALITY:
   - Validate CDC data before applying
   - Handle NULL values in change data
   - Implement error handling and retries

7. MONITORING:
   - Track CDC processing metrics
   - Monitor lag between source and target
   - Alert on processing failures

8. TESTING:
   - Test with various change scenarios
   - Verify data consistency
   - Test rollback procedures

Common CDC Patterns:
- Full refresh: Replace entire target table
- Incremental merge: Apply only changes
- Append-only: Keep all versions
- SCD Type 2: Maintain history with flags
""")

# ============================================================================
# Summary
# ============================================================================
print("\n9. CDC IMPLEMENTATION SUMMARY")
print("-" * 80)
print(f"""
CDC Operations Demonstrated:
- Initial Load: {initial_df.count()} records loaded
- CDC Changes Applied: {cdc_df.count()} operations (I/U/D)
- Final Record Count: {result_df.count()} records
- Audit Log Entries: {cdc_log_df.count()} changes tracked
- SCD2 History: Maintained version history with effective dates

Key Takeaways:
✓ Use Delta Lake for CDC operations (ACID guarantees)
✓ Separate DELETE operations from INSERT/UPDATE
✓ Track timestamps for incremental processing
✓ Maintain audit logs for compliance
✓ Implement SCD Type 2 for historical tracking
✓ Handle all operation types: INSERT, UPDATE, DELETE
""")

# Cleanup
spark.stop()
shutil.rmtree(target_path, ignore_errors=True)
shutil.rmtree(cdc_log_path, ignore_errors=True)
shutil.rmtree(scd2_path, ignore_errors=True)

print("\nDemo completed successfully!")

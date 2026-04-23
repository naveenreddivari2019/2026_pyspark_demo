"""
Handling Bad Records in PySpark

Demonstrates various approaches to handle malformed/bad records:
1. PERMISSIVE mode - puts corrupt records in _corrupt_record column
2. DROPMALFORMED mode - drops corrupt records
3. FAILFAST mode - throws exception on corrupt records
4. badRecordsPath - saves bad records to a directory
5. Manual validation and filtering
6. Null and empty value handling
7. Data quality reports
"""

import os
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnull
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import tempfile
import shutil

# Create Spark session
spark = SparkSession.builder \
    .appName("Bad Records Handling") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .master("local[1]") \
    .getOrCreate()

# Create temporary directories
input_dir = tempfile.mkdtemp(prefix="input_")
bad_records_dir = tempfile.mkdtemp(prefix="bad_records_")

print("=" * 80)
print("BAD RECORDS HANDLING IN PYSPARK")
print("=" * 80)

# ============================================================================
# 1. JSON with Corrupt Records - PERMISSIVE Mode
# ============================================================================
print("\n1. PERMISSIVE MODE (Default) - Captures corrupt records")
print("-" * 80)

# Create sample JSON data with some bad records
json_data = [
    '{"id": 1, "name": "Alice", "age": 30, "salary": 50000.0}',
    '{"id": 2, "name": "Bob", "age": 25, "salary": 45000.0}',
    '{"id": 3, "name": "Charlie", "age": "thirty-five", "salary": 60000.0}',  # Bad age
    '{"id": 4, "name": "Diana", "salary": 55000.0}',  # Missing age
    'this is not valid json at all',  # Completely corrupt
    '{"id": 5, "name": "Eve", "age": 28, "salary": "fifty thousand"}',  # Bad salary
    '{"id": 6, "name": "Frank", "age": 40, "salary": 70000.0}'
]

# Write to file
json_file = os.path.join(input_dir, "data.json")
with open(json_file, 'w') as f:
    f.write('\n'.join(json_data))

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True),
    StructField("_corrupt_record", StringType(), True)
])

# PERMISSIVE mode - captures corrupt records in _corrupt_record column
df_permissive = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(schema) \
    .json(json_file)

print("\nAll records (including corrupt):")
df_permissive.show(truncate=False)

print("\nValid records only:")
valid_df = df_permissive.filter(col("_corrupt_record").isNull())
valid_df.drop("_corrupt_record").show()

print("\nCorrupt records only:")
# Cache the DataFrame to allow selecting only _corrupt_record column
df_permissive.cache()
corrupt_df = df_permissive.filter(col("_corrupt_record").isNotNull())
corrupt_df.select("id", "name", "_corrupt_record").show(truncate=False)

# Count statistics
total_records = df_permissive.count()
valid_count = valid_df.count()
corrupt_count = corrupt_df.count()
print(f"\nStatistics: Total={total_records}, Valid={valid_count}, Corrupt={corrupt_count}")

# ============================================================================
# 2. DROPMALFORMED Mode
# ============================================================================
print("\n2. DROPMALFORMED MODE - Automatically drops bad records")
print("-" * 80)

df_drop = spark.read \
    .option("mode", "DROPMALFORMED") \
    .schema(schema) \
    .json(json_file)

print("\nOnly valid records are loaded:")
df_drop.show()
print(f"Record count: {df_drop.count()} (out of {len(json_data)} original records)")

# ============================================================================
# 3. badRecordsPath - Save bad records to a directory
# ============================================================================
print("\n3. BADRECORDSPATH - Save bad records to a separate directory")
print("-" * 80)

df_with_bad_path = spark.read \
    .option("badRecordsPath", bad_records_dir) \
    .schema(schema) \
    .json(json_file)

print("\nData loaded (bad records saved separately):")
df_with_bad_path.show()

print(f"\nBad records directory: {bad_records_dir}")
# Check if bad records were saved
bad_record_files = []
for root, dirs, files in os.walk(bad_records_dir):
    for file in files:
        if file.endswith('.json'):
            bad_record_files.append(os.path.join(root, file))

if bad_record_files:
    print(f"Found {len(bad_record_files)} bad record file(s)")
    for f in bad_record_files[:1]:
        print(f"\nSample bad record file content:")
        with open(f, 'r') as bf:
            content = bf.read()
            print(content[:500] if len(content) > 500 else content)
else:
    print("No bad record files found (all records may be valid)")

# ============================================================================
# 4. CSV with Bad Records
# ============================================================================
print("\n4. CSV BAD RECORDS HANDLING")
print("-" * 80)

# Create CSV with bad records
csv_file = os.path.join(input_dir, "data.csv")
csv_data = """id,name,age,salary
1,Alice,30,50000.0
2,Bob,25,45000.0
3,Charlie,invalid_age,60000.0
4,Diana,,55000.0
5,Eve,28,not_a_number
6,Frank,40,70000.0
7,Grace,35,
8,Henry,32,80000.0"""

with open(csv_file, 'w') as f:
    f.write(csv_data)

csv_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True),
    StructField("_corrupt_record", StringType(), True)
])

df_csv = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("header", "true") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(csv_schema) \
    .csv(csv_file)

print("\nCSV data with bad records (nulls where conversion failed):")
df_csv.show(truncate=False)

# ============================================================================
# 5. Manual Validation and Filtering
# ============================================================================
print("\n5. MANUAL VALIDATION - Custom business logic")
print("-" * 80)

# Read without strict schema enforcement
df_raw = spark.read \
    .option("header", "true") \
    .csv(csv_file)

print("\nOriginal data (all columns as strings):")
df_raw.show()

# Add validation columns
df_validated = df_raw \
    .withColumn("id_valid", col("id").cast(IntegerType()).isNotNull()) \
    .withColumn("age_valid",
                when(col("age") == "", False)
                .otherwise(col("age").cast(IntegerType()).isNotNull())) \
    .withColumn("salary_valid",
                when(col("salary") == "", False)
                .otherwise(col("salary").cast(DoubleType()).isNotNull())) \
    .withColumn("is_valid",
                col("id_valid") & col("age_valid") & col("salary_valid"))

print("\nData with validation flags:")
df_validated.show(truncate=False)

print("\nValid records with casted types:")
valid_records = df_validated.filter(col("is_valid") == True) \
    .select(
        col("id").cast(IntegerType()),
        col("name"),
        col("age").cast(IntegerType()),
        col("salary").cast(DoubleType())
    )
valid_records.show()

print("\nInvalid records:")
invalid_records = df_validated.filter(col("is_valid") == False)
invalid_records.select("id", "name", "age", "salary", "id_valid", "age_valid", "salary_valid").show(truncate=False)

# ============================================================================
# 6. Null and Empty Value Handling
# ============================================================================
print("\n6. NULL AND EMPTY VALUE HANDLING")
print("-" * 80)

# Create data with nulls and empty values
data_with_nulls = [
    (1, "Alice", 30, 50000.0),
    (2, "Bob", None, 45000.0),
    (3, "", 35, 60000.0),
    (None, "Diana", 28, 55000.0),
    (5, "Eve", 25, None),
    (6, None, None, None)
]

df_nulls = spark.createDataFrame(data_with_nulls, ["id", "name", "age", "salary"])

print("\nOriginal data with nulls:")
df_nulls.show()

print("\nIdentify records with any null values:")
df_null_check = df_nulls.withColumn(
    "has_null",
    isnull(col("id")) | isnull(col("name")) | isnull(col("age")) | isnull(col("salary"))
)
df_null_check.show()

print("\nReplace nulls with default values:")
df_filled = df_nulls.fillna({
    "id": 0,
    "name": "Unknown",
    "age": 0,
    "salary": 0.0
})
df_filled.show()

print("\nDrop rows with any null:")
df_nulls.dropna(how='any').show()

print("\nDrop rows where all values are null:")
df_nulls.dropna(how='all').show()

print("\nDrop rows where specific columns have nulls:")
df_nulls.dropna(subset=["id", "age"]).show()

# ============================================================================
# 7. Conditional Cleaning Based on Rules
# ============================================================================
print("\n7. CONDITIONAL CLEANING - Business rule validation")
print("-" * 80)

# Create test data
test_data = [
    (1, "Alice", 30, 50000.0),
    (2, "Bob", -5, 45000.0),          # Negative age (invalid)
    (3, "Charlie", 150, 60000.0),     # Age > 120 (invalid)
    (4, "Diana", 28, -10000.0),       # Negative salary (invalid)
    (5, "Eve", 25, 25000.0),
    (6, "", 35, 55000.0),             # Empty name (invalid)
    (7, "Frank", 40, 1000000000.0)    # Unrealistic salary (suspicious)
]

df_test = spark.createDataFrame(test_data, ["id", "name", "age", "salary"])

print("\nOriginal data:")
df_test.show()

# Apply business rules
df_clean = df_test \
    .withColumn("age_valid", (col("age") > 0) & (col("age") < 120)) \
    .withColumn("salary_valid", (col("salary") > 0) & (col("salary") < 10000000)) \
    .withColumn("name_valid", (col("name") != "") & col("name").isNotNull()) \
    .withColumn("is_clean", col("age_valid") & col("salary_valid") & col("name_valid"))

print("\nData with validation rules:")
df_clean.show(truncate=False)

print("\nClean records only:")
df_clean.filter(col("is_clean") == True).select("id", "name", "age", "salary").show()

print("\nDirty records only:")
df_clean.filter(col("is_clean") == False).show(truncate=False)

# ============================================================================
# 8. Try-Catch Pattern for UDFs
# ============================================================================
print("\n8. SAFE UDF WITH ERROR HANDLING")
print("-" * 80)

from pyspark.sql.functions import udf

# UDF with error handling
def safe_division(numerator, denominator):
    try:
        if denominator is None or denominator == 0:
            return None
        return float(numerator) / float(denominator)
    except Exception:
        return None

safe_div_udf = udf(safe_division, DoubleType())

# Create test data
div_data = [
    (10, 2),
    (15, 3),
    (20, 0),    # Division by zero
    (25, None), # Null denominator
    (30, 5)
]

df_div = spark.createDataFrame(div_data, ["numerator", "denominator"])

print("\nSafe division (handles errors gracefully):")
df_result = df_div.withColumn("result", safe_div_udf(col("numerator"), col("denominator")))
df_result.show()

# ============================================================================
# 9. Data Quality Summary Report
# ============================================================================
print("\n9. DATA QUALITY SUMMARY REPORT")
print("-" * 80)

def data_quality_report(df, columns):
    """Generate comprehensive data quality report"""
    print("\n" + "=" * 70)
    print("DATA QUALITY REPORT")
    print("=" * 70)

    total_rows = df.count()
    print(f"\nTotal Rows: {total_rows}\n")

    for column in columns:
        print(f"\nColumn: {column}")
        print("-" * 70)

        null_count = df.filter(col(column).isNull()).count()
        non_null_count = total_rows - null_count
        null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0

        print(f"  Non-null count: {non_null_count} ({100 - null_percentage:.2f}%)")
        print(f"  Null count:     {null_count} ({null_percentage:.2f}%)")

        # For string columns, check for empty strings
        if dict(df.dtypes)[column] == 'string':
            empty_count = df.filter((col(column) == "") | col(column).isNull()).count()
            print(f"  Empty/Null:     {empty_count} ({(empty_count/total_rows*100):.2f}%)")

        # For numeric columns, show min/max if available
        if dict(df.dtypes)[column] in ['int', 'bigint', 'double', 'float']:
            non_null_df = df.filter(col(column).isNotNull())
            if non_null_df.count() > 0:
                from pyspark.sql.functions import min as min_func, max as max_func
                stats = non_null_df.select(
                    min_func(col(column)).alias("min_val"),
                    max_func(col(column)).alias("max_val")
                ).collect()[0]
                print(f"  Min value:      {stats['min_val']}")
                print(f"  Max value:      {stats['max_val']}")

# Generate report for null data
print("\nGenerating quality report for data with nulls:")
data_quality_report(df_nulls, ["id", "name", "age", "salary"])

# ============================================================================
# 10. Handling Duplicates
# ============================================================================
print("\n10. HANDLING DUPLICATE RECORDS")
print("-" * 80)

# Create data with duplicates
dup_data = [
    (1, "Alice", 30),
    (2, "Bob", 25),
    (1, "Alice", 30),    # Exact duplicate
    (3, "Charlie", 35),
    (2, "Bob", 26),      # Duplicate ID but different age
    (4, "Diana", 28)
]

df_dup = spark.createDataFrame(dup_data, ["id", "name", "age"])

print("\nOriginal data with duplicates:")
df_dup.show()

print("\nRemove exact duplicates:")
df_dup.dropDuplicates().show()

print("\nRemove duplicates based on specific columns (keep first):")
df_dup.dropDuplicates(["id"]).show()

print("\nIdentify duplicate IDs:")
from pyspark.sql import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("id").orderBy("age")
df_with_row_num = df_dup.withColumn("row_num", row_number().over(window_spec))

print("\nAll records with row numbers:")
df_with_row_num.show()

print("\nKeep only first occurrence of each ID:")
df_with_row_num.filter(col("row_num") == 1).drop("row_num").show()

# Cleanup
spark.stop()
shutil.rmtree(input_dir, ignore_errors=True)
shutil.rmtree(bad_records_dir, ignore_errors=True)

print("\n" + "=" * 80)
print("DEMO COMPLETED SUCCESSFULLY!")
print("=" * 80)
print("\nKey Takeaways:")
print("- Use PERMISSIVE mode to capture corrupt records in a special column")
print("- Use DROPMALFORMED mode to automatically skip bad records")
print("- Use badRecordsPath to save corrupt records for later analysis")
print("- Implement custom validation logic for business rules")
print("- Handle nulls with fillna(), dropna(), or coalesce()")
print("- Always generate data quality reports before processing")

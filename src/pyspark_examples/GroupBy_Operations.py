from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, max, min, countDistinct, collect_list, collect_set, first, last, stddev, variance
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder.appName("GroupBy Operations").getOrCreate()

# Sample data schema
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("city", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("age", IntegerType(), True)
])

# Sample data
data = [
    (1, "Alice", "Engineering", "NYC", 85000.0, 28),
    (2, "Bob", "Engineering", "NYC", 90000.0, 32),
    (3, "Charlie", "Sales", "LA", 70000.0, 25),
    (4, "David", "Sales", "LA", 75000.0, 29),
    (5, "Eve", "HR", "Chicago", 65000.0, 26),
    (6, "Frank", "Engineering", "SF", 95000.0, 35),
    (7, "Grace", "HR", "Chicago", 68000.0, 27),
    (8, "Henry", "Sales", "NYC", 72000.0, 30),
    (9, "Ivy", "Engineering", "SF", 88000.0, 29),
    (10, "Jack", "Sales", "LA", 71000.0, 24)
]

df = spark.createDataFrame(data, schema)

print("Original DataFrame:")
df.printSchema()
df.show()

# ============================================
# 1. Basic GroupBy with single aggregation
# ============================================
print("\n1. Count employees by department:")
df.groupBy("department").count().show()

# ============================================
# 2. GroupBy with multiple aggregations
# ============================================
print("\n2. Multiple aggregations by department:")
df.groupBy("department").agg(
    count("employee_id").alias("num_employees"),
    avg("salary").alias("avg_salary"),
    max("salary").alias("max_salary"),
    min("salary").alias("min_salary")
).show()

# ============================================
# 3. GroupBy with multiple columns
# ============================================
print("\n3. Group by department and city:")
df.groupBy("department", "city").agg(
    count("*").alias("employee_count"),
    avg("salary").alias("avg_salary")
).orderBy("department", "city").show()

# ============================================
# 4. Using various aggregation functions
# ============================================
print("\n4. Statistical aggregations by department:")
df.groupBy("department").agg(
    sum("salary").alias("total_salary"),
    avg("age").alias("avg_age"),
    stddev("salary").alias("salary_stddev"),
    variance("salary").alias("salary_variance"),
    countDistinct("city").alias("distinct_cities")
).show()

# ============================================
# 5. collect_list and collect_set
# ============================================
print("\n5. Collect employee names by department:")
df.groupBy("department").agg(
    collect_list("name").alias("employee_names"),
    collect_set("city").alias("unique_cities")
).show(truncate=False)

# ============================================
# 6. First and Last values
# ============================================
print("\n6. First and last employee (by ID) per department:")
df.groupBy("department").agg(
    first("name").alias("first_employee"),
    last("name").alias("last_employee"),
    first("salary").alias("first_salary"),
    last("salary").alias("last_salary")
).show()

# ============================================
# 7. GroupBy with filtering (HAVING clause equivalent)
# ============================================
print("\n7. Departments with average salary > 75000:")
df.groupBy("department").agg(
    avg("salary").alias("avg_salary")
).filter(col("avg_salary") > 75000).show()

# ============================================
# 8. GroupBy with orderBy
# ============================================
print("\n8. Departments ordered by total salary (descending):")
df.groupBy("department").agg(
    sum("salary").alias("total_salary"),
    count("*").alias("emp_count")
).orderBy(col("total_salary").desc()).show()

# ============================================
# 9. Multiple groupBy operations (chaining)
# ============================================
print("\n9. City-wise statistics, then filter and sort:")
city_stats = df.groupBy("city").agg(
    count("*").alias("employee_count"),
    avg("salary").alias("avg_salary"),
    max("age").alias("max_age")
).filter(col("employee_count") > 1).orderBy(col("avg_salary").desc())
city_stats.show()

# ============================================
# 10. GroupBy with sum for specific columns
# ============================================
print("\n10. Department-wise total and average salary with formatted output:")
df.groupBy("department").agg(
    sum("salary").alias("total_salary_spent"),
    avg("salary").alias("average_employee_salary"),
    count("employee_id").alias("total_employees")
).select(
    col("department"),
    col("total_employees"),
    col("total_salary_spent").cast("decimal(10,2)"),
    col("average_employee_salary").cast("decimal(10,2)")
).show()

spark.stop()

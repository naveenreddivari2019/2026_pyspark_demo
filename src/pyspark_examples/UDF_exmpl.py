from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, pandas_udf
from pyspark.sql.types import StringType, IntegerType, DoubleType
import os

# Set environment variables to force localhost
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Create SparkSession
spark = SparkSession.builder \
    .appName("UDFExample") \
    .master("local[1]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.shuffle.service.enabled", "false") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

# Sample data
data = [
    (1, "Alice", 25, 50000),
    (2, "Bob", 30, 60000),
    (3, "Charlie", 35, 75000),
    (4, "David", 28, 55000)
]

df = spark.createDataFrame(data, ["id", "name", "age", "salary"])
print("=== Original Data ===")
df.show()

# ─────────────────────────────────────────
# 1. PYTHON UDF (Row-at-a-time execution)
# ─────────────────────────────────────────
def categorize_salary(salary):
    """Categorize salary into Low, Medium, High"""
    if salary < 55000:
        return "Low"
    elif salary < 70000:
        return "Medium"
    else:
        return "High"

# Register UDF
salary_udf = udf(categorize_salary, StringType())

df_with_category = df.withColumn("salary_category", salary_udf(col("salary")))
print("\n=== With Salary Category (Python UDF) ===")
df_with_category.show()

# ─────────────────────────────────────────
# 2. PANDAS UDF (Vectorized - faster)
# ─────────────────────────────────────────
import pandas as pd

@pandas_udf(DoubleType())
def calculate_bonus(salary: pd.Series) -> pd.Series:
    """Calculate 10% bonus on salary"""
    return salary * 0.10

df_with_bonus = df.withColumn("bonus", calculate_bonus(col("salary")))
print("\n=== With Bonus (Pandas UDF) ===")
df_with_bonus.show()

# ─────────────────────────────────────────
# 3. COMPLEX UDF (Multiple logic)
# ─────────────────────────────────────────
def generate_employee_summary(name, age, salary):
    """Generate employee summary string"""
    return f"{name} is {age} years old and earns ${salary:,.2f}"

summary_udf = udf(generate_employee_summary, StringType())

df_with_summary = df.withColumn(
    "summary",
    summary_udf(col("name"), col("age"), col("salary"))
)
print("\n=== With Employee Summary ===")
df_with_summary.show(truncate=False)

# ─────────────────────────────────────────
# 4. UDF with Complex Return Type
# ─────────────────────────────────────────
from pyspark.sql.types import StructType, StructField

def parse_employee_info(name, salary):
    """Return structured data"""
    return {
        "employee_name": name.upper(),
        "annual_salary": salary,
        "monthly_salary": round(salary / 12, 2)
    }

struct_schema = StructType([
    StructField("employee_name", StringType(), True),
    StructField("annual_salary", IntegerType(), True),
    StructField("monthly_salary", DoubleType(), True)
])

info_udf = udf(parse_employee_info, struct_schema)

df_with_info = df.withColumn(
    "employee_info",
    info_udf(col("name"), col("salary"))
)
print("\n=== With Structured Employee Info ===")
df_with_info.select("id", "employee_info.*").show()

# ─────────────────────────────────────────
# 5. SQL Registration of UDF
# ─────────────────────────────────────────
spark.udf.register("categorize_salary_sql", categorize_salary, StringType())

df.createOrReplaceTempView("employees")
result_sql = spark.sql("""
    SELECT id, name, salary, 
           categorize_salary_sql(salary) as salary_category
    FROM employees
""")
print("\n=== Using UDF in SQL ===")
result_sql.show()

spark.stop()
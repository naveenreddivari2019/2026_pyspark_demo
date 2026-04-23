import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, lead, col, round
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Lag_Lead_Example").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ─────────────────────────────────────────
# SAMPLE DATA — Monthly Sales per Employee
# ─────────────────────────────────────────
data = [
    (1, "Alice",   "Engineering", "2024-01", 90000),
    (1, "Alice",   "Engineering", "2024-02", 85000),
    (1, "Alice",   "Engineering", "2024-03", 92000),
    (1, "Alice",   "Engineering", "2024-04", 88000),
    (2, "Bob",     "Finance",     "2024-01", 75000),
    (2, "Bob",     "Finance",     "2024-02", 78000),
    (2, "Bob",     "Finance",     "2024-03", 72000),
    (2, "Bob",     "Finance",     "2024-04", 80000),
    (3, "Charlie", "Marketing",   "2024-01", 70000),
    (3, "Charlie", "Marketing",   "2024-02", 73000),
    (3, "Charlie", "Marketing",   "2024-03", 68000),
    (3, "Charlie", "Marketing",   "2024-04", 75000),
]

columns = ["empid", "empname", "dept", "month", "sal"]

df = spark.createDataFrame(data, columns)

print("=== RAW DATA ===")
df.show()

# ─────────────────────────────────────────
# WINDOW SPEC
# ─────────────────────────────────────────
# PARTITION BY empid → per employee
# ORDER BY month     → chronological order
window_spec = Window.partitionBy("empid").orderBy("month")

# ─────────────────────────────────────────
# PYSPARK — LAG & LEAD EXAMPLES
# ─────────────────────────────────────────

# ─── 1. LAG — Previous month salary ─────
print("=== 1. LAG — Previous Month Salary ===")
df_lag = df.withColumn(
    "prev_month_sal",
    lag("sal", 1).over(window_spec)           # 1 row BEHIND current
)
df_lag.show()

# ─── 2. LEAD — Next month salary ─────────
print("=== 2. LEAD — Next Month Salary ===")
df_lead = df.withColumn(
    "next_month_sal",
    lead("sal", 1).over(window_spec)          # 1 row AHEAD current
)
df_lead.show()

# ─── 3. LAG + LEAD Together ──────────────
print("=== 3. LAG + LEAD Together ===")
df_both = df \
    .withColumn("prev_month_sal", lag("sal",  1).over(window_spec)) \
    .withColumn("next_month_sal", lead("sal", 1).over(window_spec))
df_both.show()

# ─── 4. Salary Change from Previous Month ─
print("=== 4. Salary Change from Previous Month ===")
df_change = df \
    .withColumn("prev_sal", lag("sal", 1).over(window_spec)) \
    .withColumn(
        "sal_change",
        col("sal") - col("prev_sal")          # Current - Previous
    ) \
    .withColumn(
        "pct_change",
        round((col("sal") - col("prev_sal")) / col("prev_sal") * 100, 2)
    )
df_change.show()

# ─── 5. LAG with Default Value ───────────
print("=== 5. LAG with Default Value (0 instead of NULL) ===")
df_lag_default = df.withColumn(
    "prev_month_sal",
    lag("sal", 1, 0).over(window_spec)        # 0 when no previous row
)
df_lag_default.show()

# ─── 6. LAG 2 Rows Back ──────────────────
print("=== 6. LAG 2 Months Back ===")
df_lag2 = df \
    .withColumn("prev_1_sal", lag("sal", 1).over(window_spec)) \
    .withColumn("prev_2_sal", lag("sal", 2).over(window_spec))  # 2 rows BEHIND
df_lag2.show()

# ─── 7. Salary Trend ─────────────────────
print("=== 7. Salary Trend (UP/DOWN/SAME) ===")
from pyspark.sql.functions import when

df_trend = df \
    .withColumn("prev_sal", lag("sal", 1).over(window_spec)) \
    .withColumn(
        "trend",
        when(col("prev_sal").isNull(),          "FIRST MONTH")
        .when(col("sal") > col("prev_sal"),     "📈 UP")
        .when(col("sal") < col("prev_sal"),     "📉 DOWN")
        .otherwise(                             "➡ SAME")
    )
df_trend.select("empname", "month", "sal", "prev_sal", "trend").show()

# ─── 8. Compare with Next month ──────────
print("=== 8. Is Next Month Higher? ===")
df_next = df \
    .withColumn("next_sal", lead("sal", 1).over(window_spec)) \
    .withColumn(
        "next_month_trend",
        when(col("next_sal").isNull(),          "LAST MONTH")
        .when(col("next_sal") > col("sal"),     "📈 WILL INCREASE")
        .when(col("next_sal") < col("sal"),     "📉 WILL DECREASE")
        .otherwise(                             "➡ NO CHANGE")
    )
df_next.select("empname", "month", "sal", "next_sal", "next_month_trend").show()

# ─────────────────────────────────────────
# REGISTER TEMP VIEW FOR SQL EXAMPLES
# ─────────────────────────────────────────
df.createOrReplaceTempView("MonthlySalary")

# ─────────────────────────────────────────
# SQL — LAG & LEAD EXAMPLES
# ─────────────────────────────────────────

# ─── SQL 1. LAG — Previous month salary ──
print("=== SQL 1. LAG — Previous Month Salary ===")
spark.sql("""
    SELECT  empid,
            empname,
            dept,
            month,
            sal,
            LAG(sal, 1) OVER (
                PARTITION BY empid
                ORDER BY month
            ) AS prev_month_sal
    FROM MonthlySalary
""").show()

# ─── SQL 2. LEAD — Next month salary ─────
print("=== SQL 2. LEAD — Next Month Salary ===")
spark.sql("""
    SELECT  empid,
            empname,
            dept,
            month,
            sal,
            LEAD(sal, 1) OVER (
                PARTITION BY empid
                ORDER BY month
            ) AS next_month_sal
    FROM MonthlySalary
""").show()

# ─── SQL 3. LAG + LEAD Together ──────────
print("=== SQL 3. LAG + LEAD Together ===")
spark.sql("""
    SELECT  empname,
            month,
            sal,
            LAG(sal,  1) OVER (PARTITION BY empid ORDER BY month) AS prev_sal,
            LEAD(sal, 1) OVER (PARTITION BY empid ORDER BY month) AS next_sal
    FROM MonthlySalary
""").show()

# ─── SQL 4. Salary Change ─────────────────
print("=== SQL 4. Salary Change from Previous Month ===")
spark.sql("""
    SELECT  empname,
            month,
            sal,
            LAG(sal, 1) OVER (PARTITION BY empid ORDER BY month) AS prev_sal,
            sal - LAG(sal, 1) OVER (
                PARTITION BY empid ORDER BY month
            ) AS sal_change,
            ROUND(
                (sal - LAG(sal, 1) OVER (PARTITION BY empid ORDER BY month))
                / LAG(sal, 1) OVER (PARTITION BY empid ORDER BY month) * 100
            , 2) AS pct_change
    FROM MonthlySalary
""").show()

# ─── SQL 5. LAG with Default Value ───────
print("=== SQL 5. LAG with Default 0 ===")
spark.sql("""
    SELECT  empname,
            month,
            sal,
            LAG(sal, 1, 0) OVER (          -- 0 when no previous row
                PARTITION BY empid
                ORDER BY month
            ) AS prev_sal
    FROM MonthlySalary
""").show()

# ─── SQL 6. Trend UP/DOWN/SAME ───────────
print("=== SQL 6. Salary Trend ===")
spark.sql("""
    SELECT  empname,
            month,
            sal,
            LAG(sal, 1) OVER (PARTITION BY empid ORDER BY month) AS prev_sal,
            CASE
                WHEN LAG(sal,1) OVER (PARTITION BY empid ORDER BY month) IS NULL
                     THEN 'FIRST MONTH'
                WHEN sal > LAG(sal,1) OVER (PARTITION BY empid ORDER BY month)
                     THEN 'UP'
                WHEN sal < LAG(sal,1) OVER (PARTITION BY empid ORDER BY month)
                     THEN 'DOWN'
                ELSE 'SAME'
            END AS trend
    FROM MonthlySalary
""").show()

# ─── SQL 7. CTE with LAG ─────────────────
print("=== SQL 7. CTE with LAG ===")
spark.sql("""
    WITH SalaryWithLag AS (
        SELECT  empname,
                dept,
                month,
                sal,
                LAG(sal, 1)  OVER (PARTITION BY empid ORDER BY month) AS prev_sal,
                LEAD(sal, 1) OVER (PARTITION BY empid ORDER BY month) AS next_sal
        FROM MonthlySalary
    )
    SELECT  empname,
            dept,
            month,
            sal,
            prev_sal,
            next_sal,
            sal - prev_sal AS month_diff
    FROM SalaryWithLag
    ORDER BY empname, month
""").show()

# ─── SQL 8. Dept level LAG ───────────────
print("=== SQL 8. LAG Partitioned by Department ===")
spark.sql("""
    SELECT  empname,
            dept,
            month,
            sal,
            LAG(sal, 1) OVER (
                PARTITION BY dept          -- Per department (not per employee)
                ORDER BY month, empid
            ) AS prev_dept_sal
    FROM MonthlySalary
    ORDER BY dept, month
""").show()

spark.stop()



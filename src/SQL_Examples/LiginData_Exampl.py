import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import date

spark = SparkSession.builder \
    .appName("LoginData_Example") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ─────────────────────────────────────────
# PREPARE DATA — LOGIN Table
# ─────────────────────────────────────────
login_schema = StructType([
    StructField("login_id",   IntegerType(), False),   # PK
    StructField("user_name",  StringType(),  True),
    StructField("login_date", DateType(),    True)
])

login_data = [
    (101, "Michael", date(2021, 8, 21)),
    (102, "James",   date(2021, 8, 21)),
    (103, "Stewart", date(2021, 8, 22)),
    (104, "Stewart", date(2021, 8, 22)),
    (105, "Stewart", date(2021, 8, 22)),
    (106, "Michael", date(2021, 8, 23)),
    (107, "Michael", date(2021, 8, 23)),
    (108, "Stewart", date(2021, 8, 24)),
    (109, "Stewart", date(2021, 8, 24)),
    (110, "James",   date(2021, 8, 25)),
    (111, "James",   date(2021, 8, 25)),
    (112, "James",   date(2021, 8, 26)),
    (113, "James",   date(2021, 8, 27))
]

login_df = spark.createDataFrame(login_data, login_schema)

# Create Temp View
login_df.createOrReplaceTempView("LOGIN")

print("=== LOGIN TABLE ===")
login_df.show(truncate=False)
login_df.printSchema()

# ─────────────────────────────────────────
# Q1. Total Logins per User
# ─────────────────────────────────────────
print("=== Q1. Total Logins per User ===")

# SQL
spark.sql("""
    SELECT user_name,
           COUNT(login_id) AS total_logins
    FROM LOGIN
    GROUP BY user_name
    ORDER BY total_logins DESC
""").show()

# PySpark
login_df.groupBy("user_name") \
    .agg(F.count("login_id").alias("total_logins")) \
    .orderBy(F.col("total_logins").desc()) \
    .show()

# ─────────────────────────────────────────
# Q2. Logins per User per Date
# ─────────────────────────────────────────
print("=== Q2. Logins per User per Date ===")

# SQL
spark.sql("""
    SELECT user_name,
           login_date,
           COUNT(login_id) AS daily_logins
    FROM LOGIN
    GROUP BY user_name, login_date
    ORDER BY login_date, user_name
""").show()

# PySpark
login_df.groupBy("user_name", "login_date") \
    .agg(F.count("login_id").alias("daily_logins")) \
    .orderBy("login_date", "user_name") \
    .show()

# ─────────────────────────────────────────
# Q3. Users who logged in on CONSECUTIVE Days
# ─────────────────────────────────────────
print("=== Q3. SQL — Users with Consecutive Day Logins ===")

spark.sql("""
    WITH daily_logins AS (
        SELECT DISTINCT user_name, login_date       -- Distinct dates per user
        FROM LOGIN
    ),
    with_next_day AS (
        SELECT
            user_name,
            login_date,
            LEAD(login_date) OVER (
                PARTITION BY user_name
                ORDER BY login_date
            ) AS next_login_date                    -- Get next login date
        FROM daily_logins
    )
    SELECT user_name,
           login_date,
           next_login_date
    FROM with_next_day
    WHERE DATEDIFF(next_login_date, login_date) = 1 -- Consecutive = diff of 1 day
    ORDER BY user_name, login_date
""").show()

# PySpark
print("=== Q3. PySpark — Users with Consecutive Day Logins ===")
daily_distinct = login_df.select("user_name", "login_date").distinct()

window_spec = Window.partitionBy("user_name").orderBy("login_date")

consecutive_df = daily_distinct \
    .withColumn("next_login_date",
        F.lead("login_date").over(window_spec)
    ) \
    .filter(
        F.datediff(F.col("next_login_date"), F.col("login_date")) == 1
    ) \
    .orderBy("user_name", "login_date")

consecutive_df.show()

# ─────────────────────────────────────────
# Q4. First & Last Login Date per User
# ─────────────────────────────────────────
print("=== Q4. First & Last Login per User ===")

# SQL
spark.sql("""
    SELECT
        user_name,
        MIN(login_date) AS first_login,
        MAX(login_date) AS last_login,
        DATEDIFF(MAX(login_date), MIN(login_date)) AS active_days
    FROM LOGIN
    GROUP BY user_name
    ORDER BY first_login
""").show()

# PySpark
login_df.groupBy("user_name") \
    .agg(
        F.min("login_date").alias("first_login"),
        F.max("login_date").alias("last_login"),
        F.datediff(
            F.max("login_date"),
            F.min("login_date")
        ).alias("active_days")
    ) \
    .orderBy("first_login") \
    .show()

# ─────────────────────────────────────────
# Q5. Most Active Login Date (Most Users)
# ─────────────────────────────────────────
print("=== Q5. Most Active Login Date ===")

# SQL
spark.sql("""
    SELECT login_date,
           COUNT(DISTINCT user_name) AS unique_users,
           COUNT(login_id)           AS total_logins
    FROM LOGIN
    GROUP BY login_date
    ORDER BY total_logins DESC
""").show()

# PySpark
login_df.groupBy("login_date") \
    .agg(
        F.countDistinct("user_name").alias("unique_users"),
        F.count("login_id").alias("total_logins")
    ) \
    .orderBy(F.col("total_logins").desc()) \
    .show()

# ─────────────────────────────────────────
# Q6. Running Total Logins per User
# ─────────────────────────────────────────
print("=== Q6. Running Total Logins per User ===")

# SQL
spark.sql("""
    SELECT
        user_name,
        login_date,
        COUNT(login_id)  AS daily_logins,
        SUM(COUNT(login_id)) OVER (
            PARTITION BY user_name
            ORDER BY login_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_total
    FROM LOGIN
    GROUP BY user_name, login_date
    ORDER BY user_name, login_date
""").show()

# PySpark
window_running = Window.partitionBy("user_name") \
    .orderBy("login_date") \
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )

login_df.groupBy("user_name", "login_date") \
    .agg(F.count("login_id").alias("daily_logins")) \
    .withColumn("running_total",
        F.sum("daily_logins").over(window_running)
    ) \
    .orderBy("user_name", "login_date") \
    .show()


# ─────────────────────────────────────────
# Q7. Users who logged in 3 or More Times in a Day
# ─────────────────────────────────────────

# ── SQL Approach 1 — Simple GROUP BY + HAVING ────
print("=== Q7a. SQL — Users with 3+ Logins in a Day ===")
spark.sql("""
    SELECT user_name,
           login_date,
           COUNT(login_id) AS daily_logins
    FROM LOGIN
    GROUP BY user_name, login_date
    HAVING COUNT(login_id) >= 3              -- 3 or more logins same day
    ORDER BY login_date, daily_logins DESC
""").show()

print("✅ Done!")
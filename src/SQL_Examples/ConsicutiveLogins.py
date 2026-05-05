from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, row_number, sum as spark_sum, count, when, datediff, first
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import os

# Set environment variables
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Create SparkSession
spark = SparkSession.builder \
    .appName("ConsecutiveLoginsExample") \
    .master("local[1]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.shuffle.service.enabled", "false") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

# ─────────────────────────────────────────
# 1. GENERATE SAMPLE LOGIN DATA
# ─────────────────────────────────────────
login_data = [
    ("user_1", "2026-01-01"),
    ("user_1", "2026-01-02"),
    ("user_1", "2026-01-03"),  # 3 consecutive days
    ("user_1", "2026-01-05"),  # Gap - breaks streak
    ("user_1", "2026-01-06"),
    
    ("user_2", "2026-01-02"),
    ("user_2", "2026-01-03"),  # 2 consecutive days
    ("user_2", "2026-01-04"),
    ("user_2", "2026-01-05"),  # 4 consecutive days
    
    ("user_3", "2026-01-01"),
    ("user_3", "2026-01-03"),  # Gap
    ("user_3", "2026-01-04"),
    ("user_3", "2026-01-05"),  # 3 consecutive days
    ("user_3", "2026-01-06"),
    ("user_3", "2026-01-07"),
    
    ("user_4", "2026-01-01"),  # Single login - no consecutive
    ("user_4", "2026-01-05"),  # Single login - no consecutive
    
    ("user_5", "2026-01-10"),
    ("user_5", "2026-01-11"),
    ("user_5", "2026-01-12"),
    ("user_5", "2026-01-13"),  # 4 consecutive days
]

df = spark.createDataFrame(login_data, ["user_id", "login_date"])
df = df.withColumn("login_date", col("login_date").cast("date")).sort("user_id", "login_date")

print("=== Original Login Data ===")
df.show(truncate=False)

# ─────────────────────────────────────────
# 2. METHOD 1: USING LAG WINDOW FUNCTION (PySpark)
# ─────────────────────────────────────────
print("\n=== Method 1: Using LAG to detect consecutive logins ===")

# Create window spec sorted by user and date
window_spec = Window.partitionBy("user_id").orderBy("login_date")

# Add LAG to get previous login date
df_with_lag = df.withColumn(
    "prev_login_date",
    lag("login_date", 1).over(window_spec)
)

# Calculate days difference
df_with_diff = df_with_lag.withColumn(
    "days_diff",
    when(
        col("prev_login_date").isNull(),
        None
    ).otherwise(
        datediff(col("login_date"), col("prev_login_date"))
    )
)

# Identify consecutive logins (days_diff = 1)
df_consecutive = df_with_diff.withColumn(
    "is_consecutive",
    when(col("days_diff") == 1, 1).otherwise(0)
)

print("Data with consecutive flag:")
df_consecutive.show(truncate=False)

# ─────────────────────────────────────────
# 3. METHOD 2: GROUPING CONSECUTIVE DATES (PySpark) - FIXED
# ─────────────────────────────────────────
print("\n=== Method 2: Group consecutive dates into streaks ===")

# Step 1: Add row number
window_row = Window.partitionBy("user_id").orderBy("login_date")
df_with_rn = df.withColumn(
    "row_num",
    row_number().over(window_row)
)
print("Data with Add row number:")
df_with_rn.show(truncate=False)

# Step 2: Create streak_id by subtracting row_num from a running count
# The key insight: consecutive dates will have the same (row_num - datediff from first date)
df_with_streak = df_with_rn.withColumn(
    "days_from_first",
    datediff(col("login_date"), first("login_date").over(window_row))
).withColumn(
    "streak_id",
    col("row_num") - (col("days_from_first") / 1)
)

print("Data with streak_id:")
df_with_streak.show(truncate=False)

# Step 3: Count consecutive days per streak
from pyspark.sql.functions import min as spark_min, max as spark_max

streaks = df_with_streak.groupBy("user_id", "streak_id").agg(
    count("login_date").alias("consecutive_days"),
    spark_min("login_date").alias("streak_start"),
    spark_max("login_date").alias("streak_end")
).select("user_id", "streak_id", "consecutive_days", "streak_start", "streak_end") \
    .orderBy("user_id", "streak_id")

print("\nConsecutive login streaks:")
streaks.show(truncate=False)

# ─────────────────────────────────────────
# 4. FIND USERS WITH 2+ CONSECUTIVE LOGINS (PySpark)
# ─────────────────────────────────────────
print("\n=== Users with 2+ Consecutive Logins ===")
users_2_plus = streaks.filter(col("consecutive_days") >= 2) \
    .select("user_id", "consecutive_days", "streak_start") \
    .orderBy("user_id", col("consecutive_days").desc())

users_2_plus.show(truncate=False)

# ─────────────────────────────────────────
# 5. FIND USERS WITH 3+ CONSECUTIVE LOGINS (PySpark)
# ─────────────────────────────────────────
print("\n=== Users with 3+ Consecutive Logins ===")
users_3_plus = streaks.filter(col("consecutive_days") >= 3) \
    .select("user_id", "consecutive_days", "streak_start") \
    .orderBy("user_id", col("consecutive_days").desc())

users_3_plus.show(truncate=False)

# ─────────────────────────────────────────
# 6. SQL APPROACH (Using Temporary View)
# ─────────────────────────────────────────
print("\n=== SQL: Consecutive Login Streaks ===")

df.createOrReplaceTempView("login_records")

spark.sql("""
    WITH login_with_rn AS (
        SELECT 
            user_id,
            login_date,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) as rn
        FROM login_records
    ),
    login_with_streak AS (
        SELECT 
            user_id,
            login_date,
            rn,
            rn - ROW_NUMBER() OVER (
                PARTITION BY user_id 
                ORDER BY login_date
            ) as streak_id
        FROM login_with_rn
    )
    SELECT 
        user_id,
        MIN(login_date) as streak_start,
        MAX(login_date) as streak_end,
        COUNT(*) as consecutive_days
    FROM login_with_streak
    GROUP BY user_id, streak_id
    ORDER BY user_id, consecutive_days DESC
""").show(truncate=False)

# ─────────────────────────────────────────
# 7. SQL: Users with 2+ Consecutive Logins
# ─────────────────────────────────────────
print("\n=== SQL: Users with 2+ Consecutive Logins ===")

spark.sql("""
    WITH login_with_rn AS (
        SELECT 
            user_id,
            login_date,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) as rn
        FROM login_records
    ),
    login_with_streak AS (
        SELECT 
            user_id,
            login_date,
            rn,
            rn - ROW_NUMBER() OVER (
                PARTITION BY user_id 
                ORDER BY login_date
            ) as streak_id
        FROM login_with_rn
    ),
    streak_counts AS (
        SELECT 
            user_id,
            MIN(login_date) as streak_start,
            COUNT(*) as consecutive_days
        FROM login_with_streak
        GROUP BY user_id, streak_id
    )
    SELECT 
        user_id,
        streak_start,
        consecutive_days
    FROM streak_counts
    WHERE consecutive_days >= 2
    ORDER BY user_id, consecutive_days DESC
""").show(truncate=False)

# ─────────────────────────────────────────
# 8. SQL: Users with 3+ Consecutive Logins
# ─────────────────────────────────────────
print("\n=== SQL: Users with 3+ Consecutive Logins ===")

spark.sql("""
    WITH login_with_rn AS (
        SELECT 
            user_id,
            login_date,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) as rn
        FROM login_records
    ),
    login_with_streak AS (
        SELECT 
            user_id,
            login_date,
            rn,
            rn - ROW_NUMBER() OVER (
                PARTITION BY user_id 
                ORDER BY login_date
            ) as streak_id
        FROM login_with_rn
    ),
    streak_counts AS (
        SELECT 
            user_id,
            MIN(login_date) as streak_start,
            COUNT(*) as consecutive_days
        FROM login_with_streak
        GROUP BY user_id, streak_id
    )
    SELECT 
        user_id,
        streak_start,
        consecutive_days
    FROM streak_counts
    WHERE consecutive_days >= 3
    ORDER BY user_id, consecutive_days DESC
""").show(truncate=False)

# ─────────────────────────────────────────
# 9. MAX CONSECUTIVE STREAK PER USER
# ─────────────────────────────────────────
print("\n=== Max Consecutive Streak per User ===")

spark.sql("""
    WITH login_with_rn AS (
        SELECT 
            user_id,
            login_date,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) as rn
        FROM login_records
    ),
    login_with_streak AS (
        SELECT 
            user_id,
            login_date,
            rn,
            rn - ROW_NUMBER() OVER (
                PARTITION BY user_id 
                ORDER BY login_date
            ) as streak_id
        FROM login_with_rn
    ),
    streak_counts AS (
        SELECT 
            user_id,
            COUNT(*) as consecutive_days
        FROM login_with_streak
        GROUP BY user_id, streak_id
    )
    SELECT 
        user_id,
        MAX(consecutive_days) as max_consecutive_streak
    FROM streak_counts
    GROUP BY user_id
    ORDER BY max_consecutive_streak DESC
""").show(truncate=False)

spark.stop()
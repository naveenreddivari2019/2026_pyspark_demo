from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Sample input data
data = [
    ("u1", "2026-05-10 10:00:00", "e101", "view", None, "2026-05-10 10:01:00"),
    ("u1", "2026-05-10 10:00:00", "e101", "view", None, "2026-05-10 10:03:00"),
    ("u2", "2026-05-10 11:00:00", "e102", "purchase", 100.0, "2026-05-10 11:02:00"),
    ("u2", "2026-05-10 11:00:00", "e102", "purchase", 100.0, "2026-05-10 11:01:00"),
    ("u3", "2026-05-10 12:00:00", "e103", "click", None, "2026-05-10 12:01:00")
]

columns = [
    "user_id",
    "event_time",
    "event_id",
    "event_type",
    "event_value",
    "ingest_time"
]

df = spark.createDataFrame(data, columns)

# Convert to timestamp
df = df.withColumn(
    "event_time",
    F.to_timestamp("event_time")
).withColumn(
    "ingest_time",
    F.to_timestamp("ingest_time")
)

# Window specification
window_spec = Window.partitionBy("event_id") \
                    .orderBy(F.col("ingest_time").asc())

# Deduplicate
dedup_df = df.withColumn(
    "rn",
    F.row_number().over(window_spec)
).filter(
    F.col("rn") == 1
).drop("rn")

# Show result
dedup_df.show(truncate=False)
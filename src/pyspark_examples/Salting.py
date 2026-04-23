from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random

spark = SparkSession.builder \
    .appName("DataSkewExample") \
    .getOrCreate()

# ─────────────────────────────────────────
# 1. CREATE SKEWED DATA
# ─────────────────────────────────────────
# Simulate a skewed dataset where one key ("A") has 90% of the data

data = []
for i in range(100_000):
    key = "A" if random.random() < 0.9 else random.choice(["B", "C", "D"])
    value = random.randint(1, 100)
    data.append((key, value))

print(f"Total records: {len(data)}")
#print(f"data : {data}")

df = spark.createDataFrame(data, ["key", "value"])

print("=== Key Distribution (Skewed) ===")
df.groupBy("key").count().orderBy(desc("count")).show()

print("=== Partition Row Distribution ===")
df.withColumn("partition_id", spark_partition_id()) \
  .groupBy("partition_id") \
  .count() \
  .orderBy(desc("count")) \
  .show(20)

# ─────────────────────────────────────────
# 2. PROBLEMATIC: NAIVE JOIN (causes skew)
# ─────────────────────────────────────────
# One partition will handle ~90K rows for key "A" — slow & unbalanced

lookup_data = [("A", "Apple"), ("B", "Banana"), ("C", "Cherry"), ("D", "Date")]
lookup_df = spark.createDataFrame(lookup_data, ["key", "fruit"])

print("=== Naive Join (Skewed) ===")
skewed_result = df.join(lookup_df, on="key", how="left")
skewed_result.groupBy("key").count().show()


# ─────────────────────────────────────────
# 3. FIX 1 — SALTING TECHNIQUE
# ─────────────────────────────────────────
# Break the hot key "A" into N sub-keys (A_0, A_1, ... A_N)
# so work is spread across multiple partitions

SALT_FACTOR = 10

# Add a random salt to the large table
df_salted = df.withColumn(
    "salted_key",
    when(
        col("key") == "A",
        concat(col("key"), lit("_"), (rand() * SALT_FACTOR).cast("int").cast("string"))
    ).otherwise(col("key"))
)
print("=== df_salted ===")
df_salted.show(15)

# Explode the lookup table to match all salted variants
lookup_exploded = lookup_df.withColumn("salt", array([lit(i) for i in range(SALT_FACTOR)])) \
    .withColumn("salt", explode(col("salt"))) \
    .withColumn(
        "salted_key",
        when(
            col("key") == "A",
            concat(col("key"), lit("_"), col("salt").cast("string"))
        ).otherwise(col("key"))
    ).drop("salt")

print("=== lookup_exploded ===")
lookup_exploded.show(15)

# Join on salted key, then restore original key
print("=== Salted Join (Balanced) ===")
salted_result = df_salted.join(lookup_exploded, on="salted_key", how="left") \
    .drop("salted_key") \
    .drop(lookup_exploded["key"])

salted_result.groupBy("key").count().show()


# ─────────────────────────────────────────
# 4. FIX 2 — BROADCAST JOIN
# ─────────────────────────────────────────
# If the lookup table is small enough, broadcast it to all workers
# so no shuffling is needed at all

print("=== Broadcast Join (Best for small lookup tables) ===")
broadcast_result = df.join(broadcast(lookup_df), on="key", how="left")
broadcast_result.groupBy("key").count().show()


# ─────────────────────────────────────────
# 5. FIX 3 — REPARTITION BY KEY
# ─────────────────────────────────────────
# Manually repartition to give skewed keys their own partitions

print("=== Repartitioned ===")
df_repartitioned = df.repartition(20, "key")
result = df_repartitioned.join(broadcast(lookup_df), on="key", how="left")
result.groupBy("key").count().show()


# ─────────────────────────────────────────
# 6. DETECT SKEW — Partition Size Analysis
# ─────────────────────────────────────────
# Check how unevenly data is distributed across partitions

print("=== Partition Row Distribution ===")
df.withColumn("partition_id", spark_partition_id()) \
  .groupBy("partition_id") \
  .count() \
  .orderBy(desc("count")) \
  .show(10)

spark.stop()


## Key Concepts Explained

"""### What is Data Skew?
Data skew happens when one or more keys have **disproportionately more records** than others. 
In a distributed join/aggregation, the partition holding the hot key becomes a bottleneck — all other tasks finish while one executor struggles alone.

---

### Three Fix Strategies

| Strategy | Best When | Trade-off |
|---|---|---|
| **Salting** | Large-to-large skewed join | Extra complexity, need to explode lookup |
| **Broadcast Join** | Lookup table fits in memory (~few hundred MB) | Memory pressure on executors |
| **Repartition** | Uneven partition distribution | Shuffle cost upfront |

---

### How Salting Works
```
Before:  key "A" → 1 partition handles 90K rows 😬

After:   "A_0" → partition 0 → 9K rows
         "A_1" → partition 1 → 9K rows
         ...
         "A_9" → partition 9 → 9K rows  ✅"""




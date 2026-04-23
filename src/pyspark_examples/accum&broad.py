from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
# Spark Configuration
#sconf = SparkConf().setAppName("accum&broad").setMaster("local")
#sc = SparkContext(sconf)

# ─── ACCUMULATOR ───────────────────────────────────────────


spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Create accumulator
acc = sc.accumulator(0)

# Sample RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Add values to accumulator
rdd.foreach(lambda x: acc.add(x))

# Get result (only from driver)
print(acc.value)   # Output: 15

# ─── BROADCAST ─────────────────────────────────────────────
broadcast_var = sc.broadcast([10, 20, 30, 40, 50])

rdd2 = sc.parallelize([1, 2, 3, 4, 5])

# Multiply each element with corresponding broadcast value
result = rdd2.map(lambda x: x * broadcast_var.value[x - 1]).collect()

print("Broadcast result:", result)  # Output: [10, 40, 90, 160, 250]

# ─── STOP SPARK CONTEXT ────────────────────────────────────
sc.stop()

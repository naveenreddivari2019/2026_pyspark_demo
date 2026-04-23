from pyspark.sql import SparkSession
from pyspark import StorageLevel

spark=SparkSession.builder.appName("Persist Example").getOrCreate()
# Create a DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

print(df.schema)


# Cache the DataFrame
#df.cache()

#storage level
df.persist(StorageLevel.MEMORY_ONLY_2)
print(df.explain())  # Check the execution plan to see if caching is used
df.unpersist()  # Unpersist the DataFrame

# Perform an action to trigger caching
print("Count:", df.count())



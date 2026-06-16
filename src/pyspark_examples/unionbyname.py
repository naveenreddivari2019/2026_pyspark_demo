#unionbyname is used to combine two DataFrames with different column names.

from pyspark.sql import SparkSession
# Create a SparkSession
spark = SparkSession.builder.appName("UnionByNameExample").getOrCreate()
# Create the first DataFrame with columns "id" and "name"
data1 = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
columns1 = ["id", "name"]
df1 = spark.createDataFrame(data1, columns1)
# Create the second DataFrame with columns "id" and "age"
data2 = [(1, 25), (2, 30), (4, 35)]
columns2 = ["id", "age"]
df2 = spark.createDataFrame(data2, columns2)
# Use unionByName to combine the two DataFrames
combined_df = df1.unionByName(df2, allowMissingColumns=True)
# Show the combined DataFrame
combined_df.show()
# Stop the SparkSession
spark.stop()


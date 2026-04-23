from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSVRead").getOrCreate()

# Use relative path from project root
path = "src/main/data1.csv"

# Read CSV file into DataFrame
df = spark.read.option("mode", "DROPMALFORMED").csv(path, header=True, inferSchema=True)

df = df.withColumnRenamed("id", "ID")

print("Schema of the DataFrame:")
print(df.schema)
print("Columns in the DataFrame:")
print(df.columns)

# Group by Retailer and PPG, then count
df = df.groupBy("Retailer", "PPG").count()

# Select and order by count descending
df = df.select("Retailer", "PPG", "count").orderBy("count", ascending=False)

# Show top 2 results
df.show(2)

print("Done")

# Spark UI available at: http://localhost:4040/

# Stop the SparkSession
spark.stop()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
spark=SparkSession.builder.appName("test").getOrCreate()

path = "/Users/naveenkumarreddyreddivari/Downloads/FINAL_OUTPUT27022025.csv"

spark.conf.set("spark.sql.shuffle.partitions", "10")  # Set to a lower number for testing
spark.conf.set("spark.sql.files.maxPartitionBytes",'256MB') # Set to a lower value for testing, e.g., 64MB
df=spark.read.csv(path, header=True, inferSchema=True)


print(spark.conf.get("spark.sql.shuffle.partitions"))  # Default is 200
print(df.rdd.getNumPartitions())  # Check number of partitions
print(spark.conf.get("spark.sql.files.maxPartitionBytes"))  # Default is 128MB
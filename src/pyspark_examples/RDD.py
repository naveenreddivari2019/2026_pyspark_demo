from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("RDD Example").getOrCreate()
rdd = spark.sparkContext.parallelize([1,2,3])
rdd=rdd.map(lambda x: x*2)
print(rdd)
print(rdd.collect())

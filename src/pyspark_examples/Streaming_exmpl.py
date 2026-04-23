from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KafkaWindowExample").getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events_topic") \
    .load()

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, TimestampType

schema = StructType() \
    .add("user_id", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", TimestampType())

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


from pyspark.sql.functions import window

windowed_df = parsed_df \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(
        window(col("timestamp"), "10 minutes"),
        col("event_type")
    ).count()

query = windowed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .start("/delta/output")


query.awaitTermination()
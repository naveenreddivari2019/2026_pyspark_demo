import os
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

# Set environment variables to force localhost
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Create SparkSession with Delta Lake support
spark = SparkSession.builder \
    .appName("DeltaTableVersionExample") \
    .master("local[1]") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.shuffle.service.enabled", "false") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

table_path = "/Users/naveenkumarreddyreddivari/Git_2026_personal/2026_pyspark_demo/2026_pyspark_demo/src/delta_tables/pivot_table"
d_table= DeltaTable.forPath(spark, table_path)


#set retention period to 0 hours to allow immediate deletion of old files
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# Vacuum the table to remove old files
#d_table.vacuum(0).executeVacuum()


#delta log retention delta.logRetentionDuration
#.config("spark.databricks.delta.logRetentionDuration", "interval 30 days")



# Optimize the table to compact small files
d_table.optimize().executeCompaction()

#z-ordering
d_table.optimize().executeZOrderBy("Product")  # Replace "column_name" with the actual column you want to z-order by

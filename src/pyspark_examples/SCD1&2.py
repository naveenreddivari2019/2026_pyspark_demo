from delta.tables import DeltaTable

# Load target Delta table
target = DeltaTable.forPath(spark, "/mnt/delta/customer")

# Source (new data)
source_df = spark.read.format("parquet").load("/mnt/source/customer")

# Merge (Overwrite changes)
target.alias("t").merge(
    source_df.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedUpdate(
    set={
        "name": "s.name",
        "city": "s.city",
        "updated_at": "current_timestamp()"
    }
).whenNotMatchedInsert(
    values={
        "customer_id": "s.customer_id",
        "name": "s.name",
        "city": "s.city",
        "updated_at": "current_timestamp()"
    }
).execute()


from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_date, lit

target = DeltaTable.forPath(spark, "/mnt/delta/customer")

source_df = spark.read.format("parquet").load("/mnt/source/customer")

# Step 1: Identify changed records
updates_df = source_df.alias("s").join(
    target.toDF().alias("t"),
    "customer_id"
).where(
    "t.is_current = true AND (s.name <> t.name OR s.city <> t.city)"
).select("s.*")

# Step 2: Expire old records
target.alias("t").merge(
    updates_df.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true"
).whenMatchedUpdate(
    set={
        "end_date": "current_date()",
        "is_current": "false"
    }
).execute()

# Step 3: Insert new records
new_records_df = updates_df.withColumn("start_date", current_date()) \
    .withColumn("end_date", lit(None)) \
    .withColumn("is_current", lit(True))

target.alias("t").merge(
    new_records_df.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true"
).whenNotMatchedInsertAll().execute()


target.alias("t").merge(
    source_df.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true"
).whenMatchedUpdate(
    condition="t.name <> s.name OR t.city <> s.city",
    set={
        "end_date": "current_date()",
        "is_current": "false"
    }
).whenNotMatchedInsert(
    values={
        "customer_id": "s.customer_id",
        "name": "s.name",
        "city": "s.city",
        "start_date": "current_date()",
        "end_date": "null",
        "is_current": "true"
    }
).execute()
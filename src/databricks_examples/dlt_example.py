try:
    import dlt  # Available in Databricks runtime
except ImportError:
    import dlt_mock as dlt  # Use mock for local development

from pyspark.sql.functions import col, when, sum as spark_sum

# ─────────────────────────────────────────
# 1. SOURCE TABLE - Bronze Layer
# ─────────────────────────────────────────
@dlt.table(
    comment="Raw customer data from external source",
    table_properties={
        "quality": "bronze",
        "owner": "data-engineering"
    }
)
def raw_customers():
    """Ingest raw customer data"""
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("/Volumes/catalog/schema/raw_customers.csv")

# ─────────────────────────────────────────
# 2. BRONZE TABLE - Data Quality Checks
# ─────────────────────────────────────────
@dlt.table(
    comment="Customers with data quality validations",
    table_properties={"quality": "bronze"}
)
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_email", "email LIKE '%@%.%'")
@dlt.expect("valid_age", "age > 0 AND age < 150")
def bronze_customers():
    """Apply data quality checks to raw customers"""
    return dlt.read("raw_customers")

# ─────────────────────────────────────────
# 3. SILVER TABLE - Data Cleaning & Deduplication
# ─────────────────────────────────────────
@dlt.table(
    comment="Cleaned and deduplicated customer data",
    table_properties={"quality": "silver"}
)
def silver_customers():
    """Clean, deduplicate and standardize customer data"""
    return dlt.read("bronze_customers") \
        .select(
            col("customer_id"),
            col("first_name").cast("string"),
            col("last_name").cast("string"),
            col("email").lower().alias("email"),
            col("phone"),
            col("age").cast("int"),
            col("country"),
            col("created_at").cast("timestamp"),
            col("updated_at").cast("timestamp")
        ) \
        .dropDuplicates(["email"]) \
        .filter(col("customer_id").isNotNull())

# ─────────────────────────────────────────
# 4. GOLD TABLE - Business Aggregations
# ─────────────────────────────────────────
@dlt.table(
    comment="Customer metrics and aggregations",
    table_properties={"quality": "gold"}
)
def gold_customer_metrics():
    """Aggregate customer metrics by country"""
    return dlt.read("silver_customers") \
        .groupBy("country") \
        .agg(
            spark_sum(1).alias("total_customers"),
            spark_sum(when(col("age") >= 18, 1).otherwise(0)).alias("adult_count")
        ) \
        .select(
            col("country"),
            col("total_customers"),
            col("adult_count")
        )

# ─────────────────────────────────────────
# 5. STREAMING TABLE - Real-time Data Ingestion
# ─────────────────────────────────────────
@dlt.table(
    comment="Real-time customer events stream",
    table_properties={"quality": "bronze"}
)
def streaming_customer_events():
    """Ingest real-time customer events from Kafka or event hub"""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "customer-events") \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(
            col("value").cast("string").alias("event_data"),
            col("timestamp").alias("event_timestamp")
        )

# ─────────────────────────────────────────
# 6. MATERIALIZED VIEW - Incremental Processing
# ─────────────────────────────────────────
@dlt.materialized_view(
    comment="Customer activity summary - incremental updates",
    table_properties={"quality": "silver"}
)
@dlt.expect_all({
    "valid_customer": "customer_id IS NOT NULL",
    "recent_data": "updated_at >= current_date() - INTERVAL 30 DAY"
})
def customer_activity_summary():
    """Materialized view for incremental customer activity"""
    return dlt.read("silver_customers") \
        .filter(col("updated_at") >= spark.sql("SELECT current_date()").collect()[0][0])

# ─────────────────────────────────────────
# 7. DATA QUALITY RULES (SLA)
# ─────────────────────────────────────────
@dlt.table(
    comment="Data quality metrics and SLA monitoring",
    table_properties={"quality": "gold"}
)
def data_quality_sla():
    """Monitor data quality SLAs"""
    return spark.sql("""
        SELECT
            'silver_customers' as table_name,
            COUNT(*) as total_records,
            COUNT(CASE WHEN email IS NULL THEN 1 END) as null_emails,
            ROUND(100 * COUNT(CASE WHEN email IS NOT NULL THEN 1 END) / COUNT(*), 2) as data_quality_pct
        FROM silver_customers
    """)

# ─────────────────────────────────────────
# 8. DEPENDENCY CHAIN - DAG Definition
# ─────────────────────────────────────────
# Pipeline Flow:
# raw_customers → bronze_customers → silver_customers → gold_customer_metrics
#                                  → customer_activity_summary → data_quality_sla
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Pivot Example") \
    .getOrCreate()

# Sample data
data = [
    ("Banana", 1000, "USA"),
    ("Carrots", 1500, "USA"),
    ("Beans", 1600, "USA"),
    ("Orange", 2000, "USA"),
    ("Orange", 2000, "USA"),
    ("Banana", 400, "China"),
    ("Carrots", 1200, "China"),
    ("Beans", 1500, "China"),
    ("Orange", 4000, "China"),
    ("Banana", 2000, "Canada"),
    ("Carrots", 2000, "Canada"),
    ("Beans", 2000, "Mexico"),
]

# Define the schema
columns = ["Product", "Amount", "Country"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

df.show()

# Perform pivot operation
pivot_df = df.groupBy("Product").pivot("Country").sum("Amount")

# Show the result
pivot_df.show()



# Register the DataFrame as a temporary SQL table
df.createOrReplaceTempView("sales_data")

# Perform pivot operation using SQL
pivot_sql = spark.sql("""
    SELECT Product,
           SUM(CASE WHEN Country = 'USA' THEN Amount ELSE 0 END) AS USA,
           SUM(CASE WHEN Country = 'China' THEN Amount ELSE 0 END) AS China,
           SUM(CASE WHEN Country = 'Canada' THEN Amount ELSE 0 END) AS Canada,
           SUM(CASE WHEN Country = 'Mexico' THEN Amount ELSE 0 END) AS Mexico
    FROM sales_data
    GROUP BY Product
""")

# Show the result
pivot_sql.show()

# Perform pivot operation using SQL
pivot_sql2 = spark.sql("""
    SELECT *
    FROM (
        SELECT Product, Amount, Country
        FROM sales_data
    ) 
    PIVOT (
        SUM(Amount) FOR Country IN ('USA', 'China', 'Canada', 'Mexico', 'India')
    )
""")

# Show the result
pivot_sql2.show()


print("=== CTE 1 — Basic CTE with PIVOT ===")
pivot_cte1 = spark.sql("""
    WITH sales_cte AS (
        SELECT Product, Amount, Country     -- Step 1: Define CTE
        FROM sales_data
    )
    SELECT *
    FROM sales_cte
    PIVOT (
        SUM(Amount)                         -- Step 2: Pivot CTE
        FOR Country IN (
            'USA'    AS USA,
            'China'  AS China,
            'Canada' AS Canada,
            'Mexico' AS Mexico,
            'India'  AS India
        )
    )
    ORDER BY Product
""")
pivot_cte1.show()

# Stop the SparkSession
spark.stop()
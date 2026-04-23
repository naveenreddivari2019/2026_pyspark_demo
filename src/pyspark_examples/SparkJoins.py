from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

df1_struct_schema = "id INT, name STRING"

df2_struct_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("dept", StringType(), True)
])

df1 = spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie")
], df1_struct_schema)



df2 = spark.createDataFrame([
    (1, "HR"),
    (2, "IT"),
    (4, "Finance")
], df2_struct_schema)

print("df1 schema:")
df1.printSchema()
print("df2 schema:")
df2.printSchema()

# Inner Join
inner_join = df1.join(df2, on="id", how="inner")
print("Inner Join Result:")
inner_join.show()

# Left Outer Join
left_outer_join = df1.join(df2, on="id", how="left")
print("Left Outer Join Result:")
left_outer_join.show()

# Right Outer Join
right_outer_join = df1.join(df2, on="id", how="right")
print("Right Outer Join Result:")
right_outer_join.show()

# Full Outer Join
full_outer_join = df1.join(df2, on="id", how="outer")
print("Full Outer Join Result:")
full_outer_join.show()

#Left Semi Join
left_semi_join = df1.join(df2, on="id", how="left_semi")
print("Left Semi Join Result:")
left_semi_join.show()

# Left Anti Join
left_anti_join = df1.join(df2, on="id", how="left_anti")
print("Left Anti Join Result:")
left_anti_join.show()

# Cross Join
cross_join = df1.crossJoin(df2)
print("Cross Join Result:")
cross_join.show()



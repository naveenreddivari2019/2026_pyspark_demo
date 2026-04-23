from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType, TimestampType, BooleanType, ArrayType, MapType

spark = SparkSession.builder.getOrCreate()


data1=[1,1,1,2,2,3,4,5,6]
data2=[1,1,2,None,None]
table1= spark.createDataFrame(data1, IntegerType()).toDF("id")
table2= spark.createDataFrame(data2, IntegerType()).toDF("id")

table1.show()
table2.show()

table1.createOrReplaceTempView("Table1")
table2.createOrReplaceTempView("Table2")


# Inner Join
inner_join = spark.sql(
    """
    SELECT * FROM Table1 t1 JOIN Table2 t2 ON t1.id = t2.id
    """
)
print("Inner Join Result:")
print(f"Inner Join Count: {inner_join.count()}" )
inner_join.show()

# Left Outer Join
left_outer_join = spark.sql(
    """
    SELECT * FROM Table1 t1 LEFT JOIN Table2 t2 ON t1.id = t2.id
    """
)
print("Left Outer Join Result:")
print(f"left_outer_join Count: {left_outer_join.count()}" )
left_outer_join.show()

# Right Outer Join
right_outer_join = spark.sql(
    """
    SELECT * FROM Table1 t1 RIGHT JOIN Table2 t2 ON t1.id = t2.id
    """
)
print("Right Outer Join Result:")
print(f"right_outer_join Count: {right_outer_join.count()}" )
right_outer_join.show()

# Full Outer Join
full_outer_join = spark.sql(
    """
    SELECT * FROM Table1 t1 FULL OUTER JOIN Table2 t2 ON t1.id = t2.id
    """
)
print("Full Outer Join Result:")
print(f"full_outer_join Count: {full_outer_join.count()}" )
full_outer_join.show()

# Left Semi Join
left_semi_join = spark.sql(
    """
    SELECT * FROM Table1 t1 LEFT SEMI JOIN Table2 t2 ON t1.id = t2.id
    """
)
print("Left Semi Join Result:")
print(f"left_semi_join Count: {left_semi_join.count()}" )
left_semi_join.show()

# Left Anti Join
left_anti_join = spark.sql(
    """
    SELECT * FROM Table1 t1 LEFT ANTI JOIN Table2 t2 ON t1.id = t2.id
    """
)
print("Left Anti Join Result:")
print(f"left_anti_join Count: {left_anti_join.count()}" )
left_anti_join.show()

# Cross Join
cross_join = spark.sql(
    """
    SELECT * FROM Table1 t1 CROSS JOIN Table2 t2
    """
)
print("Cross Join Result:")
print(f"cross_join Count: {cross_join.count()}" )
cross_join.show()

# Stop the SparkSession
spark.stop()



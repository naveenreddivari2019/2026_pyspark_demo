from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField,
                                IntegerType, StringType,
                                FloatType, DateType)

class SQLDataExample:
    def __init__(self):
        pass


    def PrepareData(self):
        spark = SparkSession.builder \
            .appName("Emp_Dept_Mgr") \
            .getOrCreate()

        # ─────────────────────────────────────────
        # 1. EMPLOYEE TABLE
        # empid, empname, mgrid, deptid, sal, joindate
        # ─────────────────────────────────────────
        emp_schema = StructType([
            StructField("empid",    IntegerType(), True),
            StructField("empname",  StringType(),  True),
            StructField("mgrid",    IntegerType(), True),   # NULL for top-level managers
            StructField("deptid",   IntegerType(), True),
            StructField("sal",      FloatType(),   True),
            StructField("joindate", StringType(),  True),
        ])

        emp_data = [
            (101, "Alice",   None, 10, 90000.0, "2020-01-15"),  # Top Manager
            (102, "Bob",     101,  10, 75000.0, "2020-03-10"),
            (103, "Charlie", 101,  10, 70000.0, "2021-06-20"),
            (104, "David",   None, 20, 95000.0, "2019-08-01"),  # Top Manager
            (105, "Eve",     104,  20, 80000.0, "2021-02-14"),
            (106, "Frank",   104,  20, 72000.0, "2022-05-30"),
            (107, "Grace",   None, 30, 88000.0, "2018-11-11"),  # Top Manager
            (108, "Henry",   107,  30, 65000.0, "2023-01-05"),
            (109, "Ivy",     107,  30, 67000.0, "2022-09-19"),
            (110, "Jack",    104,  20, 60000.0, "2023-07-01"),
        ]

        emp_df = spark.createDataFrame(emp_data, emp_schema)
        print("=== EMPLOYEE TABLE ===")
        #emp_df.show()
        emp_df.printSchema()

        # ─────────────────────────────────────────
        # 2. DEPARTMENT TABLE
        # deptid, deptname, deptheadid
        # ─────────────────────────────────────────
        dept_schema = StructType([
            StructField("deptid",      IntegerType(), True),
            StructField("deptname",    StringType(),  True),
            StructField("deptheadid",  IntegerType(), True),  # References empid
            StructField("deptheadname", StringType(),  True),
        ])

        dept_data = [
            (10, "Engineering", 101, "Alice"),   # Alice heads Engineering
            (20, "Finance",     104, "David"),   # David heads Finance
            (30, "Marketing",   107, "Grace"),   # Grace heads Marketing
            (40, "HR",          None, None),     # No head assigned
        ]

        dept_df = spark.createDataFrame(dept_data, dept_schema)
        print("=== DEPARTMENT TABLE ===")
        #dept_df.show()
        dept_df.printSchema()

        # ─────────────────────────────────────────
        # 3. MANAGER TABLE
        # mgrid, mgrname, mgremail, mgrphone
        # ─────────────────────────────────────────
        mgr_schema = StructType([
            StructField("mgrid",    IntegerType(), True),
            StructField("mgrname",  StringType(),  True),
            StructField("mgremail", StringType(),  True),
            StructField("mgrphone", StringType(),  True),
        ])

        mgr_data = [
            (101, "Alice", "alice@company.com", "555-1001"),
            (104, "David", "david@company.com", "555-1004"),
            (107, "Grace", "grace@company.com", "555-1007"),
        ]

        mgr_df = spark.createDataFrame(mgr_data, mgr_schema)
        print("=== MANAGER TABLE ===")
        #mgr_df.show()
        mgr_df.printSchema()

        return emp_df, dept_df, mgr_df
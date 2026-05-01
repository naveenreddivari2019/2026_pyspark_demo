import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, max, min, countDistinct, collect_list, collect_set
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing"""
    spark = SparkSession.builder \
        .appName("GroupBy Operations Test") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def sample_df(spark):
    """Create sample DataFrame for testing"""
    schema = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("city", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("age", IntegerType(), True)
    ])

    data = [
        (1, "Alice", "Engineering", "NYC", 85000.0, 28),
        (2, "Bob", "Engineering", "NYC", 90000.0, 32),
        (3, "Charlie", "Sales", "LA", 70000.0, 25),
        (4, "David", "Sales", "LA", 75000.0, 29),
        (5, "Eve", "HR", "Chicago", 65000.0, 26),
        (6, "Frank", "Engineering", "SF", 95000.0, 35),
        (7, "Grace", "HR", "Chicago", 68000.0, 27),
        (8, "Henry", "Sales", "NYC", 72000.0, 30),
        (9, "Ivy", "Engineering", "SF", 88000.0, 29),
        (10, "Jack", "Sales", "LA", 71000.0, 24)
    ]

    return spark.createDataFrame(data, schema)


class TestBasicGroupBy:
    """Test basic groupBy operations"""

    def test_count_by_department(self, sample_df):
        """Test counting employees by department"""
        result = sample_df.groupBy("department").count().collect()
        dept_counts = {row["department"]: row["count"] for row in result}

        assert dept_counts["Engineering"] == 4
        assert dept_counts["Sales"] == 4
        assert dept_counts["HR"] == 2
        assert len(dept_counts) == 3

    def test_dataframe_row_count(self, sample_df):
        """Test total row count"""
        assert sample_df.count() == 10


class TestMultipleAggregations:
    """Test groupBy with multiple aggregation functions"""

    def test_multiple_aggs_by_department(self, sample_df):
        """Test multiple aggregations on department"""
        result = sample_df.groupBy("department").agg(
            count("employee_id").alias("num_employees"),
            avg("salary").alias("avg_salary"),
            max("salary").alias("max_salary"),
            min("salary").alias("min_salary")
        ).collect()

        eng_row = [row for row in result if row["department"] == "Engineering"][0]
        assert eng_row["num_employees"] == 4
        assert eng_row["avg_salary"] == 89500.0
        assert eng_row["max_salary"] == 95000.0
        assert eng_row["min_salary"] == 85000.0

        sales_row = [row for row in result if row["department"] == "Sales"][0]
        assert sales_row["num_employees"] == 4
        assert sales_row["avg_salary"] == 72000.0
        assert sales_row["max_salary"] == 75000.0
        assert sales_row["min_salary"] == 70000.0

    def test_sum_aggregation(self, sample_df):
        """Test sum aggregation"""
        result = sample_df.groupBy("department").agg(
            sum("salary").alias("total_salary")
        ).collect()

        dept_totals = {row["department"]: row["total_salary"] for row in result}
        assert dept_totals["Engineering"] == 358000.0
        assert dept_totals["Sales"] == 288000.0
        assert dept_totals["HR"] == 133000.0


class TestMultiColumnGroupBy:
    """Test groupBy with multiple columns"""

    def test_group_by_department_and_city(self, sample_df):
        """Test grouping by department and city"""
        result = sample_df.groupBy("department", "city").agg(
            count("*").alias("employee_count"),
            avg("salary").alias("avg_salary")
        ).collect()

        assert len(result) == 5

        eng_nyc = [row for row in result if row["department"] == "Engineering" and row["city"] == "NYC"][0]
        assert eng_nyc["employee_count"] == 2
        assert eng_nyc["avg_salary"] == 87500.0

        sales_la = [row for row in result if row["department"] == "Sales" and row["city"] == "LA"][0]
        assert sales_la["employee_count"] == 3
        assert sales_la["avg_salary"] == 72000.0


class TestStatisticalAggregations:
    """Test statistical aggregation functions"""

    def test_distinct_count(self, sample_df):
        """Test countDistinct aggregation"""
        result = sample_df.groupBy("department").agg(
            countDistinct("city").alias("distinct_cities")
        ).collect()

        dept_cities = {row["department"]: row["distinct_cities"] for row in result}
        assert dept_cities["Engineering"] == 2
        assert dept_cities["Sales"] == 2
        assert dept_cities["HR"] == 1

    def test_avg_age_by_department(self, sample_df):
        """Test average age calculation"""
        result = sample_df.groupBy("department").agg(
            avg("age").alias("avg_age")
        ).collect()

        dept_avg_age = {row["department"]: row["avg_age"] for row in result}
        assert dept_avg_age["Engineering"] == 31.0
        assert dept_avg_age["Sales"] == 27.0
        assert dept_avg_age["HR"] == 26.5


class TestCollectionFunctions:
    """Test collect_list and collect_set functions"""

    def test_collect_list_names(self, sample_df):
        """Test collect_list for employee names"""
        result = sample_df.groupBy("department").agg(
            collect_list("name").alias("employee_names")
        ).collect()

        eng_row = [row for row in result if row["department"] == "Engineering"][0]
        names = eng_row["employee_names"]
        assert len(names) == 4
        assert set(names) == {"Alice", "Bob", "Frank", "Ivy"}

    def test_collect_set_cities(self, sample_df):
        """Test collect_set for unique cities"""
        result = sample_df.groupBy("department").agg(
            collect_set("city").alias("unique_cities")
        ).collect()

        sales_row = [row for row in result if row["department"] == "Sales"][0]
        cities = sales_row["unique_cities"]
        assert len(cities) == 2
        assert set(cities) == {"LA", "NYC"}

        hr_row = [row for row in result if row["department"] == "HR"][0]
        hr_cities = hr_row["unique_cities"]
        assert len(hr_cities) == 1
        assert hr_cities[0] == "Chicago"


class TestFilteringAndOrdering:
    """Test filtering and ordering on grouped data"""

    def test_filter_avg_salary_greater_than_threshold(self, sample_df):
        """Test filtering departments with avg salary > 75000"""
        result = sample_df.groupBy("department").agg(
            avg("salary").alias("avg_salary")
        ).filter(col("avg_salary") > 75000).collect()

        assert len(result) == 1
        assert result[0]["department"] == "Engineering"
        assert result[0]["avg_salary"] == 89500.0

    def test_order_by_total_salary_desc(self, sample_df):
        """Test ordering by total salary descending"""
        result = sample_df.groupBy("department").agg(
            sum("salary").alias("total_salary")
        ).orderBy(col("total_salary").desc()).collect()

        assert result[0]["department"] == "Engineering"
        assert result[0]["total_salary"] == 358000.0
        assert result[1]["department"] == "Sales"
        assert result[1]["total_salary"] == 288000.0
        assert result[2]["department"] == "HR"
        assert result[2]["total_salary"] == 133000.0

    def test_filter_employee_count_greater_than_one(self, sample_df):
        """Test filtering cities with more than 1 employee"""
        result = sample_df.groupBy("city").agg(
            count("*").alias("employee_count")
        ).filter(col("employee_count") > 1).collect()

        assert len(result) == 4
        city_counts = {row["city"]: row["employee_count"] for row in result}
        assert city_counts["NYC"] == 3
        assert city_counts["LA"] == 3
        assert city_counts["SF"] == 2
        assert city_counts["Chicago"] == 2


class TestEdgeCases:
    """Test edge cases and special scenarios"""

    def test_empty_dataframe_groupby(self, spark):
        """Test groupBy on empty DataFrame"""
        schema = StructType([
            StructField("dept", StringType(), True),
            StructField("salary", DoubleType(), True)
        ])
        empty_df = spark.createDataFrame([], schema)
        result = empty_df.groupBy("dept").count().collect()
        assert len(result) == 0

    def test_single_row_groupby(self, spark):
        """Test groupBy with single row"""
        schema = StructType([
            StructField("dept", StringType(), True),
            StructField("salary", DoubleType(), True)
        ])
        single_df = spark.createDataFrame([("IT", 80000.0)], schema)
        result = single_df.groupBy("dept").agg(avg("salary").alias("avg_salary")).collect()
        assert len(result) == 1
        assert result[0]["dept"] == "IT"
        assert result[0]["avg_salary"] == 80000.0

    def test_groupby_with_nulls(self, spark):
        """Test groupBy with null values"""
        schema = StructType([
            StructField("dept", StringType(), True),
            StructField("salary", DoubleType(), True)
        ])
        data = [("IT", 80000.0), ("IT", None), ("HR", 60000.0), (None, 70000.0)]
        df_with_nulls = spark.createDataFrame(data, schema)

        result = df_with_nulls.groupBy("dept").count().collect()
        dept_counts = {row["dept"]: row["count"] for row in result}
        assert dept_counts["IT"] == 2
        assert dept_counts["HR"] == 1
        assert dept_counts[None] == 1


class TestComplexScenarios:
    """Test complex groupBy scenarios"""

    def test_chained_operations(self, sample_df):
        """Test chaining groupBy with filter and orderBy"""
        result = sample_df.groupBy("city").agg(
            count("*").alias("employee_count"),
            avg("salary").alias("avg_salary"),
            max("age").alias("max_age")
        ).filter(col("employee_count") > 1).orderBy(col("avg_salary").desc()).collect()

        assert len(result) == 4
        assert result[0]["city"] == "SF"
        assert result[0]["avg_salary"] == 91500.0

    def test_multiple_groupby_columns_with_filter(self, sample_df):
        """Test groupBy with multiple columns and filtering"""
        result = sample_df.groupBy("department", "city").agg(
            avg("salary").alias("avg_salary")
        ).filter(col("avg_salary") > 70000).collect()

        assert len(result) >= 3
        avg_salaries = [row["avg_salary"] for row in result]
        assert all(salary > 70000 for salary in avg_salaries)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

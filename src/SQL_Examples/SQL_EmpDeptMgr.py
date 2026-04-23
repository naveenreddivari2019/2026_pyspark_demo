from SQL_data import SQLDataExample
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Emp_Dept_Mgr").getOrCreate()

obj=SQLDataExample()

print("===================== SQL QUERIES =====================")

# ✅ Correct way to unpack returned DataFrames
emp_df, dept_df, mgr_df = obj.PrepareData()

print("===================== SQL QUERIES =====================")

# ─────────────────────────────────────────
# CREATE TEMP VIEWS FOR SQL QUERIES
# ─────────────────────────────────────────
emp_df.createOrReplaceTempView("Employee")
dept_df.createOrReplaceTempView("Department")
mgr_df.createOrReplaceTempView("Manager")

# ─────────────────────────────────────────
# SAMPLE QUERIES
# ─────────────────────────────────────────

# 1. Employee with Department Name
print("=== Employee with Department ===")
spark.sql("""
    SELECT e.empid, e.empname, e.sal, d.deptname
    FROM Employee e
    JOIN Department d ON e.deptid = d.deptid
""").show()

# 2. Employee with Manager Name
print("=== Employee with Manager ===")
spark.sql("""
    SELECT e.empid, e.empname, e.sal, m.mgrname
    FROM Employee e
    LEFT JOIN Manager m ON e.mgrid = m.mgrid
""").show()

# 3. Employees without Manager (Top Level)
print("=== Employees without Manager ===")
spark.sql("""
    SELECT empid, empname, sal
    FROM Employee
    WHERE mgrid IS NULL
""").show()

# 4. Department wise salary
print("=== Department wise Avg Salary ===")
spark.sql("""
    SELECT d.deptname,
           COUNT(e.empid) AS emp_count,
           AVG(e.sal)     AS avg_sal,
           MAX(e.sal)     AS max_sal,
           MIN(e.sal)     AS min_sal
    FROM Employee e
    JOIN Department d ON e.deptid = d.deptid
    GROUP BY d.deptname
""").show()

# 5. Full Detail — Employee + Department + Manager
print("=== Full Detail ===")
spark.sql("""
    SELECT  e.empid,
            e.empname,
            e.sal,
            e.joindate,
            d.deptname,
            d.deptheadname,
            m.mgrname
    FROM Employee e
    LEFT JOIN Department d ON e.deptid = d.deptid
    LEFT JOIN Manager    m ON e.mgrid  = m.mgrid
    ORDER BY e.empid
""").show()

#6. empname, mgrname, deptheadname, sal where sal>12000 
print("=== Employees with Salary > 12000 ===")
spark.sql("""
    SELECT e.empname, m.mgrname, d.deptheadname, e.sal
    FROM Employee e
    LEFT JOIN Department d ON e.deptid = d.deptid
    LEFT JOIN Manager m ON e.mgrid = m.mgrid
    WHERE e.sal > 12000
""").show()

#7. Employees who joined after 2021-01-01
print("=== Employees who joined after 2021-01-01 ===")
spark.sql("""
    SELECT empid, empname, joindate
    FROM Employee
    WHERE joindate > '2021-01-01'
""").show()

#8. 2nd highest salary employee from each department
print("=== 2nd Highest Salary Employee from Each Department ===")
spark.sql("""
 
        SELECT d.deptname, e.empname, e.sal,
               ROW_NUMBER() OVER (PARTITION BY d.deptname ORDER BY e.sal DESC) AS rn
        FROM Employee e
        JOIN Department d ON e.deptid = d.deptid
   
""").show()

spark.sql("""
    SELECT deptname, empname, sal
    FROM (
        SELECT d.deptname, e.empname, e.sal,
               ROW_NUMBER() OVER (PARTITION BY d.deptname ORDER BY e.sal DESC) AS rn
        FROM Employee e
        JOIN Department d ON e.deptid = d.deptid
    ) sub
    WHERE rn = 2
""").show()

spark.sql(
"""
select empname, deptname, sal from (
select e.empname,d.deptname,e.sal, 
row_number() over(partition by d.deptname order by e.sal desc ) as rank 
from Employee e 
left join Department d on e.deptid=d.deptid
) rn
where rank=1
"""
).show()



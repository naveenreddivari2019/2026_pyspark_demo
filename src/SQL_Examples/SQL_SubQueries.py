import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from SQL_data import SQLDataExample
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Emp_Dept_Mgr").getOrCreate()

obj = SQLDataExample()
emp_df, dept_df, mgr_df = obj.PrepareData()

emp_df.createOrReplaceTempView("Employee")
dept_df.createOrReplaceTempView("Department")
mgr_df.createOrReplaceTempView("Manager")

# ─────────────────────────────────────────
# 1. SCALAR SUBQUERY — Employee earning more than avg salary
# ─────────────────────────────────────────
print("=== 1. Employees earning more than AVG Salary ===")
spark.sql("""
    SELECT empid, empname, sal
    FROM Employee
    WHERE sal > (SELECT AVG(sal) FROM Employee)   -- Scalar Subquery
    ORDER BY sal DESC
""").show()


spark.sql("""

 select avg(sal) as avg_sal from employee
""").show()

# ─────────────────────────────────────────
# 2. CORRELATED SUBQUERY — Employees earning more than dept avg
# ─────────────────────────────────────────
print("=== 2. Employees earning more than their Dept AVG Salary ===")
spark.sql("""
    SELECT e1.empid, e1.empname, e1.sal, e1.deptid
    FROM Employee e1
    WHERE e1.sal > (
        SELECT AVG(e2.sal)               -- Correlated Subquery
        FROM Employee e2
        WHERE e2.deptid = e1.deptid      -- References outer query
    )
    ORDER BY e1.deptid, e1.sal DESC
""").show()

spark.sql(
    """
    select e1.empid, e1.empname, e1.sal, e1.deptid from employee e1 left join
    (
    select e.deptid, avg(e.sal) as avg_sal from employee e 
    group by e.deptid 
    )
    e2 on e1.deptid = e2.deptid where e1.sal > e2.avg_sal
    """
).show()

spark.sql(
    """
    select e1.empid, e1.empname, e1.sal, e1.deptid from employee e1 left join(
    select e.deptid, d.deptname, avg(e.sal) as avg_sal from employee e 
    left join department d on e.deptid = d.deptid 
    group by e.deptid, d.deptname ) e2 on e1.deptid = e2.deptid where e1.sal > e2.avg_sal
    """
).show()
# ─────────────────────────────────────────
# 3. IN SUBQUERY — Employees who ARE managers
# ─────────────────────────────────────────
print("=== 3. Employees who ARE Managers ===")
spark.sql("""
    SELECT empid, empname, sal
    FROM Employee
    WHERE empid IN (SELECT mgrid FROM Manager)   -- IN Subquery
""").show()

spark.sql("""
    SELECT e.empid, e.empname, e.sal
    FROM Employee e left semi join Manager m on e.empid = m.mgrid
""").show()

# ─────────────────────────────────────────
# 4. NOT IN SUBQUERY — Employees who are NOT managers
# ─────────────────────────────────────────
print("=== 4. Employees who are NOT Managers ===")
spark.sql("""
    SELECT empid, empname, sal
    FROM Employee
    WHERE empid NOT IN (SELECT mgrid FROM Manager)  -- NOT IN Subquery
""").show()

spark.sql("""
    SELECT e.empid, e.empname, e.sal
    FROM Employee e left anti join Manager m on e.empid = m.mgrid
""").show()

# ─────────────────────────────────────────
# 5. EXISTS SUBQUERY — Departments that HAVE employees
# ─────────────────────────────────────────
print("=== 5. Departments that HAVE Employees ===")
spark.sql("""
    SELECT d.deptid, d.deptname
    FROM Department d
    WHERE EXISTS (
        SELECT 1 FROM Employee e         -- EXISTS Subquery
        WHERE e.deptid = d.deptid
    )
""").show()

spark.sql("""
    SELECT d.deptid, d.deptname
    FROM Department d
    left semi JOIN Employee e ON e.deptid = d.deptid
""").show()

# ─────────────────────────────────────────
# 6. NOT EXISTS — Departments with NO employees
# ─────────────────────────────────────────
print("=== 6. Departments with NO Employees ===")
spark.sql("""
    SELECT d.deptid, d.deptname
    FROM Department d
    WHERE NOT EXISTS (
        SELECT 1 FROM Employee e         -- NOT EXISTS Subquery
        WHERE e.deptid = d.deptid
    )
""").show()


spark.sql("""
    SELECT d.deptid, d.deptname
    FROM Department d
    left anti JOIN Employee e ON e.deptid = d.deptid
""").show()

# ─────────────────────────────────────────
# 7. DERIVED TABLE — Top earner per department
# ─────────────────────────────────────────
print("=== 7. Top Earner per Department ===")
spark.sql("""
    SELECT d.deptname, e.empname, e.sal
    FROM Employee e
    JOIN Department d ON e.deptid = d.deptid
    JOIN (
        SELECT deptid, MAX(sal) AS max_sal   -- Derived Table
        FROM Employee
        GROUP BY deptid
    ) max_dept
    ON e.deptid = max_dept.deptid
    AND e.sal   = max_dept.max_sal
    ORDER BY e.sal DESC
""").show()


spark.sql("""
    select deptid, deptname, empid,empname, sal from (
            select e.empid, e.empname,e.sal,e.deptid,d.deptname, 
            rank() over(partition by e.deptid order by e.sal desc) as rank from Employee e
            left join department d on e.deptid = d.deptid   
            ) 
    emp where rank = 1 
""").show()



# ─────────────────────────────────────────
# 8. WITH CTE — 2nd Highest salary per dept
# ─────────────────────────────────────────
print("=== 8. 2nd Highest Salary per Department (CTE) ===")
spark.sql("""
    WITH RankedEmp AS (
        SELECT  e.empname,
                d.deptname,
                e.sal,
                DENSE_RANK() OVER (
                    PARTITION BY d.deptname
                    ORDER BY e.sal DESC
                ) AS rnk
        FROM Employee e
        JOIN Department d ON e.deptid = d.deptid
    )
    SELECT empname, deptname, sal
    FROM RankedEmp
    WHERE rnk = 2
""").show()

# ─────────────────────────────────────────
# 9. MULTIPLE CTE — Avg sal + employees above avg
# ─────────────────────────────────────────
print("=== 9. Multiple CTE — Dept Avg + Employees above Avg ===")
spark.sql("""
    WITH DeptAvg AS (
        SELECT deptid, AVG(sal) AS avg_sal
        FROM Employee
        GROUP BY deptid
    ),
    AboveAvg AS (
        SELECT e.empname, e.sal, e.deptid
        FROM Employee e
        JOIN DeptAvg da ON e.deptid = da.deptid
        WHERE e.sal > da.avg_sal
    )
    SELECT a.empname, a.sal, d.deptname
    FROM AboveAvg a
    JOIN Department d ON a.deptid = d.deptid
    ORDER BY d.deptname, a.sal DESC
""").show()

# ─────────────────────────────────────────
# 10. NESTED SUBQUERY — Dept with highest avg salary
# ─────────────────────────────────────────
print("=== 10. Department with Highest AVG Salary ===")
spark.sql("""
    SELECT d.deptname, AVG(e.sal) AS avg_sal
    FROM Employee e
    JOIN Department d ON e.deptid = d.deptid
    GROUP BY d.deptname
    HAVING AVG(e.sal) = (
        SELECT MAX(avg_sal)              -- Nested Subquery
        FROM (
            SELECT AVG(sal) AS avg_sal
            FROM Employee
            GROUP BY deptid
        ) dept_avg
    )
""").show()


spark.sql("""
    SELECT d.deptname,
           ROUND(AVG(e.sal), 2) AS avg_sal
    FROM Employee e
    JOIN Department d ON e.deptid = d.deptid
    GROUP BY d.deptname
    ORDER BY avg_sal DESC                         -- Highest first
    LIMIT 1                                       -- Only top dept
""").show()

# ─────────────────────────────────────────
# 11. SELF JOIN — Employee with their Manager name
# ─────────────────────────────────────────
print("=== 11. Self Join — Employee and their Manager ===")
spark.sql("""
    SELECT  e.empname  AS employee,
            m.empname  AS manager,
            e.sal
    FROM Employee e
    LEFT JOIN Employee m ON e.mgrid = m.empid   -- Self Join
    ORDER BY e.empid
""").show()

# ─────────────────────────────────────────
# 12. WINDOW FUNCTIONS — Running total salary per dept
# ─────────────────────────────────────────
print("=== 12. Running Total Salary per Department ===")
spark.sql("""
    SELECT  e.empname,
            d.deptname,
            e.sal,
            SUM(e.sal) OVER (
                PARTITION BY d.deptname
                ORDER BY e.sal DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS running_total
    FROM Employee e
    JOIN Department d ON e.deptid = d.deptid
    ORDER BY d.deptname, e.sal DESC
""").show()
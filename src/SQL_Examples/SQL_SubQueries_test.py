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
select avg(sal) as avg_salary from Employee
          """).show()

spark.sql("""
select e1.empid, e1.empname, e1.sal from Employee e1 where 
          e1.sal > (select avg(sal) as avg_salary from Employee)
          """).show()


# ─────────────────────────────────────────
# 2. CORRELATED SUBQUERY — Employees earning more than dept avg
# ─────────────────────────────────────────
print("=== 2. Employees earning more than their Dept AVG Salary ===")

spark.sql("""
          select e2.deptid, avg(e2.sal) as dept_avg_sal from Employee e2 group by e2.deptid

          """).show()

spark.sql("""
          select e1.empid, e1.empname,e1.deptid, e1.sal from Employee e1 left join (
          select e2.deptid, avg(e2.sal) as dept_avg_sal from Employee e2 group by e2.deptid ) 
          e3 on e1.deptid = e3.deptid where e1.sal < e3.dept_avg_sal
          """).show()

spark.sql("""
          select e1.empid, e1.empname,e1.deptid, e1.sal from Employee e1 where 
          e1.sal < (select avg(e2.sal) as dept_avg_sal from Employee e2 where e1.deptid = e2.deptid) 
          """).show()

# ─────────────────────────────────────────
# 3. IN SUBQUERY — Employees who ARE managers
# ─────────────────────────────────────────
print("=== 3. Employees who ARE Managers ===")

spark.sql("""
          select * from Employee e1 where e1.empid in (select mgrid from Manager)
          """).show()

spark.sql("""
          select * from Employee e1 left semi join Manager m1 on e1.empid = m1.mgrid
          """).show()
# ─────────────────────────────────────────
# 4. NOT IN SUBQUERY — Employees who are NOT managers
# ─────────────────────────────────────────
print("=== 4. Employees who are NOT Managers ===")

spark.sql("""
          select e1.empid, e1.empname,e1.deptid, e1.mgrid, e1.sal from Employee e1 
          where e1.empid not in (select mgrid from Manager)
          """).show()

spark.sql("""
          select * from Employee e1 left anti join Manager m1 on e1.empid = m1.mgrid
          """).show()

# ─────────────────────────────────────────
# 5. EXISTS SUBQUERY — Departments that HAVE employees
# ─────────────────────────────────────────
print("=== 5. Departments that HAVE Employees ===")


# ─────────────────────────────────────────
# 6. NOT EXISTS — Departments with NO employees
# ─────────────────────────────────────────
print("=== 6. Departments with NO Employees ===")


# ─────────────────────────────────────────
# 7. DERIVED TABLE — Top earner per department
# ─────────────────────────────────────────
print("=== 7. Top Earner per Department ===")


# ─────────────────────────────────────────
# 8. WITH CTE — 2nd Highest salary per dept
# ─────────────────────────────────────────
print("=== 8. 2nd Highest Salary per Department (CTE) ===")


# ─────────────────────────────────────────
# 9. MULTIPLE CTE — Avg sal + employees above avg
# ─────────────────────────────────────────
print("=== 9. Multiple CTE — Dept Avg + Employees above Avg ===")


# ─────────────────────────────────────────
# 10. NESTED SUBQUERY — Dept with highest avg salary
# ─────────────────────────────────────────
print("=== 10. Department with Highest AVG Salary ===")


# ─────────────────────────────────────────
# 11. SELF JOIN — Employee with their Manager name
# ─────────────────────────────────────────
print("=== 11. Self Join — Employee and their Manager ===")


# ─────────────────────────────────────────
# 12. WINDOW FUNCTIONS — Running total salary per dept
# ─────────────────────────────────────────
print("=== 12. Running Total Salary per Department ===")

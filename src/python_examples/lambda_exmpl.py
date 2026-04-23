

res=lambda a,b: a + b
print(res(5,6))


people = [("Ravi", 30), ("Anita", 25), ("Kumar", 35)]
print(people)
# sort by age (second element)
ps=sorted(people, key=lambda p: p[1])
# [('Anita', 25), ('Ravi', 30), ('Kumar', 35)]
print(ps)
# sort dicts by a field
employees = [{"name": "Ravi", "salary": 90000},
             {"name": "Anita", "salary": 75000}]
print(employees)
es=sorted(employees, key=lambda e: e["salary"], reverse=True)
print(es)


nums = [1, 2, 3, 4, 5, 6]

list(map(lambda x: x ** 2, nums))          # [1, 4, 9, 16, 25, 36]
list(filter(lambda x: x % 2 == 0, nums))   # [2, 4, 6]

from functools import reduce
reduce(lambda acc, x: acc + x, nums)       # 21
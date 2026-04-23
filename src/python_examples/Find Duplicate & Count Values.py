import sys

print("test is running..........")

r1=10

#del r1
print(sys.getrefcount(r1))

r=range(0,10,2)
print(r)
for i in r:
    print(i)



l1=[1,2,3,4,5,5]
l1.append(6)
print(l1.index(3))
l1[0]=10
print(l1)
print(l1.count(3))

# Method 1: Using a loop (existing approach)
l2 = []
l3 = []
for i in l1:
    if i in l2:
        l3.append(i)
    else:
        l2.append(i)
print("Duplicates (Method 1):", l3)

# Method 2: Using list comprehension
duplicates = [i for i in l1 if l1.count(i) >1 ]
print("Duplicates (Method 2):", list(set(duplicates)))

# Method 3: Using set
seen = set()
duplicates = set()
for i in l1:
    if i in seen:
        duplicates.add(i)
    else:
        seen.add(i)
print("Duplicates (Method 3):", list(duplicates))





# Method 1: Using a loop
count_dict = {}
for i in l1:
    if i in count_dict:
        count_dict[i] += 1
    else:
        count_dict[i] = 1
print("Count (Method 1):", count_dict)

# Method 2: Using list.count()
count_dict2 = {i: l1.count(i) for i in l1}
print("Count (Method 2):", count_dict2)

# Method 3: Using collections.Counter (Most Efficient)
from collections import Counter
count_dict3 = Counter(l1)
print("Count (Method 3):", count_dict3)
print("Count of each element:", dict(count_dict3))


f1=count_dict2.keys()
print(list(f1))









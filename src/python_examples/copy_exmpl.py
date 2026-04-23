#1. Assignment (No Copy ❗) 
# This does NOT create a new object 
# Both a and b point to the same memory
a = [1, 2, 3]
b = a
b.append(4)
print(a)

print(a == b)


#2. Shallow Copy
# This creates a new object, but the elements are still references to the same objects
#Creates a new object, but nested objects are still shared

import copy

a = [0,[1, 2], [3, 4]]
b = copy.copy(a)

b[0] = 99
print(a)  # [0, [1, 2], [3, 4]]
b[1][0] = 88
print(a)  # [0, [88, 2], [3, 4]]

#3. Deep Copy
# This creates a new object and recursively copies all nested objects

import copy

a = [[1, 2], [3, 4]]
b = copy.deepcopy(a)

b[0][0] = 99
print(a)  # [[1, 2], [3, 4]]
print(b)  # [[99, 2], [3, 4]]




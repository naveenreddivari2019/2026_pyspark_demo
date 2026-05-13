
r = range(5)
print(r)  # range(0, 5)
print(list(r))  # [0, 1, 2, 3, 4]



d1={'a': 1, 'b': 2, 'c': 3}
print(d1)  # {'a': 1, 'b': 2, 'c': 3}

print(d1.keys())  # dict_keys(['a', 'b', 'c'])
print(list(d1.keys()))  # ['a', 'b', 'c']
print(d1.values())  # dict_values([1, 2, 3])

print(d1.items())  # dict_items([('a', 1), ('b', 2), ('c', 3)])


nums = [1, 2,2, 2, 3, 4, 4]

unique = []
duplicate=[]
for num in nums:

    if num not in unique:
        unique.append(num)
    else:
        print(f"{num} is a duplicate")
        duplicate.append(num)


print(unique)  # [1, 2, 3, 4]
print(duplicate)  # [2, 4]
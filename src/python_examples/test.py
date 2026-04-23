
r = range(5)
print(r)  # range(0, 5)
print(list(r))  # [0, 1, 2, 3, 4]



d1={'a': 1, 'b': 2, 'c': 3}
print(d1)  # {'a': 1, 'b': 2, 'c': 3}

print(d1.keys())  # dict_keys(['a', 'b', 'c'])

print(d1.values())  # dict_values([1, 2, 3])

for x,y in d1.items():
    print(x)  
    print(y)
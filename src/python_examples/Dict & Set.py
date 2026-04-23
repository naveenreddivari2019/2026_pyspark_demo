s1=set()
for i in range(10):
    s1.add(i)

print(s1)






d1=dict()
for i in range(10):
    d1[i]='A'+str(i)

print(d1)


for k,v in d1.items():
    print(k,v)


d1['']='B3'
d1['']='B4'
print(d1)

del d1[0]

print(d1)

print(d1.get(11,'Not Found'))
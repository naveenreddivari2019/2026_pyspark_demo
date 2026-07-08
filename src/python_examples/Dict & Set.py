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


#dict merging
dic1={'A':1,'B':2,'C':3}
dic2={'D':4,'E':5,'F':6}

#dic1.update(dic2) #modifies original dictionary
result={**dic1,**dic2} #creates new dictionary unpacking the key value pairs of both dictionaries
print(result)
result2=dic1 | dic2 #creates new dictionary by merging d1 and d2, if there are duplicate keys, the value from d2 will be used
print(result2)
#update updates the original dictionary with the key value pairs of the other dictionary
dic1.update(dic2) #creates new dictionary by merging d1 and d2, if there are duplicate keys, the value from d2 will be used
print(dic1)
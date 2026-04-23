import sys

l1= [1,2,3,4,5,6,7,8,9,10]
t1= (1,2,3,4,5,6,7,8,9,10)

print("List: ", l1)
print("Tuple: ", t1)

print(sys.getsizeof(l1), "bytes")
print(sys.getsizeof(t1), "bytes")


for e in t1:
    print(e)
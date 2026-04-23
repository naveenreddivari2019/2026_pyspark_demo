
l1=[1,2,3,4,9,5,6,7,8,9]

targetValue=9

def findIndex(l1,targetValue):
    for i in range(len(l1)):
        if l1[i]==targetValue:
            return i
    return -1


print(findIndex(l1,targetValue))


targetValue=5

def sumoftwo(l1,targetValue):
    pairs = []
    for i in range(len(l1)):
        for j in range(i+1,len(l1)):
            if l1[i]+l1[j]==targetValue:
                print(f"Pair found: ({l1[i]}, {l1[j]}) at indices ({i}, {j})")
                pairs.append((l1[i],l1[j]))
    return pairs

print(sumoftwo(l1,targetValue))
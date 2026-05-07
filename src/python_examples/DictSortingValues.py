dic1={'A':2,'B':3,'C':1}
print("Dictionary before sorting:")
print(dic1)

# Sorting the dictionary by values
sorted_dic1 = dict(sorted(dic1.items(), key=lambda item: item[1]))

print("Dictionary after sorting by values:")
print(sorted_dic1)

#Sorting the dictionary by values in descending order
sorted_dic1_desc = dict(sorted(dic1.items(), key=lambda item: item[1], reverse=True))

print("Dictionary after sorting by values in descending order:")
print(sorted_dic1_desc)
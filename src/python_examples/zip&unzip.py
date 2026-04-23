k=[1,2,3]
v=['one','two','three','four']

print(zip(k, v))  # This will print a zip object

# Using zip to combine two lists into a dictionary
zipped_dict = dict(zip(k, v))

print("Zipped Dictionary:", zipped_dict)

# Using zip to combine two lists into a list of tuples
zipped_list = list(zip(k, v))

print("Zipped List of Tuples:", zipped_list)

#unzipping the zipped list of tuples back into two separate lists
unzipped_k, unzipped_v = zip(*zipped_dict.items())

print("Unzipped Keys:", unzipped_k)
print("Unzipped Values:", unzipped_v)
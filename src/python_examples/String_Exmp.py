
str1='hello world'

# Method 1: Using slicing (Most Pythonic)
print("Method 1 (Slicing):", str1[::-1])

# Method 2: Using reversed() function
print("Method 2 (reversed):", ''.join(reversed(str1)))

# Method 3: Using a loop
reversed_str = ''
for i in str1:
    reversed_str = i +reversed_str
print("Method 3 (Loop):", reversed_str)

# Method 4: Using list reverse()
str_list = list(str1)
print(str_list)
str_list.reverse()
print("Method 4 (list.reverse):", ''.join(str_list))

# Method 5: Using recursion
def reverse_string(s):
    print("Current string:", s)
    if len(s) == 0:
        return s
    return reverse_string(s[1:]) + s[0]
print("Method 5 (Recursion):", reverse_string(str1))




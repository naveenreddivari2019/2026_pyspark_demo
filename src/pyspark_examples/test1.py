from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Create accumulator
acc = sc.accumulator(0)

# Sample RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Add values to accumulator
rdd.foreach(lambda x: acc.add(x))

# Get result (only from driver)
print(acc.value)   # Output: 15



# Armstrong Number Check
n = 154

# Count number of digits
num_digits = len(str(n))

# Sum of each digit raised to power of digit count
total = sum(int(digit) ** num_digits for digit in str(n))

if total == n:
    print(f"{n} is an Armstrong Number ✅")
else:
    print(f"{n} is NOT an Armstrong Number ❌")

def fibonacci_recursive(n):
    if n <= 0:
        return 0
    elif n == 1:
        return 1
    return fibonacci_recursive(n - 1) + fibonacci_recursive(n - 2)

print(fibonacci_recursive(10))  # 55




def n_largest(lst, n):
    return sorted(lst, reverse=True)[:n]

lst = [10, 20, 30, 40, 50, 60, 70]
print(n_largest(lst, 3))   # [70, 60, 50]

def second_largest(lst):
    unique = list(set(lst))        # Remove duplicates
    unique.sort(reverse=True)      # Sort descending
    return unique[1]               # 2nd element
lst = [10, 20, 30, 40, 50]
print(second_largest(lst))   # 40
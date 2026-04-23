

import sys

# List — computes everything upfront, holds all values in memory
big_list = [x * x for x in range(1_000_000)]
print(sys.getsizeof(big_list))    # ~8.7 MB

# Generator — holds only the current frame, no matter how large
big_gen  = (x * x for x in range(1_000_000))
print(big_gen)
print(sys.getsizeof(big_gen))     # ~200 bytes always



def gen1(n):
    for i in range(n):
        yield i * i

it1=gen1(10)
print(it1.__next__())
print(it1.__next__())
print(it1.__next__())
#res=[x for x in gen1(200)]
#print(res)
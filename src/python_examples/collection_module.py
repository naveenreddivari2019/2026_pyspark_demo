from collections import Counter, defaultdict, namedtuple, OrderedDict

# Counter
data = ['apple', 'banana', 'apple', 'orange', 'banana', 'apple']
counter = Counter(data)
print("Counter:", counter)


# defaultdict
def_dict = defaultdict(int)
def_dict['a'] += 1
def_dict['b'] += 2
print("defaultdict:", def_dict)

# namedtuple
Point = namedtuple('Point', ['x', 'y'])
p = Point(1, 2)
print("namedtuple:", p)

# OrderedDict
ordered_dict = OrderedDict()
ordered_dict['first'] = 1
ordered_dict['second'] = 2
ordered_dict['third'] = 3
print("OrderedDict:", ordered_dict)

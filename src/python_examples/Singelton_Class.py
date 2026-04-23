# Method 1: Using __new__ method
class Singleton:
    _instance = None  # Class variable to store the single instance

    def __new__(cls):
        if cls._instance is None:
            print("Creating new instance")
            cls._instance = super().__new__(cls)
        else:
            print("Instance already exists")
        return cls._instance

    def __init__(self):
        print("Initializing instance")
        self.name = "Singleton Object"

# Testing Method 1
s1 = Singleton()
s2 = Singleton()
s3 = Singleton()

print("s1 id:", id(s1))  # Same id
print("s2 id:", id(s2))  # Same id
print("s3 id:", id(s3))  # Same id
print("s1 is s2:", s1 is s2)  # True
print("s1 is s3:", s1 is s3)  # True


# Method 2: Using class method
class SingletonClassMethod:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            print("Creating new instance")
            cls._instance = cls()
        else:
            print("Instance already exists")
        return cls._instance

    def __init__(self):
        self.name = "Singleton Object"

# Testing Method 2
s4 = SingletonClassMethod.get_instance()
s5 = SingletonClassMethod.get_instance()
s6 = SingletonClassMethod.get_instance()

print("s4 id:", id(s4))  # Same id
print("s5 id:", id(s5))  # Same id
print("s6 id:", id(s6))  # Same id
print("s4 is s5:", s4 is s5)  # True


# Method 3: Using decorator
def singleton(cls):
    instances = {}
    def get_instance(*args, **kwargs):
        if cls not in instances:
            print("Creating new instance")
            instances[cls] = cls(*args, **kwargs)
        else:
            print("Instance already exists")
        return instances[cls]
    return get_instance

@singleton
class SingletonDecorator:
    def __init__(self):
        self.name = "Singleton Object"

# Testing Method 3
s7 = SingletonDecorator()
s8 = SingletonDecorator()

print("s7 id:", id(s7))  # Same id
print("s8 id:", id(s8))  # Same id
print("s7 is s8:", s7 is s8)  # True
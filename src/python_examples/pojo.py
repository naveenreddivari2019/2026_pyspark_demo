#Method 1
class Student:
    def __init__(self, name):
        self.name = name

s1 = Student("Alice")
print(s1.name)  # Output: Alice
s1.name = "Bob"
print(s1.name)  # Output: Bob

#Method2
class Person:
    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value


p1=Person("Alice")
print(p1.name)  # Output: Alice
p1.name = "Bob"
print(p1.name)  # Output: Bob

#Method 3
from dataclasses import dataclass

@dataclass
class Employee:
    name: str
    id: int

e1 = Employee("Charlie", 123)
print(e1.name)  # Output: Charlie
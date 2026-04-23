from abc import ABC, abstractmethod

class Animal(ABC):
    @abstractmethod
    def make_sound(self):
        pass
    def m1(self):
        print("This is a concrete method in the abstract class.")

class Dog(Animal):
    def make_sound(self):
        return "Woof!"

    
#creating an instance of Dog
dog = Dog()
print(dog.make_sound())  # Output: Woof!
dog.m1()  # Output: This is a concrete method in the abstract class.

class Animal:
    def __init__(self):
        print("Animal created")
        
    def speak(self):
        print("Animal speaks")

class Dog(Animal):
    def __init__(self):
        super().__init__()  # Call the constructor of the parent class
        print("Dog created")
        
    def speak(self):
        #super().speak()  # Call the speak method of the parent class
        print("Woof!")


# Create an instance of Dog
my_dog = Dog()
# Call the speak method
my_dog.speak()

my_Animal = Animal()
my_Animal.speak()


class A:
    def __init__(self):
        print("Class A created")

    def m1(self):
        print("Method A")

class B:
    def __init__(self):
        print("Class B created")

    def m1(self):
        print("Method B")

    def m2(self):
        print("Method2 B")

class C(A, B):
    def __init__(self):
        super().__init__()  # This will call the constructor of class A due to MRO Method Resolution Order
        print("Class C created")

    def m1(self):
        #super().m1()  # This will call the m1 method of class A due to MRO
        print("Method C")

# Create an instance of C
my_c = C()
# Call the m1 method
my_c.m1()

my_c.m2()

print(C.mro())  # This will show the method resolution order for class C

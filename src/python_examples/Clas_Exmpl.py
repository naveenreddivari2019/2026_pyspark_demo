class Person:
    def __init__(self, name,age):
        print("Constructor called")
        self.name = name  # instance variable
        self.age = age

    def greet(self):
        print("Hello, my name is", self.name)

p1 = Person("Naveen",45)
print(p1)
p1.greet()

p2 = Person("kumar", 30)
p3 = Person("kumar", 30)
person_list= []
person_list.append(p1) 
person_list.append(p2)
person_list.append(p3)
print(person_list)

print(p2.name==p3.name)  # True

print(person_list[0].name)  # Naveen
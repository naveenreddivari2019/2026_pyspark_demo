"""x = 10  # global namespace

def outer():
    y = 20  # enclosing namespace
    def inner():
        z = 30  # local namespace
        print(locals())   # {'z': 30}
        print(globals())  # {'x': 10, ...module-level names}
    inner()

outer()"""

x = 10  # global namespace

def outer():
  
    y = 20  # enclosing namespace
    
    def inner():
        z = 30  # local namespace
        nonlocal y
        y=25
        print(f' x : {x} y : {y} z : {z}')  
    inner()
    global x
    x=50
outer()

print(f' x : {x}')
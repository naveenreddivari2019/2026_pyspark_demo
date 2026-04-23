#using if else if statements
def switch1(day):
    if day == 1:
        return "Monday" 
    elif day == 2:
        return "Tuesday" 
    else:
        return "Invalid day"

print(switch1(2))

#using dictionary
def switch2(day):
    switcher = {
        1: "Monday",
        2: "Tuesday",
        3: "Wednesday"
    }
    return switcher.get(day, "Invalid day")

print(switch2(3))

#using match case (Python 3.10+)
def switch3(day):
    match day:
        case 1:
            return "Monday"
        case 2:
            return "Tuesday"
        case 3:
            return "Wednesday"
        case _:
            return "Invalid day"
        
print(switch3(1))
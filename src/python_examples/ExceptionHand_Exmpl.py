
# Define custom exception
class CustomException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

def ExceptionHand_Exmpl():
    try:
        a = 10
        b = 0
        c = a/ b
        print(c)
        #raise CustomException("This is a custom exception.")  # custom exception
    
    except ZeroDivisionError:
        print("Error: Division by zero is not allowed. ")
    except Exception as e:
        print(f"An unexpected error occurred: {e}") 
    finally:
        print("This block will always execute.")



ExceptionHand_Exmpl()



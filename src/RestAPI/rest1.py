import requests

def get_user_info(user_id):
    url = f"https://jsonplaceholder.typicode.com/users/{user_id}"
    response = requests.get(url)
    print(f"Request URL response : {response.json()}")
    
    if response.status_code == 200:
        return response.json()
    else:
        return None
    

if __name__ == "__main__":
    user_id = 1
    user_info = get_user_info(user_id)
    
    if user_info:
        print(f"User ID: {user_info['id']}")
        print(f"Name: {user_info['name']}")
        print(f"Email: {user_info['email']}")
    else:
        print("User not found.")
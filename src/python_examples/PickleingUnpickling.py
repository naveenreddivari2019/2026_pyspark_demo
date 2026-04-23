
import pickle

data = {"name": "Naveen", "age": 30}

with open("data.txt", "wb") as f:
    pickle.dump(data, f)   # pickling


with open("data.txt", "rb") as f:
    loaded_data = pickle.load(f)   # unpickling
    print(loaded_data)  # {'name': 'Naveen', 'age': 30}
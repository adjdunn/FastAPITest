# Import FastAPI
from fastapi import FastAPI
from db_functions import get_data

# Create an instance of the FastAPI class
app = FastAPI()

# Define a root endpoint that handles GET requests
@app.get("/")
def read_root():
    return {"Hello": "World"}

# Define a path operation for /items/{item_id}
@app.get("/news")
def read_item():
    data = get_data()
    return data


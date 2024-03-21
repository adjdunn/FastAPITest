# Import FastAPI
from fastapi import FastAPI

# Create an instance of the FastAPI class
app = FastAPI()

# Define a root endpoint that handles GET requests
@app.get("/")
def read_root():
    return {"Hello": "World"}

# Define a path operation for /items/{item_id}
@app.get("/items/{item_id}")
def read_item(item_id: int, q: str = None):
    return {"item_id": item_id, "q": q}

# Define a path operation for /greet/{name}
@app.get("/greet/{name}")
def read_greeting(name: str):
    return {"greeting": f"Hello, {name}!"}

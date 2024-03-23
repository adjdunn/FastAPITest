# Import FastAPI
from fastapi import FastAPI, Query
from typing import Optional
from db_functions import get_data, api_call

# Create an instance of the FastAPI class
app = FastAPI()

# Define a root endpoint that handles GET requests
@app.get("/")
def read_root():
    return {"Hello": "World"}

# Define a path operation for /items/{item_id}
@app.get("/news")
def get_news(sectors: Optional[str] = None,
             regions: Optional[str] = None,
             sizes: Optional[str] = None,
             date: Optional[str] = None):
    
    data = api_call(sectors, regions, sizes, date)
    return data


@app.get("/count")
def read_item():
    data = get_data()
    return str(len(data))
    


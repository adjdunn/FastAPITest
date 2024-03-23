# Import FastAPI
from fastapi import FastAPI, Query, Header, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from db_functions import get_data, api_call

# Create an instance of the FastAPI class
app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins for demonstration; specify your domain in production
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Define a root endpoint that handles GET requests
@app.get("/")
def read_root():
    return {"Hello": "World"}


def validate_api_key(api_key: str = Header(...)):
    # Replace 'your_api_key' with your validation logic
    if api_key != "your_api_key":
        raise HTTPException(status_code=401, detail="Invalid API Key")



@app.get("/news", dependencies=[Depends(validate_api_key)])
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
    


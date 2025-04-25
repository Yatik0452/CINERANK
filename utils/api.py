from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from passlib.context import CryptContext
from datetime import datetime, timedelta
from pydantic import BaseModel
from dotenv import load_dotenv
import os
from fastapi.middleware.cors import CORSMiddleware
import json
from fastapi import Response

from utils.datasetup import AzureDB


load_dotenv()

# Security
SECRET_KEY = os.environ.get('JWT_SECRET_KEY')
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


app = FastAPI()

# List of allowed origins
origins = ["http://127.0.0.1:5500", "https://etl-tutorial.vercel.app"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Password context
pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Database simulation
users_db = {
    "manager1": {
        "username": "manager1",
        "full_name": "Manager One",
        "hashed_password": pwd_context.hash("managerpass"),
        "roles": ["manager"],
        "id": 0
    },
    "john": {
        "username": "john",
        "full_name": "John Smith",
        "hashed_password": pwd_context.hash("1234"),
        "roles": ["employee"],
        "id": 1
    },
    "bob": {
        "username": "bob",
        "full_name": "Bob Wong",
        "hashed_password": pwd_context.hash("1234"),
        "roles": ["employee"],
        "id": 2
    },
    "ann": {
        "username": "ann",
        "full_name": "Ann Li",
        "hashed_password": pwd_context.hash("1234"),
        "roles": ["employee"],
        "id": 3
    }
}

# SQL database access
database=AzureDB()
database.access_container("example-data")

class Token(BaseModel):
    access_token: str
    token_type: str

class User(BaseModel):
    username: str
    roles: list
    id: int

# Authentication functions
def authenticate_user(username: str, password: str):
    user = users_db.get(username)
    if not user:
        return False
    if not pwd_context.verify(password, user['hashed_password']):
        return False
    return user

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta if expires_delta else datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def add_cors_headers(response: Response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'POST, GET, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Authorization, Content-Type'
    return response

@app.post("/token", response_model=Token)
async def login_for_access_token(response: Response, form_data: OAuth2PasswordRequestForm = Depends()):
    response = add_cors_headers(response)   
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user['username'], "roles": user['roles'], "id": user['id'], "name": user['full_name']},
        expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        user_data = users_db.get(username, None)
        if user_data is None:
            raise credentials_exception
        return User(username=user_data['username'], roles=user_data['roles'], id=user_data['id'])
    except JWTError:
        raise credentials_exception

def check_user_role(role: str):
    def role_checker(current_user: User = Depends(get_current_user)):
        if role not in current_user.roles:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return current_user
    return role_checker

# API endpoints
@app.get("/")
async def hello():
    return json.dumps({"data": "This is ETL implementation API"})

@app.get("/data/common")
async def read_common_data(response: Response, current_user: User = Depends(get_current_user)):
    response = add_cors_headers(response) 
    return json.dumps({"data": "This is common data available to all authenticated users"})

@app.get("/data/employee")
async def read_employee_data(response: Response, current_user: User = Depends(check_user_role("employee"))):
    
    response = add_cors_headers(response) 
    id = current_user.id
    total_pay1 = f'''
        SELECT date, SUM([work payment]) as Hourly_Pay, SUM([travel allowance amount]) as Travel_Pay, SUM([weather allowance amount]) as Weather_Pay, SUM([total pay this job]) as Total_Pay  
        FROM [dbo].[Total_Pay_Fact] 
        JOIN [dbo].[Date_dim] ON [dbo].[Total_Pay_Fact].Date_id = [dbo].[Date_dim].Date_id
        WHERE [dbo].[Total_Pay_Fact].Staff_id = {id}
        GROUP BY date
    '''
    
    total_pay2 = f'''
        SELECT date, SUM([work hours]) as Total_Hours 
        FROM [dbo].[Total_Pay_Fact] 
        JOIN [dbo].[Date_dim] ON [dbo].[Total_Pay_Fact].Date_id = [dbo].[Date_dim].Date_id
        WHERE [dbo].[Total_Pay_Fact].Staff_id = {id}
        GROUP BY date
    '''
    queries = [total_pay1, total_pay2]
    return json.dumps([database.get_sql_table(query) for query in queries])

@app.get("/data/manager")
async def read_manager_data(response: Response, current_user: User = Depends(check_user_role("manager"))):
    response = add_cors_headers(response) 
    total_pay1 = '''
        SELECT Name, SUM([work payment]) as Hourly_Pay, SUM([travel allowance amount]) as Travel_Pay, SUM([weather allowance amount]) as Weather_Pay 
        FROM [dbo].[Total_Pay_Fact] 
        JOIN [dbo].[Staff_dim] ON [dbo].[Total_Pay_Fact].Staff_id = [dbo].[Staff_dim].Staff_id
        GROUP BY Name
    '''
    
    total_pay2 = '''
        SELECT Name, SUM([total pay this job]) as Total_Pay 
        FROM [dbo].[Total_Pay_Fact] 
        JOIN [dbo].[Staff_dim] ON [dbo].[Total_Pay_Fact].Staff_id = [dbo].[Staff_dim].Staff_id
        GROUP BY Name
    '''
    queries = [total_pay1, total_pay2]
    return json.dumps([database.get_sql_table(query) for query in queries])
    
# Running the app with Uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, port=8000)

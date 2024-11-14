from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.orm import sessionmaker
from fastapi import FastAPI, HTTPException
from typing import List

# SERVER = '163.188.20.58\STUDIODB'
# DATABASE = 'DVN'
# USERNAME = 'sks_admin'
# PASSWORD = 'sks_admin'

SERVER = 'db'
DATABASE = 'employees'
USERNAME = 'mckenzie'
PASSWORD = 'password1'
PORT = "3306"



# DATABASE_URL = f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
DATABASE_URL = f"mysql+pymysql://{USERNAME}:{PASSWORD}@{SERVER}:{PORT}/{DATABASE}"
# DATABASE_URL=f"mysql+mysqlconnector://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}"
engine = create_engine(DATABASE_URL)


# Reflect the existing database
metadata = MetaData()
metadata.reflect(bind=engine)

# Create a session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# inspector = inspect(engine)
# table_names = inspector.get_table_names()
# print(table_names)

# Define a FastAPI app
app = FastAPI()


# Function to get all table names
def get_table_names():
    inspector = inspect(engine)
    return inspector.get_table_names()

# Endpoint to get table names
@app.get("/tables", response_model=List[str])
async def get_tables():
    try:
        tables = get_table_names()
        return tables
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint to get columns for a specific table
@app.get("/columns/{table_name}", response_model=List[str])
async def get_columns(table_name: str):
    try:
        # Inspect the table for columns
        inspector = inspect(engine)
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        if not columns:
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found.")
        return columns
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
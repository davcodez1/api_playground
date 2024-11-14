from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.orm import sessionmaker
from fastapi import FastAPI, HTTPException
from typing import List





SERVER = 'SLB-HVR0MN3\SQLEXPRESS'
DATABASE = 'IGNITE_TEST'
USERNAME = 'sa'
PASSWORD = 'ssms_admin'


DATABASE_URL = f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
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

TABLE_NAMES = list(metadata.tables.keys())  # List of table names
TABLE_COLUMNS = {table_name: [col.name for col in metadata.tables[table_name].columns]
                 for table_name in TABLE_NAMES}  # Columns for each table

# Function to get all table names
def get_table_names():
    return TABLE_NAMES

def get_columns(table_name: str):
    if table_name in TABLE_COLUMNS:
        return TABLE_COLUMNS[table_name]  # ['id', 'name', 'email'] for 'users'
    else:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")

# def get_db():
#     db: Session = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()

# Endpoint to get table names
@app.get("/tables", response_model=List[str])
async def get_tables():
    try:
        tables = get_table_names()
        return tables
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint to get columns for a specific table
# @app.get("/columns/{table_name}", response_model=List[str])
# async def get_columns(table_name: str):
#     try:
#         # Inspect the table for columns
#         inspector = inspect(engine)
#         columns = [col['name'] for col in inspector.get_columns(table_name)]
#         if not columns:
#             raise HTTPException(status_code=404, detail=f"Table {table_name} not found.")
#         return columns
#     except Exception as e:
    

@app.get("/{table_name}/{col_name}", response_model=List[str])
async def get_column_data(table_name: str, col_name: str):
    try:
        # Inspect the table for columns
        #Retrieve the cached table name in TABLE_NAMES
        tables = get_table_names()
        if table_name not in tables:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")
        
        # if the column does exist we want to call the get_columns func and assign the columns of that table to columns
        columns = get_columns(table_name)
        #if the col name the user is trying to access is not in the dict throw error
        if col_name not in columns:
            raise HTTPException(status_code=404, detail=f"Column '{col_name}' not found in table '{table_name}'.")
        
        # Get the table object from metadata
        table = metadata.tables[table_name]

        # Create a database session
        with SessionLocal() as db_session:
            # Query the specific column data from the table
            results = db_session.query(table.c[col_name]).all()

        # Return the data as a list of values
        return [result[0] for result in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
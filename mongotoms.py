import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient
from datetime import datetime, timedelta

# Connect to MongoDB
mongo_uri = "mongodb://log:pass@ip/db?authSource=admin"
client = MongoClient(mongo_uri)
db = client["db"]
collection = db["table"]

# Calculate the date 14 days ago
date_threshold = datetime.now() - timedelta(days=14)

# Extract data from the last 14 days
data = list(collection.find({"created_at": {"$gte": date_threshold}}))
df = pd.DataFrame(data)

# Connect to MSSQL using the provided connection string
conn_str = 'mssql+pyodbc://log:pass@ip/db?driver=ODBC Driver 17 for SQL Server'

# Create a SQLAlchemy engine for MSSQL
engine = create_engine(conn_str)

# Convert DataFrame to match the data types of the MSSQL table
df['_id'] = df['_id'].astype(str)
df['col1'] = df['col1'].astype(str)
df['col2'] = df['col2'].astype(str)
df['col3'] = df['col3'].astype(str)
df['col4'] = df['col4'].astype(int)
# You can add columns ...

# Load data into a temporary table
df.to_sql('table', engine, if_exists='replace', index=False)

print("Load end.")

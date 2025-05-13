import os
from dotenv import load_dotenv
from pymongo import MongoClient

# 1) Load .env file
load_dotenv()

# 2) Baca connection string
CONN_STR = os.getenv("CONNECTION_STRING")
if not CONN_STR:
    raise ValueError("Missing CONNECTION_STRING in .env")

# 3) Buat client & db
_client = MongoClient(CONN_STR)
_db     = _client.get_database("groceries")

def get_mongo_db():
    """Return object database 'groceries'."""
    return _db

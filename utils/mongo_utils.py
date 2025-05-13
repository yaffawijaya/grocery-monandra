# utils/mongo_utils.py
import os
from dotenv import load_dotenv
from pymongo import MongoClient

# Load .env
load_dotenv()
CONN_STR = os.getenv("CONNECTION_STRING")
if not CONN_STR:
    raise ValueError("Missing CONNECTION_STRING in .env")

_client = MongoClient(CONN_STR)
_db     = _client.get_database("groceries")

def get_mongo_db():
    """
    Return object for 'groceries' MongoDB database.
    """
    return _db
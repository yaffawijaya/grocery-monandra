# utils/mongo_index_utils.py
import os
from dotenv import load_dotenv
from pymongo import MongoClient

# Load .env
load_dotenv()
conn_str = os.getenv("CONNECTION_STRING")
if not conn_str:
    raise ValueError("Missing CONNECTION_STRING in .env file")

# Connect to MongoDB and select new database 'indexed_groceries'
client = MongoClient(conn_str)
db = client['indexed_groceries']


def init_indexed_groceries():
    """
    Initialize 'indexed_groceries' database:
      - Create collections 'cabang_toko' and 'karyawan'.
      - Create indexes for performance testing.
      - Insert sample documents.
    """
    # Collections
    cabang = db['cabang_toko']
    karyawan = db['karyawan']

    # Ensure collections exist
    if 'cabang_toko' not in db.list_collection_names():
        db.create_collection('cabang_toko')
    if 'karyawan' not in db.list_collection_names():
        db.create_collection('karyawan')

    # Create indexes
    cabang.create_index('lokasi')         # index on lokasi field
    karyawan.create_index('id_cabang')    # non-unique index on id_cabang

    # Insert sample branches
    cabang.insert_many([
        {'_id': 'CB01', 'nama_cabang': 'Cabang A', 'lokasi': 'Jambi - Sungai Sipin', 'contact': '021-1234567'},
        {'_id': 'CB02', 'nama_cabang': 'Cabang B', 'lokasi': 'Jakarta Timur - Duren Sawit', 'contact': '022-7654321'}
    ], ordered=False)

    # Insert sample employees
    karyawan.insert_many([
        {'_id': 'KR001', 'nama': 'Yaffa',   'id_cabang': 'CB01'},
        {'_id': 'KR002', 'nama': 'Aqiela',  'id_cabang': 'CB01'},
        {'_id': 'KR003', 'nama': 'Dimitri', 'id_cabang': 'CB02'},
        {'_id': 'KR004', 'nama': 'Cinta',   'id_cabang': 'CB02'}
    ], ordered=False)

    print("Init done bro! MongoDB is rock n rollin' on 'indexed_groceries'!")


if __name__ == '__main__':
    init_indexed_groceries()
from mongo_utils import get_mongo_db

db        = get_mongo_db()
cabang    = db["cabang_toko"]
karyawan  = db["karyawan"]

cabang.create_index("lokasi")
karyawan.create_index("id_cabang")

cabang.insert_many([
    {
      "_id": "CB01",
      "nama_cabang": "Cabang A",
      "lokasi": "Jambi - Sungai Sipin",
      "contact": "021-1234567"
    },
    {
      "_id": "CB02",
      "nama_cabang": "Cabang B",
      "lokasi": "Jakarta Timur - Duren Sawit",
      "contact": "022-7654321"
    }
], ordered=False)

karyawan.insert_many([
    {
      "_id": "KR001",
      "nama": "Yaffa",
      "id_cabang": "CB01"
    },
    {
      "_id": "KR002",
      "nama": "Aqiela",
      "id_cabang": "CB01"
    },
    {
      "_id": "KR003",
      "nama": "Dimitri",
      "id_cabang": "CB02"
    },
    {
      "_id": "KR004",
      "nama": "Cinta",
      "id_cabang": "CB02"
    }
], ordered=False)

print("Init done bro! MongoDB is rock n rollin'!")

# Grocery Monandra DB Execution Time Monitor

Panduan lengkap untuk setup dan menjalankan *Streamlit* app yang memonitor execution time query ke Cassandra dan MongoDB.

## 📁 Struktur Direktori

```
CATATAN_TUBES_ROBD/
│   app.py
│   README.md
├── data/
│   ├── Cassandra/
│   │   ├── gen_cql.py
│   │   └── inserts.cql
│   └── MongoDB/
│       ├── .env
│       ├── init_mongo.py
│       └── mongo_utils.py
└── utils/
    ├── cassandra_utils.py
    └── mongo_utils.py
```

## 🔧 Prasyarat

* Docker
* Conda atau virtualenv
* Python 3.11
* `pip`

## 1. Setup Environment Python

1. Buat dan aktifkan Conda environment dengan Python 3.11:

   ```bash
   conda create -n monandra311 python=3.11 -y
   conda activate monandra311
   ```
2. Install dependency:

   ```bash
   pip install \
     cassandra-driver \
     pymongo python-dotenv \
     streamlit pandas matplotlib
   ```

## 2. Setup Cassandra

1. Pull image dan buat network Docker:

   ```bash
   docker pull cassandra:latest
   docker network create cassandra
   ```
2. Jalankan container Cassandra dengan port mapping:

   ```bash
   docker run -d \
     --name cassandra \
     --network cassandra \
     -p 9042:9042 \
     cassandra:latest
   ```
3. Tunggu 30–60 detik hingga Cassandra siap, kemudian buat keyspace & tabel:

   ```bash
   docker exec -it cassandra cqlsh
   ```

   Di prompt `cqlsh>` jalankan:

   ```sql
   CREATE KEYSPACE IF NOT EXISTS groceries
     WITH replication = {'class':'SimpleStrategy','replication_factor':1};

   USE groceries;

   CREATE TABLE IF NOT EXISTS transaksi_harian (
     id_transaksi_harian uuid PRIMARY KEY,
     id_transaksi        text,
     id_cabang           text,
     id_karyawan         text,
     tanggal             date,
     nama_barang         text,
     qty                 int,
     harga_barang        decimal,
     total_transaksi     decimal
   );

   CREATE INDEX IF NOT EXISTS idx_transaksi_cabang
     ON transaksi_harian (id_cabang);
   ```
4. (Opsional) Import data bulk via `data/Cassandra/inserts.cql`:

   ```bash
   docker exec -i cassandra cqlsh -k groceries < data/Cassandra/inserts.cql
   ```

## 3. Setup MongoDB

1. Tambahkan file `.env` di `data/MongoDB/` dengan isi:

   ```dotenv
   CONNECTION_STRING="mongodb+srv://ilokuda:<passwordnya_masukin>@cluster-experiment-yaff.bfurl13.mongodb.net/?retryWrites=true&w=majority&appName=cluster-experiment-yaffa"
   ```
2. Jalankan script inisialisasi:

   ```bash
   cd data/MongoDB
   python init_mongo.py
   ```

   Ini akan membuat koleksi `cabang_toko` & `karyawan` beserta data awal.

## 4. Struktur Modul Python

* `utils/cassandra_utils.py`: koneksi ke Cassandra (`Cluster(contact_points=['127.0.0.1'], port=9042)`).
* `utils/mongo_utils.py`: koneksi ke MongoDB (baca `CONNECTION_STRING` dari `.env`).
* `app.py`: *Streamlit* app utama.

## 5. Menjalankan Aplikasi

Dari direktori root (`CATATAN_TUBES_ROBD/`):

```bash
conda activate monandra311   # atau virtualenv yang sesuai
docker ps                   # pastikan container cassandra berjalan
python -m streamlit run app.py
```

Buka browser di `http://localhost:8501`.

## 6. Cara Penggunaan

1. Pilih cabang (`CB01` atau `CB02`) di sidebar.
2. Klik:

   * **Query Cassandra** → tampilkan DataFrame & waktu.
   * **Query MongoDB** → tampilkan DataFrame & waktu.
   * **Run Benchmark All** → perbandingan waktu dalam bar chart.

---

## 🛠 Troubleshooting

* **`NoHostAvailable`**: pastikan port 9042 ter-publish dan container Cassandra sudah siap.
* **`ALLOW FILTERING`**: sudah disertakan di query Cassandra untuk filter by `id_cabang`.
* **Environment**: wajib Python 3.11 agar driver Cassandra dapat import `asyncore`.

---

Dokumentasi ini memudahkan setup ulang dan memastikan app dapat berjalan tanpa kendala. Selamat mencoba!

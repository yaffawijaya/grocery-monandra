import random
from datetime import date, timedelta

# ── Konfigurasi ──────────────────────────────────────────────────────────────
NUM_ROWS         =  2000
UNIQUE_TXN_COUNT = 300

branches   = ['CB01', 'CB02']
employees  = ['KR01','KR02','KR03','KR04']
items      = ['Beras','Gula','Minyak Goreng','Telur','Roti',
              'Susu','Teh','Garam','Gula Pasir','Kopi']

# Harga konsisten per barang
price_map = {
    'Beras':       10000,
    'Gula':        8000,
    'Minyak Goreng':12000,
    'Telur':       2000,
    'Roti':        5000,
    'Susu':        7000,
    'Teh':         3000,
    'Garam':       2500,
    'Gula Pasir':  8500,
    'Kopi':        15000
}

start_date = date(2025, 5, 1)

# ── 1) Mapping unik transaksi → (cabang, karyawan)
txn_map = {}
for i in range(1, UNIQUE_TXN_COUNT + 1):
    cabang   = random.choice(branches)
    karyawan = random.choice(employees)
    txn_id   = f"{cabang}-{karyawan}-{i:010d}"
    txn_map[txn_id] = (cabang, karyawan)

txn_ids = list(txn_map.keys())

# ── 2) Generate minimal satu baris per transaksi
rows = []
for txn_id, (cabang, karyawan) in txn_map.items():
    tg      = start_date + timedelta(days=random.randint(0, 29))
    item    = random.choice(items)
    qty     = random.randint(1, 10)
    price   = price_map[item]
    total   = qty * price

    rows.append(f"""INSERT INTO groceries.transaksi_harian (
  id_transaksi_harian,
  id_transaksi,
  id_cabang,
  id_karyawan,
  tanggal,
  nama_barang,
  qty,
  harga_barang,
  total_transaksi
) VALUES (
  uuid(),
  '{txn_id}',
  '{cabang}',
  '{karyawan}',
  '{tg.isoformat()}',
  '{item}',
  {qty},
  {price},
  {total}
);""")

# ── 3) Tambah baris hingga NUM_ROWS
extra = NUM_ROWS - len(rows)
for _ in range(extra):
    txn_id   = random.choice(txn_ids)
    cabang, karyawan = txn_map[txn_id]
    tg       = start_date + timedelta(days=random.randint(0, 29))
    item     = random.choice(items)
    qty      = random.randint(1, 10)
    price    = price_map[item]
    total    = qty * price

    rows.append(f"""INSERT INTO groceries.transaksi_harian (
  id_transaksi_harian,
  id_transaksi,
  id_cabang,
  id_karyawan,
  tanggal,
  nama_barang,
  qty,
  harga_barang,
  total_transaksi
) VALUES (
  uuid(),
  '{txn_id}',
  '{cabang}',
  '{karyawan}',
  '{tg.isoformat()}',
  '{item}',
  {qty},
  {price},
  {total}
);""")

# ── 4) Acak & cetak semua CQL
random.shuffle(rows)
print("-- generated CQL inserts for transaksi_harian")
for cql in rows:
    print(cql)

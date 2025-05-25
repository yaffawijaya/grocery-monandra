# data/Cassandra/setup_indexed_table.py
from cassandra_utils import get_cassandra_session
import uuid, random
from datetime import date, timedelta

# Configuration
NUM_ROWS = 2000
UNIQUE_TXN_COUNT = 300
BRANCHES = ['CB01', 'CB02']
EMPLOYEES = ['KR01', 'KR02', 'KR03', 'KR04']
ITEMS = ['Beras','Gula','Minyak Goreng','Telur','Roti','Susu','Teh','Garam','Gula Pasir','Kopi']
PRICE_MAP = {
    'Beras':        10000,
    'Gula':         8000,
    'Minyak Goreng':12000,
    'Telur':        2000,
    'Roti':         5000,
    'Susu':         7000,
    'Teh':          3000,
    'Garam':        2500,
    'Gula Pasir':   8500,
    'Kopi':         15000
}
start_date = date(2025, 5, 1)

target_table = "groceries.indexed_transaksi_harian"


def create_indexed_cassandra_table():
    """
    Create optimized table with partition on id_cabang and clustering by tanggal, id_transaksi_harian.
    """
    session = get_cassandra_session()
    session.execute(f"DROP TABLE IF EXISTS {target_table}")
    cql = f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
      id_cabang text,
      tanggal date,
      id_transaksi_harian uuid,
      id_karyawan text,
      nama_barang text,
      qty int,
      harga_barang decimal,
      total_transaksi decimal,
      PRIMARY KEY ((id_cabang), tanggal, id_transaksi_harian)
    ) WITH CLUSTERING ORDER BY (tanggal DESC);
    """
    session.execute(cql)
    print(f"Table {target_table} created")


def generate_indexed_cassandra_data():
    """
    Generate the same synthetic dataset used for transaksi_harian, now into indexed_transaksi_harian.
    """
    # 1) Build unique mapping txn_id -> (cabang, karyawan)
    txn_map = {}
    for i in range(1, UNIQUE_TXN_COUNT+1):
        cabang = random.choice(BRANCHES)
        karyawan = random.choice(EMPLOYEES)
        txn_id = f"{cabang}-{karyawan}-{i:010d}"
        txn_map[txn_id] = (cabang, karyawan)
    txn_ids = list(txn_map.keys())

    # 2) Create at least one row per transaction
    rows = []
    for txn_id, (cabang, karyawan) in txn_map.items():
        tg = start_date + timedelta(days=random.randint(0, 29))
        item = random.choice(ITEMS)
        qty = random.randint(1, 10)
        harga = PRICE_MAP[item]
        total = qty * harga
        rows.append((cabang, tg, uuid.uuid4(), karyawan, item, qty, harga, total))

    # 3) Fill remaining rows up to NUM_ROWS
    extra = NUM_ROWS - len(rows)
    for _ in range(extra):
        txn_id = random.choice(txn_ids)
        cabang, karyawan = txn_map[txn_id]
        tg = start_date + timedelta(days=random.randint(0, 29))
        item = random.choice(ITEMS)
        qty = random.randint(1, 10)
        harga = PRICE_MAP[item]
        total = qty * harga
        rows.append((cabang, tg, uuid.uuid4(), karyawan, item, qty, harga, total))

    random.shuffle(rows)

    # 4) Insert into Cassandra
    session = get_cassandra_session()
    insert_cql = f"""
    INSERT INTO {target_table} (
      id_cabang, tanggal, id_transaksi_harian, id_karyawan,
      nama_barang, qty, harga_barang, total_transaksi
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """
    stmt = session.prepare(insert_cql)
    for rec in rows:
        session.execute(stmt, rec)

    print(f"✅ Inserted {len(rows)} rows into {target_table}")


if __name__ == '__main__':
    create_indexed_cassandra_table()
    generate_indexed_cassandra_data()

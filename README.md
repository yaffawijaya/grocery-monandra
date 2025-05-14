# Grocery Monandra DB Execution Time Monitor

This repository contains a Streamlit application designed to measure and compare query execution times against two NoSQL databases: Cassandra and MongoDB. It supports both raw and indexed access patterns, as well as custom query execution and aggregation scenarios.

## App Demos
### Cassandra Execution Time Monitoring
![Cassandra Page](https://raw.githubusercontent.com/yaffawijaya/grocery-monandra/refs/heads/master/assets/cassandra.png)

### MongoDB Execution Time Monitoring
![MongoDB Page](https://raw.githubusercontent.com/yaffawijaya/grocery-monandra/refs/heads/master/assets/mongodb.png)

## Project Structure

```
grocery-monandra
│   app.py
│   README.md
│
├── data
│   ├── Cassandra
│   │   ├── gen_cql.py           # Script to generate initial CQL inserts
│   │   └── inserts.cql          # Sample insert statements
│   └── MongoDB
│       ├── .env                 # Environment variables for MongoDB connection
│       ├── .gitignore
│       ├── init_mongo.py        # Initial data loader for MongoDB
│       └── mongo_utils.py       # Helper for MongoDB connection
│
└── utils
    ├── .env                     # Additional environment settings
    ├── .gitignore
    ├── cassandra_utils.py       # Cassandra connection helper
    ├── mongo_utils.py           # MongoDB connection helper
    ├── mongo_index_utils.py     # Script to create indexed MongoDB collections
    ├── setup_indexes_cassandra.py  # Script to create and populate indexed Cassandra table
```

## Recent Progress and Features

- **Database Setup Automation**: Scripts under `data/` and `utils/` automate the creation of keyspaces, tables, collections, and indexes for both databases.
- **Indexed vs Non‑indexed Workflows**: Separate pipelines and tables for raw and indexed access patterns in Cassandra (`transaksi_harian` vs `indexed_transaksi_harian`), plus separate MongoDB databases (`groceries` vs `indexed_groceries`).
- **Streamlit Dashboard** (`app.py`):
  - Side‑by‑side benchmarking for Cassandra and MongoDB.
  - Custom query/filter inputs with recommended examples in collapsible side panels.
  - Execution time measurement includes full result fetch to ensure accurate timing.
  - Aggregation scenarios for Cassandra (top sales, averages, daily counts) alongside simple retrieval workflows.
- **State Persistence**: Results and timings are retained in session state so that tables and charts remain visible until explicitly cleared or the app is refreshed.
- **Visualization**: Dynamic bar charts rendered with Streamlit’s built‑in charting for clear comparison between non‑indexed and indexed runs.

## Prerequisites

- Docker (for Cassandra container)
- Conda or any Python 3.11 environment
- `pip` for dependency installation

## Environment Setup

1. **Create Python environment** (example with Conda):
   ```bash
   conda create -n monandra311 python=3.11 -y
   conda activate monandra311
   ```
2. **Install Python packages**:
   ```bash
   pip install cassandra-driver pymongo python-dotenv streamlit pandas
   ```

## Project Clone
1. Clone from github:
   ```bash
   git clone https://github.com/yaffawijaya/grocery-monandra.git
   cd grocery-monandra
   ```

## Data Initialization

### Cassandra

1. Pull and run the Docker container:
   ```bash
   docker pull cassandra:latest
   docker network create cassandra
   docker run -d --name cassandra --network cassandra -p 9042:9042 cassandra:latest
   ```
2. Create keyspace, table, and index in one interactive session:
   ```bash
   docker exec -it cassandra cqlsh
   ```
   in `cqlsh` or cql shell:
   ```cqlsh
   -- 1) Create keyspace
   CREATE KEYSPACE IF NOT EXISTS groceries
     WITH replication = {'class':'SimpleStrategy','replication_factor':1};

   -- 2) Use the keyspace
   USE groceries;

   -- 3) Create the raw transactions table
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

   -- 4) Create a secondary index on branch
   CREATE INDEX IF NOT EXISTS idx_transaksi_cabang
     ON transaksi_harian (id_cabang);

   -- 5) Then exit shell
   exit
   ```
3. Generate and import sample data:
   - Run the generator script to produce `inserts.cql`:
     ```bash
     python data/Cassandra/gen_cql.py > data/Cassandra/inserts.cql
     ```
   - Load the data into Cassandra:
     ```bash
     docker exec -i cassandra cqlsh -k groceries < data/Cassandra/inserts.cql
     ```
4. Set up the indexed table and populate it:
   ```bash
   python utils/setup_indexes_cassandra.py
   ```

### MongoDB

1. Prepare the connection string in `data/MongoDB/.env` & `utils/.env`:
   ```env
   CONNECTION_STRING="mongodb+srv://<username>:<password>@cluster-experiment-yaff.bfurl13.mongodb.net/?retryWrites=true&w=majority&appName=cluster-experiment-yaffa"
   ```
2. Load initial collections into `groceries` database:
   ```bash
   cd data/MongoDB
   python init_mongo.py
   ```
3. Create and index the `indexed_groceries` database:
   ```bash
   python utils/mongo_index_utils.py
   ```

## Running the Application


From the project root:

```bash
conda activate monandra311  # or activate your Python 3.11 env
docker ps                  # ensure Cassandra is running
streamlit run app.py
```

Access the app at `http://localhost:8501`. Use the sidebar to switch between Cassandra and MongoDB benchmarks, toggle custom queries, and expand recommended examples.

## Usage Notes

- For Cassandra, timing includes full page fetch by converting the result set to a list.
- For MongoDB, `list(cursor)` triggers a full fetch of matching documents.
- Aggregation queries for Cassandra are included under recommended examples, such as top‑N sales by employee and daily transaction counts.

## Troubleshooting

- **Connection errors**: Verify that Docker container for Cassandra is running and port 9042 is mapped.
- **Missing indexes**: Run the setup scripts in `utils/` to regenerate indexed tables/collections.
- **Unexpected performance**: Ensure you use filters or queries that can leverage the created indexes to see a meaningful speedup.

---

This README should help you replicate the environment, understand the available features, and extend the benchmarking scenarios as needed.

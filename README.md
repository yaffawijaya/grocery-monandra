# NoSQL Database Lab: Interaction & Performance Monitor (ROSBD Project)

This repository contains a Streamlit application developed for the "Rekayasa dan Organisasi Sistem Big Data (ROSBD)" university project. The application serves as an interactive platform to demonstrate, benchmark, and interact with two NoSQL databases: Cassandra (a columnar store) and MongoDB (a document store), using a **Groceries** domain.

It showcases data modeling, the impact of indexing on query performance, allows for flexible data manipulation and querying, and provides combined analytics by fetching and processing data from both database systems.

## Application Features

* **Home Page:** Introduces the project, its objectives for the ROSBD course, authors, and outlines the system architecture.
* **MongoDB Benchmark:** Compares query execution times (for Find, Aggregate, Count Documents) between non-indexed (`cabang`, `karyawan`) and indexed (`indexed_cabang`, `indexed_karyawan`) collections in MongoDB. Allows custom JSON-based query parameters.
* **MongoDB Playground:** An interactive interface to perform various CRUD (Create, Read, Update, Delete) and administrative operations on a user-specified MongoDB database and collection. Supports operations like creating/dropping collections, inserting documents, finding, updating, deleting, running aggregation pipelines, and managing indexes.
* **Cassandra Benchmark:** Compares CQL query execution times between a base table (`transaksi_harian`) and an optimized/indexed table (`indexed_transaksi_harian`). Supports custom CQL queries.
* **Combined Analytics:** A dedicated page to analyze employee performance by fetching sales and transaction data from Cassandra, enriching it with employee details from MongoDB, and presenting key insights and top performer rankings.
* **User Interface:**
    * Sidebar navigation with an expander for page selection.
    * Logos for MongoDB and Cassandra.
    * Use of expanders for detailed information, query inputs, and results to maintain a clean interface.
    * Dynamic bar charts for visual comparison of execution times.
    * Informative status messages and error handling.

## App Previews
*(These screenshots show the benchmark pages. The app now includes additional Home, Combined Analytics, and MongoDB Playground pages.)*

### Cassandra Execution Time Monitoring
![Cassandra Page](https://raw.githubusercontent.com/yaffawijaya/grocery-monandra/refs/heads/master/assets/cassandra.png)

### MongoDB Execution Time Monitoring
![MongoDB Page](https://raw.githubusercontent.com/yaffawijaya/grocery-monandra/refs/heads/master/assets/mongodb.png)

## Project Context: ROSBD Assignment

This application directly addresses requirements of the "Tugas Besar / Projek ROBD Spring 2025":
* **System Components:** Implements interaction with DB NoSQL 1 (Cassandra for daily transactions) and DB NoSQL 2 (MongoDB for employee/branch master data). The "Combined Analytics" page acts as a form of query aggregation by programmatically fetching and combining data from both.
* **Domain & Data:** Utilizes a "Groceries" domain with synthetically generated data for daily transactions, employees, and branches.
* **Indexing & Optimization:** The benchmark pages specifically demonstrate the performance differences between querying non-indexed and indexed data structures in both MongoDB and Cassandra.
* **Data Interaction:** The MongoDB Playground allows for a wide range of DDL and DML operations.

**Authors (Group A):**
* Yaffazka Afazillah Wijaya
* Dimitri Aulia Rasyidin
* Aqiela Putriana Shabira

## Project Structure

```
grocery-monandra/
│
├── app.py                     # Main Streamlit application
├── README.md                  # This file
│
├── notebooks/
│   ├── .env                   # MongoDB CONNECTION_STRING and other environment variables
│   ├── 01-generate-data.ipynb # Jupyter notebook to generate synthetic grocery data (outputs .xlsx)
│   └── 02-ingest-to-nosql.ipynb # Jupyter notebook to ingest data into MongoDB & Cassandra
│   └── synthetic_grocery_data.xlsx # Example output from 01-generate-data.ipynb
│
├── utils/
│   └── cassandra_utils.py     # Helper for Cassandra connection
│
└── assets/                    # Images for README
    ├── cassandra.png
    └── mongodb.png
```

## Prerequisites

* Docker (for running Cassandra locally)
* Python 3.9+ (Python 3.11 recommended as used in development)
* Conda (optional, for environment management) or `venv`
* Access to a MongoDB instance (local or Atlas)

## Environment Setup

1.  **Clone the Repository:**
    ```bash
    git clone [https://github.com/yaffawijaya/grocery-monandra.git](https://github.com/yaffawijaya/grocery-monandra.git)
    cd grocery-monandra
    ```

2.  **Create Python Environment** (example with Conda):
    ```bash
    conda create -n monandra_env python=3.11 -y
    conda activate monandra_env
    ```
    Alternatively, use `venv`:
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3.  **Install Python Packages:**
    Create a `requirements.txt` file in the root of the project with the following content:
    ```txt
    streamlit
    pandas
    pymongo[srv]
    python-dotenv
    cassandra-driver
    # altair # Optional, if you extend with Altair charts
    openpyxl # For reading .xlsx in notebooks
    faker    # For data generation notebook
    jupyter  # For running notebooks
    ```
    Then install:
    ```bash
    pip install -r requirements.txt
    ```

## Data Initialization and Database Setup

The data used by this application is synthetically generated and then ingested into MongoDB and Cassandra using Jupyter notebooks provided in the `notebooks/` directory.

**1. Set up MongoDB Connection:**
   * Create a `.env` file inside the `notebooks/` directory (i.e., `notebooks/.env`).
   * Add your MongoDB connection string to it:
     ```env
     CONNECTION_STRING="your_mongodb_connection_string_here"
     ```
     (e.g., for MongoDB Atlas: `mongodb+srv://<username>:<password>@yourcluster.mongodb.net/?retryWrites=true&w=majority`)

**2. Run Cassandra Docker Container:**
   ```bash
   docker pull cassandra:latest
   docker network create cassandra-net # Create a network if you haven't
   # Check if container already exists: docker ps -a --filter "name=cassandra-node1"
   # If it exists and you want to start fresh: docker rm -f cassandra-node1
   docker run -d --name cassandra-node1 --network cassandra-net -p 9042:9042 cassandra:latest
   ```
   Wait a minute or two for Cassandra to initialize. You can check logs with `docker logs cassandra-node1`.

**3. Generate Synthetic Data:**
   * Open and run the `notebooks/01-generate-data.ipynb` notebook. This will create an Excel file (e.g., `synthetic_grocery_data.xlsx`) in the `notebooks/` directory.

**4. Ingest Data into Databases:**
   * Open and run the `notebooks/02-ingest-to-nosql.ipynb` notebook. This notebook will:
     * Connect to your Cassandra instance (running in Docker).
     * Create the `day_grocery` keyspace.
     * Create tables: `transaksi_harian` and `indexed_transaksi_harian`.
     * Ingest data from the Excel file into Cassandra tables.
     * Connect to your MongoDB instance (using the `CONNECTION_STRING` from `notebooks/.env`).
     * Create the `grocery_store_db` database (or your configured `DEFAULT_MONGO_DB_NAME`).
     * Create collections: `cabang`, `indexed_cabang`, `karyawan`, `indexed_karyawan`.
     * Ingest data from the Excel file into MongoDB collections and create specified indexes.

## Running the Streamlit Application

1.  Ensure your Cassandra Docker container is running.
2.  Ensure your MongoDB instance is accessible.
3.  Activate your Python environment:
    ```bash
    conda activate monandra_env  # Or source venv/bin/activate
    ```
4.  Navigate to the project root directory (`grocery-monandra/`).
5.  Run the Streamlit app:
    ```bash
    streamlit run app.py
    ```
The application will typically be available at `http://localhost:8501`.

## Usage Notes

* The **Home** page provides an overview of the project.
* **MongoDB Benchmark** and **Cassandra Benchmark** pages allow you to compare query performance on non-indexed versus indexed data structures. You can use recommended queries/filters or input your own.
* **MongoDB Playground** offers a flexible interface for direct DDL/DML operations on MongoDB.
* **Combined Analytics** demonstrates how data from both Cassandra and MongoDB can be merged to derive cross-database insights, such as employee performance.
* Execution times displayed include the time taken to fetch all results from the database.

## Troubleshooting

* **Connection Errors:**
    * **MongoDB:** Verify your `CONNECTION_STRING` in `notebooks/.env` is correct and your MongoDB instance is accessible (check IP whitelisting if using Atlas).
    * **Cassandra:** Ensure the Docker container (`cassandra-node1`) is running (`docker ps`) and port 9042 is correctly mapped.
* **Data Not Appearing:** Ensure you have successfully run the `01-generate-data.ipynb` and `02-ingest-to-nosql.ipynb` notebooks to populate the databases.
* **Streamlit Errors:** Check the terminal where you ran `streamlit run app.py` for detailed error messages. Ensure all dependencies in `requirements.txt` are installed.
import os
import time
import streamlit as st
import pandas as pd
from utils.cassandra_utils import get_cassandra_session
from pymongo import MongoClient
from dotenv import load_dotenv




# ----------------- Configuration -----------------
# Load MongoDB connection string
env_path = os.path.join(os.path.dirname(__file__), 'data', 'MongoDB', '.env')
load_dotenv(env_path)
conn_str = os.getenv('CONNECTION_STRING')
if not conn_str:
    st.error('Missing CONNECTION_STRING in .env file')
    st.stop()
# MongoDB clients
db_orig = MongoClient(conn_str)['groceries']
db_idx  = MongoClient(conn_str)['indexed_groceries']

# ----------------- Streamlit Setup -----------------
st.set_page_config(page_title='DB Execution Time Monitor', layout='wide')
st.title('🕑 DB Execution Time Monitor')

# ----------------- Sidebar -----------------
page = st.sidebar.radio('Select Benchmark', ['Cassandra', 'MongoDB'])
st.sidebar.markdown('---')
use_custom = st.sidebar.checkbox('Use Custom Queries/Filters')

if page == 'Cassandra':
    st.sidebar.markdown('**Basics:**')
    with st.sidebar.expander("📋 Recommended Cassandra Queries", expanded=False):
        st.markdown('**Basic Queries:**')
        st.code("SELECT * FROM transaksi_harian WHERE id_cabang = 'CB01' ALLOW FILTERING")
        st.code("SELECT * FROM indexed_transaksi_harian WHERE id_cabang = 'CB01'")
        st.code("SELECT * FROM transaksi_harian WHERE tanggal > '2025-05-10' ALLOW FILTERING")
        st.code("SELECT COUNT(*) FROM indexed_transaksi_harian WHERE id_cabang = 'CB02'")
    st.sidebar.markdown('**Aggregation Queries:**')
    with st.sidebar.expander("🔢 Top Sales by Employee", expanded=False):
        st.markdown('*Non-Indexed:*')
        st.code("SELECT id_karyawan, sum(total_transaksi) AS total_sales FROM transaksi_harian WHERE id_cabang='CB01' ALLOW FILTERING GROUP BY id_karyawan")
        st.markdown('*Indexed:*')
        st.code("SELECT id_karyawan, sum(total_transaksi) AS total_sales FROM indexed_transaksi_harian WHERE id_cabang='CB01'")
    with st.sidebar.expander("📊 Average Transaction Value per Branch", expanded=False):
        st.markdown('*Non-Indexed:*')
        st.code("SELECT id_cabang, avg(total_transaksi) AS avg_val FROM transaksi_harian ALLOW FILTERING GROUP BY id_cabang")
        st.markdown('*Indexed:*')
        st.code("SELECT id_cabang, avg(total_transaksi) AS avg_val FROM indexed_transaksi_harian GROUP BY id_cabang")
    with st.sidebar.expander("📆 Transactions per Day", expanded=False):
        st.markdown('*Non-Indexed:*')
        st.code("SELECT tanggal, count(*) AS cnt FROM transaksi_harian ALLOW FILTERING GROUP BY tanggal")
        st.markdown('*Indexed:*')
        st.code("SELECT tanggal, count(*) AS cnt FROM indexed_transaksi_harian GROUP BY tanggal")
else:
    with st.sidebar.expander("📋 Recommended MongoDB Filters", expanded=False):
        st.markdown('**Filters for MongoDB:**')
        st.code("{'id_cabang':'CB01'}")
        st.code("{'id_karyawan':'KR001'}")
        st.code("{'nama': /Ya/}")
        st.code("{'lokasi':'Jakarta Timur - Duren Sawit'}")

# ----------------- State Initialization -----------------
def init_state():
    keys = ['cas_non_df','cas_non_time','cas_idx_df','cas_idx_time',
            'mongo_non_df','mongo_non_time','mongo_idx_df','mongo_idx_time']
    for key in keys:
        if key not in st.session_state:
            st.session_state[key] = None
init_state()

# ----------------- Benchmark Functions -----------------
def run_cassandra_query(cql: str):
    session = get_cassandra_session()
    start = time.perf_counter()
    rows = session.execute(cql)
    duration = time.perf_counter() - start
    return pd.DataFrame(rows.current_rows), duration

def run_mongodb_query(db, flt: dict):
    coll = db['karyawan']
    start = time.perf_counter()
    docs = list(coll.find(flt))
    duration = time.perf_counter() - start
    return pd.DataFrame(docs), duration

# ----------------- Cassandra Page -----------------
if page == 'Cassandra':
    st.header('📊 Cassandra Benchmark')
    # Default or custom queries
    default_non = "SELECT * FROM transaksi_harian WHERE id_cabang = 'CB01' ALLOW FILTERING"
    default_idx = "SELECT * FROM indexed_transaksi_harian WHERE id_cabang = 'CB01'"
    if use_custom:
        q_non = st.text_area('Non-Indexed CQL', default_non, height=80)
        q_idx = st.text_area('Indexed CQL', default_idx, height=80)
    else:
        q_non, q_idx = default_non, default_idx

    # Run buttons
    if st.button('Run Non-Indexed'):
        df, t = run_cassandra_query(q_non)
        st.session_state.cas_non_df = df
        st.session_state.cas_non_time = t
    if st.button('Run Indexed'):
        df, t = run_cassandra_query(q_idx)
        st.session_state.cas_idx_df = df
        st.session_state.cas_idx_time = t

    # Display results
    if st.session_state.cas_non_df is not None:
        st.subheader('Non-Indexed Results')
        st.code(q_non)
        st.write(f'⏱️ Time: {st.session_state.cas_non_time:.4f} s')
        st.dataframe(st.session_state.cas_non_df)
    if st.session_state.cas_idx_df is not None:
        st.subheader('Indexed Results')
        st.code(q_idx)
        st.write(f'⏱️ Time: {st.session_state.cas_idx_time:.4f} s')
        st.dataframe(st.session_state.cas_idx_df)

    # Comparison
    if st.session_state.cas_non_time is not None and st.session_state.cas_idx_time is not None:
        st.subheader('⏲️ Execution Time Comparison')
        comp_df = pd.DataFrame(
            {'Non-Indexed': [st.session_state.cas_non_time], 'Indexed': [st.session_state.cas_idx_time]}
        )
        st.bar_chart(comp_df.T)

# ----------------- MongoDB Page -----------------
else:
    st.header('📊 MongoDB Benchmark')

    # Select which collection to query
    coll_name = st.selectbox('Select Collection', ['karyawan', 'cabang_toko'])

    # Define filters for original and indexed DBs
    default_non, default_idx = {}, {}
    if use_custom:
        txt_non = st.text_area('Original Filter (JSON)', default_non and '{}', height=80)
        txt_idx = st.text_area('Indexed Filter (JSON)', default_idx and '{}', height=80)
        try:
            flt_non = eval(txt_non)
        except Exception:
            st.error('Original filter parse error, using {}')
            flt_non = {}
        try:
            flt_idx = eval(txt_idx)
        except Exception:
            st.error('Indexed filter parse error, using {}')
            flt_idx = {}
    else:
        flt_non, flt_idx = {}, {}

    # Run Original DB query
    if st.button('Run Original', key='run_mongo_non'):
        start = time.perf_counter()
        docs = list(db_orig[coll_name].find(flt_non))
        duration = time.perf_counter() - start
        df = pd.DataFrame(docs)
        st.session_state.mongo_non_df = df
        st.session_state.mongo_non_time = duration

    # Run Indexed DB query
    if st.button('Run Indexed', key='run_mongo_idx'):
        start = time.perf_counter()
        docs = list(db_idx[coll_name].find(flt_idx))
        duration = time.perf_counter() - start
        df = pd.DataFrame(docs)
        st.session_state.mongo_idx_df = df
        st.session_state.mongo_idx_time = duration

    # Display Original Results
    if st.session_state.mongo_non_df is not None:
        st.subheader(f'Original DB Results: {coll_name}')
        st.code(f"db_orig['{coll_name}'].find({flt_non})")
        st.write(f'⏱️ Time: {st.session_state.mongo_non_time:.4f} s')
        st.dataframe(st.session_state.mongo_non_df)

    # Display Indexed Results
    if st.session_state.mongo_idx_df is not None:
        st.subheader(f'Indexed DB Results: {coll_name}')
        st.code(f"db_idx['{coll_name}'].find({flt_idx})")
        st.write(f'⏱️ Time: {st.session_state.mongo_idx_time:.4f} s')
        st.dataframe(st.session_state.mongo_idx_df)

    # Comparison chart
    if st.session_state.mongo_non_time is not None and st.session_state.mongo_idx_time is not None:
        st.subheader('⏲️ Execution Time Comparison')
        comp_df = pd.DataFrame(
            {'Original': [st.session_state.mongo_non_time], 'Indexed': [st.session_state.mongo_idx_time]},
            index=[coll_name]
        )
        st.bar_chart(comp_df.T)

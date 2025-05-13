# app.py
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

# Setup MongoDB clients: original and indexed
db_orig = MongoClient(conn_str)['groceries']
db_idx  = MongoClient(conn_str)['indexed_groceries']

# Initialize Streamlit page
st.set_page_config(page_title='DB Execution Time Monitor', layout='wide')
st.title('🕑 DB Execution Time Monitor')

# Sidebar: select benchmark and custom mode
page = st.sidebar.radio('Select Benchmark', ['Cassandra', 'MongoDB'])
st.sidebar.markdown('---')
use_custom = st.sidebar.checkbox('Use Custom Queries/Filters')

# Recommended test queries/filters
if page == 'Cassandra':
    st.sidebar.markdown('**Recommended Cassandra Queries for Benchmark:**')
    st.sidebar.code("SELECT * FROM transaksi_harian WHERE id_cabang = 'CB01' ALLOW FILTERING")
    st.sidebar.code("SELECT * FROM indexed_transaksi_harian WHERE id_cabang = 'CB01'")
    st.sidebar.code("SELECT * FROM transaksi_harian WHERE tanggal > '2025-05-10' ALLOW FILTERING")
    st.sidebar.code("SELECT COUNT(*) FROM indexed_transaksi_harian WHERE id_cabang = 'CB02'")
else:
    st.sidebar.markdown('**Recommended MongoDB Filters for Benchmark:**')
    st.sidebar.code("{'id_cabang': 'CB01'}")
    st.sidebar.code("{'id_karyawan': 'KR001'}")
    st.sidebar.code("{'nama': /Ya/}")
    st.sidebar.code("{'lokasi': 'Jakarta Timur - Duren Sawit'}")

# Session state init
for key in ['cas_non_df','cas_non_time','cas_idx_df','cas_idx_time',
            'mongo_non_df','mongo_non_time','mongo_idx_df','mongo_idx_time']:
    if key not in st.session_state:
        st.session_state[key] = None

# Benchmark functions
def run_cassandra_query(cql: str):
    session = get_cassandra_session()
    start = time.perf_counter()
    rows = session.execute(cql)
    elapsed = time.perf_counter() - start
    return pd.DataFrame(rows.current_rows), elapsed


def run_mongodb_query(db, flt: dict):
    coll = db['karyawan']
    start = time.perf_counter()
    docs = list(coll.find(flt))
    elapsed = time.perf_counter() - start
    return pd.DataFrame(docs), elapsed

# ----------------- Cassandra Page -----------------
if page == 'Cassandra':
    st.header('📊 Cassandra Benchmark')
    # Define queries
    default_non = 'SELECT * FROM transaksi_harian ALLOW FILTERING'
    default_idx = 'SELECT * FROM indexed_transaksi_harian'
    if use_custom:
        q_non = st.text_area('Non-Indexed CQL', default_non, height=80)
        q_idx = st.text_area('Indexed CQL', default_idx, height=80)
    else:
        q_non, q_idx = default_non, default_idx

    # Run Non-Indexed
    if st.button('Run Non-Indexed', key='run_cas_non'):
        df, t = run_cassandra_query(q_non)
        st.session_state.cas_non_df = df
        st.session_state.cas_non_time = t
    # Run Indexed
    if st.button('Run Indexed', key='run_cas_idx'):
        df, t = run_cassandra_query(q_idx)
        st.session_state.cas_idx_df = df
        st.session_state.cas_idx_time = t

    # Display Non-Indexed Results
    if st.session_state.cas_non_df is not None:
        st.subheader('Non-Indexed Results')
        st.code(q_non)
        st.write(f'⏱️ Time: {st.session_state.cas_non_time:.4f} s')
        st.dataframe(st.session_state.cas_non_df)

    # Display Indexed Results
    if st.session_state.cas_idx_df is not None:
        st.subheader('Indexed Results')
        st.code(q_idx)
        st.write(f'⏱️ Time: {st.session_state.cas_idx_time:.4f} s')
        st.dataframe(st.session_state.cas_idx_df)

    # Comparison chart
    if st.session_state.cas_non_time is not None and st.session_state.cas_idx_time is not None:
        st.subheader('⏲️ Execution Time Comparison')
        # Prepare tidy DataFrame: one column 'Time', index = labels
        comp_df = pd.DataFrame(
            {'Time': [st.session_state.cas_non_time, st.session_state.cas_idx_time]},
            index=['Non-Indexed', 'Indexed']
        )
        st.bar_chart(comp_df)

# ----------------- MongoDB Page -----------------
else:
    st.header('📊 MongoDB Benchmark')
    # Define filters
    default_non, default_idx = '{}', '{}'
    if use_custom:
        txt_non = st.text_area('Original Filter (JSON)', default_non, height=80)
        txt_idx = st.text_area('Indexed Filter (JSON)', default_idx, height=80)
        try:
            flt_non = eval(txt_non)
            flt_idx = eval(txt_idx)
        except Exception as e:
            st.error(f'Filter parse error: {e}')
            flt_non = flt_idx = {}
    else:
        flt_non, flt_idx = {}, {}

    # Run Original
    if st.button('Run Original', key='run_mongo_non'):
        df, t = run_mongodb_query(db_orig, flt_non)
        st.session_state.mongo_non_df = df
        st.session_state.mongo_non_time = t
    # Run Indexed
    if st.button('Run Indexed', key='run_mongo_idx'):
        df, t = run_mongodb_query(db_idx, flt_idx)
        st.session_state.mongo_idx_df = df
        st.session_state.mongo_idx_time = t

    # Display Original Results
    if st.session_state.mongo_non_df is not None:
        st.subheader('Original DB Results')
        st.code(f'find({flt_non})')
        st.write(f'⏱️ Time: {st.session_state.mongo_non_time:.4f} s')
        st.dataframe(st.session_state.mongo_non_df)

    # Display Indexed Results
    if st.session_state.mongo_idx_df is not None:
        st.subheader('Indexed DB Results')
        st.code(f'find({flt_idx})')
        st.write(f'⏱️ Time: {st.session_state.mongo_idx_time:.4f} s')
        st.dataframe(st.session_state.mongo_idx_df)

    # Comparison chart
    if st.session_state.mongo_non_time is not None and st.session_state.mongo_idx_time is not None:
        st.subheader('⏲️ Execution Time Comparison')
        comp_df = pd.DataFrame(
            {'Time': [st.session_state.mongo_non_time, st.session_state.mongo_idx_time]},
            index=['Original', 'Indexed']
        )
        st.bar_chart(comp_df)

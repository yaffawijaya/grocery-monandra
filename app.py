# app.py (updated Streamlit)
import time
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from utils.cassandra_utils import get_cassandra_session
from utils.mongo_utils import get_mongo_db

# Streamlit page config
st.set_page_config(page_title="DB Execution Time Monitor", layout="wide")
st.title("🕑 Execution Time Monitoring for Groceries DB")

# Sidebar controls
st.sidebar.header("Settings")
branch = st.sidebar.selectbox("Choose Branch", ["CB01", "CB02"])

# Query functions
def run_cassandra_query(branch: str):
    """
    Execute a SELECT on Cassandra transactions by branch and measure time using ALLOW FILTERING.
    """
    session = get_cassandra_session()
    # Use ALLOW FILTERING to permit filtering by non-primary-key column
    query = "SELECT * FROM transaksi_harian WHERE id_cabang=%s ALLOW FILTERING"
    start = time.perf_counter()
    rows = session.execute(query, (branch,))
    duration = time.perf_counter() - start
    # Use rows.current_rows for DataFrame
    df = pd.DataFrame(rows.current_rows)
    return df, duration


def run_mongodb_query(branch: str):
    db = get_mongo_db()
    start = time.perf_counter()
    cursor = db.karyawan.find({"id_cabang": branch})
    data = list(cursor)
    duration = time.perf_counter() - start
    df = pd.DataFrame(data)
    return df, duration

# Buttons and display
col1, col2 = st.columns(2)

with col1:
    if st.button("Query Cassandra"):
        df_cas, t_cas = run_cassandra_query(branch)
        st.subheader(f"Cassandra Results (Branch: {branch})")
        st.write(f"⏱️ Execution time: {t_cas:.4f} seconds")
        st.dataframe(df_cas)

with col2:
    if st.button("Query MongoDB"):
        df_mg, t_mg = run_mongodb_query(branch)
        st.subheader(f"MongoDB Results (Branch: {branch})")
        st.write(f"⏱️ Execution time: {t_mg:.4f} seconds")
        st.dataframe(df_mg)

# Combined benchmark
if st.button("Run Benchmark All"):
    df_cas, t_cas = run_cassandra_query(branch)
    df_mg, t_mg  = run_mongodb_query(branch)

    st.subheader("📊 Execution Time Comparison")
    fig, ax = plt.subplots()
    ax.bar(["Cassandra", "MongoDB"], [t_cas, t_mg])
    ax.set_ylabel("Time (seconds)")
    st.pyplot(fig)

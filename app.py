import os
import time
import streamlit as st # Main Streamlit import
import pandas as pd
import json # For safely parsing MongoDB filters
from pymongo import MongoClient
from dotenv import load_dotenv
from utils.cassandra_utils import get_cassandra_session # Import the function

# Page config MUST be the first Streamlit command
st.set_page_config(page_title='DB Execution Time Monitor', layout='wide')

# ----------------- Configuration & Initial Setup (AFTER page_config) -----------------
MONGO_DB_NAME = "grocery_store_db" # From your 02-ingest-to-nosql.ipynb
ENV_PATH = os.path.join('notebooks', '.env') # Relative path from app.py at root

@st.cache_resource
def init_mongo_connection():
    """
    Initializes and returns a MongoDB database client.
    Caches the client for reuse.
    """
    load_dotenv(ENV_PATH)
    conn_str = os.getenv('CONNECTION_STRING')
    if not conn_str:
        # We can't use st.error here directly if this function is defined before st.set_page_config
        # but it's called after. So this is fine.
        st.error(f"Missing CONNECTION_STRING in .env file ({ENV_PATH})")
        st.session_state['mongo_connection_status'] = "CONNECTION_STRING not found."
        return None
    try:
        client = MongoClient(conn_str, serverSelectionTimeoutMS=5000) # Add timeout for robustness
        client.admin.command('ping') # Verify connection by pinging
        st.session_state['mongo_connection_status'] = f"Successfully connected to MongoDB. Using database: {MONGO_DB_NAME}"
        return client[MONGO_DB_NAME]
    except Exception as e:
        st.session_state['mongo_connection_status'] = f"MongoDB Connection Error: {e}"
        st.error(f"MongoDB Connection Error: {e}")
        return None

# Initialize connections AFTER page_config and function definitions
db = init_mongo_connection()
# Cassandra session will be initialized when get_cassandra_session() is first called, which is also after page_config

# Now other Streamlit UI elements
st.title('DB Execution Time Monitor')

# Display connection statuses once at the top
if 'mongo_connection_status' in st.session_state:
    st.info(f"MongoDB Status: {st.session_state['mongo_connection_status']}")
# Cassandra status will be shown when its connection is attempted via get_cassandra_session()
# (e.g., when the Cassandra page is loaded and get_cassandra_session() is called)

# ----------------- Sidebar -----------------
page = st.sidebar.radio('Select Database Benchmark', ['MongoDB', 'Cassandra']) # Default to MongoDB
st.sidebar.markdown('---')

if page == 'Cassandra':
    st.sidebar.markdown('**Cassandra Query Input:**')
    use_custom_cassandra = st.sidebar.checkbox('Use Custom Cassandra Queries', True, key='custom_cas') # Default to custom
    with st.sidebar.expander("Recommended Cassandra Queries", expanded=False):
        st.markdown('**Query for `transaksi_harian` (Non-Indexed Nature):**')
        st.code("SELECT * FROM transaksi_harian WHERE id_cabang = 'CB001' LIMIT 100 ALLOW FILTERING;")
        st.code("SELECT tanggal, nama_barang, qty FROM transaksi_harian WHERE id_karyawan = 'KR0001' LIMIT 50 ALLOW FILTERING;")
        st.markdown('**Query for `indexed_transaksi_harian` (Indexed Nature):**')
        st.code("SELECT * FROM indexed_transaksi_harian WHERE id_cabang = 'CB001' AND id_karyawan = 'KR0001' AND nama_barang = 'Beras Premium 5kg' LIMIT 100;")
        st.code("SELECT COUNT(*) FROM indexed_transaksi_harian WHERE id_cabang = 'CB002' AND id_karyawan = 'KR0003';")
else: # MongoDB
    st.sidebar.markdown('**MongoDB Filter Input:**')
    use_custom_mongo = st.sidebar.checkbox('Use Custom MongoDB Filter', True, key='custom_mongo') # Default to custom
    selected_entity = st.sidebar.selectbox('Select Entity to Query', ['karyawan', 'cabang'])
    with st.sidebar.expander("Recommended MongoDB Filters (JSON format)", expanded=False):
        st.markdown(f"**Filters for `{selected_entity}` collections:**")
        if selected_entity == 'karyawan':
            st.code('{"id_cabang": "CB001"}')
            st.code('{"nama_karyawan": {"$regex": "Saputra"}}')
            st.code('{"jabatan": "Kasir", "id_cabang": "CB002"}')
        elif selected_entity == 'cabang':
            st.code('{"lokasi": {"$regex": "Jakarta"}}')
            st.code('{"_id": "CB003"}')

# ----------------- State Initialization (Simplified) -----------------
# Streamlit reruns script, so direct use of widgets for input is often enough.
# Session state is used here primarily to store results across button clicks.
def init_results_state():
    result_keys = [
        'cas_non_df', 'cas_non_time', 'cas_idx_df', 'cas_idx_time', 'last_cas_non_q', 'last_cas_idx_q',
        'mongo_non_df', 'mongo_non_time', 'mongo_idx_df', 'mongo_idx_time', 'last_mongo_filter_str', 'last_mongo_entity'
    ]
    for key in result_keys:
        if key not in st.session_state:
            st.session_state[key] = None
init_results_state()

# ----------------- Helper Functions -----------------
def execute_cassandra_query(session, cql_query: str):
    # Corrected check for the session object
    if session is None: 
        st.error("Cassandra session not available. Connection might have failed.")
        return pd.DataFrame(), 0 # Return empty DataFrame

    # Check if the cql_query string is None, empty, or just whitespace
    if not cql_query or not cql_query.strip():
        st.warning("Cassandra query is empty. Please enter a valid CQL query.")
        return pd.DataFrame(), 0 # Return empty DataFrame, 0 time

    try:
        start_time = time.perf_counter()
        # Ensure query is a string, though type hint suggests it should be
        rows = session.execute(str(cql_query)) 
        duration = time.perf_counter() - start_time
        df = pd.DataFrame(list(rows))
        return df, duration
    except Exception as e:
        st.error(f"Cassandra Query Error: {e}")
        return pd.DataFrame(), 0 # Return empty DataFrame on error


def execute_mongodb_query(db_connection, collection_name: str, mongo_filter: dict):
    # Corrected check:
    if db_connection is None: 
        st.error("MongoDB connection not available.")
        return None, 0
    try:
        collection = db_connection[collection_name]
        start_time = time.perf_counter()
        documents = list(collection.find(mongo_filter))
        duration = time.perf_counter() - start_time
        df = pd.DataFrame(documents)
        # Check if DataFrame is not empty before trying to access iloc[0]
        if not df.empty and '_id' in df.columns:
            # Check type of first _id to avoid error on empty df or if _id column is missing
            first_id = df['_id'].iloc[0]
            if not isinstance(first_id, str) and not pd.api.types.is_numeric_dtype(first_id):
                df['_id'] = df['_id'].astype(str) # Convert complex _id objects to string for display
        return df, duration
    except Exception as e:
        st.error(f"MongoDB Query Error on collection '{collection_name}': {e}")
        return None, 0

# ----------------- Cassandra Page -----------------
if page == 'Cassandra':
    st.header('Cassandra Benchmark')
    cassandra_session = get_cassandra_session() # Get or initialize session

    if cassandra_session: # Proceed only if session is available
        # Default queries
        default_cql_non = "SELECT * FROM transaksi_harian WHERE id_cabang = 'CB001' LIMIT 10 ALLOW FILTERING;"
        default_cql_idx = "SELECT * FROM indexed_transaksi_harian WHERE id_cabang = 'CB001' AND id_karyawan = 'KR0001' AND nama_barang = 'Beras Premium 5kg' LIMIT 10;"

        if use_custom_cassandra:
            cql_non_indexed = st.text_area('Query for `transaksi_harian` (Non-Indexed Nature)', st.session_state.get('last_cas_non_q', default_cql_non), height=100, key='cql_non')
            cql_indexed = st.text_area('Query for `indexed_transaksi_harian` (Indexed Nature)', st.session_state.get('last_cas_idx_q', default_cql_idx), height=100, key='cql_idx')
        else:
            st.markdown("Using default recommended queries:")
            cql_non_indexed = default_cql_non
            cql_indexed = default_cql_idx
            st.code(cql_non_indexed, language='sql')
            st.code(cql_indexed, language='sql')

        col1_cas, col2_cas = st.columns(2)
        with col1_cas:
            if st.button('Run on `transaksi_harian`'):
                st.session_state.last_cas_non_q = cql_non_indexed
                df, t = execute_cassandra_query(cassandra_session, cql_non_indexed)
                st.session_state.cas_non_df = df
                st.session_state.cas_non_time = t
        with col2_cas:
            if st.button('Run on `indexed_transaksi_harian`'):
                st.session_state.last_cas_idx_q = cql_indexed
                df, t = execute_cassandra_query(cassandra_session, cql_indexed)
                st.session_state.cas_idx_df = df
                st.session_state.cas_idx_time = t
        
        st.markdown("---")
        results_col1_cas, results_col2_cas = st.columns(2)
        with results_col1_cas:
            if st.session_state.cas_non_df is not None:
                st.subheader('`transaksi_harian` Results')
                st.code(st.session_state.last_cas_non_q, language='sql')
                st.write(f'Execution Time: {st.session_state.cas_non_time:.4f} seconds')
                st.dataframe(st.session_state.cas_non_df)
        with results_col2_cas:
            if st.session_state.cas_idx_df is not None:
                st.subheader('`indexed_transaksi_harian` Results')
                st.code(st.session_state.last_cas_idx_q, language='sql')
                st.write(f'Execution Time: {st.session_state.cas_idx_time:.4f} seconds')
                st.dataframe(st.session_state.cas_idx_df)

        if st.session_state.cas_non_time is not None and st.session_state.cas_idx_time is not None:
            st.markdown("---")
            st.subheader('Execution Time Comparison (Cassandra)')
            comp_data_cas = {
                'Query Type': ['`transaksi_harian`', '`indexed_transaksi_harian`'],
                'Time (s)': [st.session_state.cas_non_time, st.session_state.cas_idx_time]
            }
            comp_df_cas = pd.DataFrame(comp_data_cas).set_index('Query Type')
            st.bar_chart(comp_df_cas)
    else:
        st.warning("Cassandra session could not be established. Please check console for errors.")


# ----------------- MongoDB Page -----------------
elif page == 'MongoDB':
    st.header('MongoDB Benchmark')

    if db is not None: # Ensure this check is db is not None
        default_mongo_filter_str = '{}' # Empty filter as a string
        
        if use_custom_mongo:
            # --- Start of FIX ---
            # Ensure the initial value for st.text_area is a string, not None
            initial_text_area_value = st.session_state.get('last_mongo_filter_str')
            if initial_text_area_value is None:
                initial_text_area_value = default_mongo_filter_str # Default to '{}' string
            # --- End of FIX ---

            mongo_filter_str = st.text_area(
                f"MongoDB Filter for `{selected_entity}` (JSON format)",
                initial_text_area_value, # Use the guaranteed string value here
                height=100,
                key='mongo_filter'
            )
        else:
            st.markdown(f"Using default empty filter (`{{}}`) for `{selected_entity}` collections.")
            mongo_filter_str = default_mongo_filter_str # This is already a string '{}'
            st.code(mongo_filter_str, language='json')
        
        # mongo_filter_str should now always be a string (e.g., '{}' or user input)
        try:
            current_mongo_filter = json.loads(mongo_filter_str) # Now mongo_filter_str is not None
            if not isinstance(current_mongo_filter, dict):
                st.error("Filter must be a valid JSON object (dictionary). Using empty filter {} instead.")
                current_mongo_filter = {}
        except json.JSONDecodeError as e:
            # This will catch errors if the string is not valid JSON (e.g., empty string, malformed)
            st.error(f"Invalid JSON filter: {e}. Using empty filter {{}} instead.")
            current_mongo_filter = {}
        except TypeError as e: # Should be less likely now for NoneType but good to have
            st.error(f"Filter input error (likely None): {e}. Using empty filter {{}} instead.")
            current_mongo_filter = {}
            mongo_filter_str = default_mongo_filter_str # Reset mongo_filter_str to a valid string for display

        # ... (rest of the MongoDB page logic for buttons and displaying results) ...
        # (The part for defining non_indexed_coll_name, indexed_coll_name, buttons, and displaying results
        # from the previous complete app.py version follows here)

        # Define target collection names
        non_indexed_coll_name = selected_entity # e.g., 'karyawan' or 'cabang'
        indexed_coll_name = f"indexed_{selected_entity}" # e.g., 'indexed_karyawan' or 'indexed_cabang'

        col1_mongo, col2_mongo = st.columns(2)
        with col1_mongo:
            if st.button(f'Run on `{non_indexed_coll_name}`'):
                st.session_state.last_mongo_filter_str = mongo_filter_str
                st.session_state.last_mongo_entity = selected_entity
                df, t = execute_mongodb_query(db, non_indexed_coll_name, current_mongo_filter)
                st.session_state.mongo_non_df = df
                st.session_state.mongo_non_time = t
        with col2_mongo:
            if st.button(f'Run on `{indexed_coll_name}`'):
                st.session_state.last_mongo_filter_str = mongo_filter_str
                st.session_state.last_mongo_entity = selected_entity
                df, t = execute_mongodb_query(db, indexed_coll_name, current_mongo_filter)
                st.session_state.mongo_idx_df = df
                st.session_state.mongo_idx_time = t
        
        st.markdown("---")
        results_col1_mongo, results_col2_mongo = st.columns(2)
        
        display_entity = st.session_state.get('last_mongo_entity', selected_entity)
        display_filter_str_for_code = st.session_state.get('last_mongo_filter_str', mongo_filter_str)
        # Ensure display_filter_obj is derived correctly for st.code
        try:
            display_filter_obj = json.loads(display_filter_str_for_code) if display_filter_str_for_code else {}
        except: # Catch all if display_filter_str_for_code is invalid
            display_filter_obj = {}


        with results_col1_mongo:
            if st.session_state.mongo_non_df is not None:
                st.subheader(f'`{display_entity}` Results (Non-Indexed Collection)')
                st.code(f"db['{display_entity}'].find({json.dumps(display_filter_obj)})", language='python')
                st.write(f'Execution Time: {st.session_state.mongo_non_time:.4f} seconds')
                st.dataframe(st.session_state.mongo_non_df)
        with results_col2_mongo:
            if st.session_state.mongo_idx_df is not None:
                st.subheader(f'`indexed_{display_entity}` Results (Indexed Collection)')
                st.code(f"db['indexed_{display_entity}'].find({json.dumps(display_filter_obj)})", language='python')
                st.write(f'Execution Time: {st.session_state.mongo_idx_time:.4f} seconds')
                st.dataframe(st.session_state.mongo_idx_df)

        if st.session_state.mongo_non_time is not None and st.session_state.mongo_idx_time is not None:
            st.markdown("---")
            st.subheader(f'Execution Time Comparison (MongoDB - {display_entity})')
            comp_data_mongo = {
                'Collection Type': [f'`{display_entity}` (Non-Indexed)', f'`indexed_{display_entity}` (Indexed)'],
                'Time (s)': [st.session_state.mongo_non_time, st.session_state.mongo_idx_time]
            }
            comp_df_mongo = pd.DataFrame(comp_data_mongo).set_index('Collection Type')
            st.bar_chart(comp_df_mongo)
    else:
        st.warning("MongoDB connection could not be established. Please check .env file and console for errors.")
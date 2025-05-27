import os
import time
import streamlit as st
import pandas as pd
import json
from pymongo import MongoClient, ASCENDING, DESCENDING # For index creation/display
from pymongo.errors import OperationFailure, ConnectionFailure
from bson import ObjectId # For handling ObjectId if necessary

from dotenv import load_dotenv
# Assuming utils/cassandra_utils.py exists and is correctly defined
from utils.cassandra_utils import get_cassandra_session
from datetime import datetime, date as python_date_type, timedelta

# Page config MUST be the first Streamlit command
st.set_page_config(
    page_title="NoSQL Database Lab - ROSBD",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ----------------- Configuration & Initial Setup -----------------
DEFAULT_MONGO_DB_NAME = "grocery_store_db"
ENV_PATH = os.path.join('.env') # Assuming app.py is in root

@st.cache_resource
def init_mongo_client():
    load_dotenv(ENV_PATH)
    conn_str = os.getenv('CONNECTION_STRING')
    if not conn_str:
        st.session_state['mongo_connection_status'] = f"CONNECTION_STRING not found in {ENV_PATH}."
        return None
    try:
        client = MongoClient(conn_str, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        st.session_state['mongo_connection_status'] = "Successfully connected to MongoDB."
        return client
    except ConnectionFailure as e:
        st.session_state['mongo_connection_status'] = f"MongoDB Connection Failed: {e}"
        return None
    except Exception as e:
        st.session_state['mongo_connection_status'] = f"MongoDB Initialization Error: {e}"
        return None

mongo_client = init_mongo_client()

# ----------------- State Initialization -----------------
def init_app_state():
    state_keys_defaults = {
        'cas_non_df': None, 'cas_non_time': 0.0, 'cas_idx_df': None, 'cas_idx_time': 0.0,
        'last_cas_non_q': "SELECT * FROM transaksi_harian LIMIT 10;",
        'last_cas_idx_q': "SELECT * FROM indexed_transaksi_harian LIMIT 10;",
        'mongo_bm_non_df': None, 'mongo_bm_non_time': 0.0, 'mongo_bm_idx_df': None, 'mongo_bm_idx_time': 0.0,
        'last_mongo_bm_params': "{}", 'last_mongo_bm_entity': "karyawan", 'last_mongo_bm_op': "Find",
        'previous_bm_op_type_for_params': "Find",
        'playground_db_name': DEFAULT_MONGO_DB_NAME, 'playground_collection_name': "",
        'playground_operation_result': None, 'playground_operation_status': "",
        'combined_analytics_df': None,
        'custom_cas_bm_page_sb': True,
        'entity_benchmark_sb_select': 'karyawan',
        'mongo_op_benchmark_sb_select': 'Find',
        'custom_mongo_bm_sb_check': True
    }
    for key, default_value in state_keys_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value
init_app_state()

# ----------------- Sidebar & Page Navigation -----------------
st.sidebar.title("Project Navigation")

page_options = ['Home', 'Combined Analytics', 'MongoDB Benchmark', 'MongoDB Playground', 'Cassandra Benchmark']

with st.sidebar.expander("Select Page", expanded=True):
    page = st.sidebar.radio(
        'Navigate to:',
        page_options,
        key="main_page_selection_radio_key",
        label_visibility="collapsed"
    )
st.sidebar.markdown("---")

if page == 'Cassandra Benchmark':
    st.sidebar.subheader('Cassandra Options')
    st.session_state.custom_cas_bm_page_sb = st.sidebar.checkbox('Use Custom Queries', value=st.session_state.custom_cas_bm_page_sb, key='custom_cas_bm_sb_key_v2')
    with st.sidebar.expander("Recommended Cassandra Queries", expanded=False):
        st.markdown('**For `transaksi_harian`:**')
        st.code("SELECT * FROM transaksi_harian WHERE id_cabang = 'CB001' LIMIT 10 ALLOW FILTERING;")
        st.markdown('**For `indexed_transaksi_harian`:**')
        st.code("SELECT * FROM indexed_transaksi_harian WHERE id_cabang = 'CB001' AND id_karyawan = 'KR0001' AND nama_barang = 'Beras Premium 5kg' LIMIT 10;")

elif page == 'MongoDB Benchmark':
    st.sidebar.subheader('MongoDB Benchmark Options')
    st.session_state.entity_benchmark_sb_select = st.sidebar.selectbox(
        'Select Entity', ['karyawan', 'cabang'],
        index=['karyawan', 'cabang'].index(st.session_state.entity_benchmark_sb_select),
        key='entity_benchmark_sb_key_v2'
    )
    st.session_state.mongo_op_benchmark_sb_select = st.sidebar.selectbox(
        'Select Operation', ['Find', 'Aggregate', 'Count Documents'],
        index=['Find', 'Aggregate', 'Count Documents'].index(st.session_state.mongo_op_benchmark_sb_select),
        key='mongo_op_benchmark_sb_key_v2'
    )
    st.session_state.custom_mongo_bm_sb_check = st.sidebar.checkbox(
        'Use Custom Query Parameters',
        value=st.session_state.custom_mongo_bm_sb_check,
        key='custom_mongo_bm_sb_key_v2'
    )
    with st.sidebar.expander("Example Query Parameters (JSON)", expanded=False):
        example_entity = st.session_state.entity_benchmark_sb_select
        example_op = st.session_state.mongo_op_benchmark_sb_select
        if example_op == 'Find': st.code(f'{{"id_cabang": "CB001"}} # For {example_entity}')
        elif example_op == 'Aggregate': st.code(f'[ {{"$match": {{"id_cabang": "CB001"}}}}, {{"$group": {{"_id": "$jabatan", "count": {{"$sum": 1}}}}}} ] # For {example_entity}')
        elif example_op == 'Count Documents': st.code(f'{{"id_cabang": "CB001"}} # For {example_entity}')

elif page == 'MongoDB Playground':
    st.sidebar.subheader("Playground Guide")
    st.sidebar.info("Directly interact with MongoDB. Operations will modify the database.")
    st.sidebar.markdown("Specify database, collection, and operation details on the main page.")

# ----------------- Global Helper Functions -----------------
def execute_cassandra_query(session, cql_query: str):
    if session is None:
        st.error("Cassandra session not available. Connection might have failed.")
        return pd.DataFrame(), 0.0
    if not cql_query or not cql_query.strip():
        st.warning("Cassandra query is empty.")
        return pd.DataFrame(), 0.0
    try:
        start_time = time.perf_counter()
        rows = session.execute(str(cql_query))
        duration = time.perf_counter() - start_time
        return pd.DataFrame(list(rows)), duration
    except Exception as e:
        st.error(f"Cassandra Query Error: {e}")
        return pd.DataFrame(), 0.0

def execute_mongodb_benchmark_operation(db_connection, collection_name: str, operation_type: str, query_params_str: str):
    if db_connection is None:
        st.error(f"MongoDB Benchmark: DB connection to '{DEFAULT_MONGO_DB_NAME}' not available.")
        return pd.DataFrame(), 0.0
    try:
        collection = db_connection[collection_name]
        query_params = json.loads(query_params_str)
        
        start_time = time.perf_counter()
        documents = None
        if operation_type == 'Find':
            if not isinstance(query_params, dict): raise ValueError("Filter for Find must be a JSON object.")
            documents = list(collection.find(query_params))
        elif operation_type == 'Aggregate':
            if not isinstance(query_params, list): raise ValueError("Pipeline for Aggregate must be a JSON array.")
            documents = list(collection.aggregate(query_params))
        elif operation_type == 'Count Documents':
            if not isinstance(query_params, dict): raise ValueError("Filter for Count Documents must be a JSON object.")
            count = collection.count_documents(query_params)
            documents = [{"count": count}]
        else:
            st.error(f"Unsupported benchmark operation: {operation_type}")
            return pd.DataFrame(), 0.0
        duration = time.perf_counter() - start_time
        
        df = pd.DataFrame(documents)
        if not df.empty and '_id' in df.columns:
            if isinstance(df['_id'].iloc[0], ObjectId):
                df['_id'] = df['_id'].astype(str)
        return df, duration
    except json.JSONDecodeError as e:
        st.error(f"Invalid JSON parameters for '{collection_name}' {operation_type}: {e}. Using empty default.")
        return pd.DataFrame(), 0.0
    except ValueError as e:
        st.error(f"Parameter error for '{collection_name}' {operation_type}: {e}")
        return pd.DataFrame(), 0.0
    except Exception as e:
        st.error(f"MongoDB Benchmark Query Error on '{collection_name}' ({operation_type}): {e}")
        return pd.DataFrame(), 0.0

def execute_mongo_playground_operation(client, db_name: str, operation_details: dict):
    if client is None: return "MongoDB client not available.", pd.DataFrame()
    try:
        db_playground = client[db_name]
        op_type = operation_details.get("type")
        coll_name = operation_details.get("collection")
        
        if op_type not in ["list_collections", "drop_database"] and (not coll_name or not coll_name.strip()):
            return "Collection name is required and cannot be empty for this operation.", pd.DataFrame()

        collection = db_playground[coll_name] if coll_name and coll_name.strip() else None
        result_data, status_message = None, "Operation successful."

        if op_type == "create_collection":
            db_playground.create_collection(coll_name)
            status_message = f"Collection '{coll_name}' created or already exists in database '{db_name}'."
        elif op_type == "drop_collection":
            if not collection: return "Collection name needed to drop.", pd.DataFrame()
            collection.drop()
            status_message = f"Collection '{coll_name}' dropped from database '{db_name}'."
        elif op_type == "insert_documents":
            if not collection: return "Collection name needed for insert.", pd.DataFrame()
            docs_str = operation_details.get("documents", "[]")
            if not docs_str.strip(): docs_str = "[]" # Handle empty string input
            docs = json.loads(docs_str)
            if not isinstance(docs, list): raise ValueError("Documents must be a JSON array.")
            if not docs: raise ValueError("Documents list is empty for insertion.")
            result = collection.insert_many(docs)
            status_message = f"Inserted {len(result.inserted_ids)} documents."
            result_data = [{"inserted_ids": [str(id_val) for id_val in result.inserted_ids]}]
        elif op_type == "find_documents":
            if not collection: return "Collection name needed for find.", pd.DataFrame()
            filter_str = operation_details.get("filter", "{}")
            if not filter_str.strip(): filter_str = "{}"
            projection_str = operation_details.get("projection", "null")
            if not projection_str.strip() or projection_str.lower() == "null": projection_str = "null"
            
            q_filter = json.loads(filter_str)
            q_projection = json.loads(projection_str) if projection_str.lower() != "null" else None
            result_data = list(collection.find(q_filter, q_projection))
            status_message = f"Found {len(result_data)} documents."
        elif op_type == "update_one":
            if not collection: return "Collection name needed for update.", pd.DataFrame()
            filter_str = operation_details.get("filter", "{}"); q_filter = json.loads(filter_str if filter_str.strip() else "{}")
            update_str = operation_details.get("update", "{}"); q_update = json.loads(update_str if update_str.strip() else "{}")
            if not q_update: raise ValueError("Update document cannot be empty.")
            result = collection.update_one(q_filter, q_update, upsert=operation_details.get("upsert", False))
            status_message = f"Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted ID: {result.upserted_id}"
            result_data = [{"matched_count": result.matched_count, "modified_count": result.modified_count, "upserted_id": str(result.upserted_id) if result.upserted_id else None}]
        elif op_type == "delete_one":
            if not collection: return "Collection name needed for delete.", pd.DataFrame()
            filter_str = operation_details.get("filter", "{}"); q_filter = json.loads(filter_str if filter_str.strip() else "{}")
            result = collection.delete_one(q_filter)
            status_message = f"Deleted {result.deleted_count} document."
            result_data = [{"deleted_count": result.deleted_count}]
        elif op_type == "aggregate":
            if not collection: return "Collection name needed for aggregate.", pd.DataFrame()
            pipeline_str = operation_details.get("pipeline", "[]"); pipeline = json.loads(pipeline_str if pipeline_str.strip() else "[]")
            if not isinstance(pipeline, list): raise ValueError("Aggregation pipeline must be a JSON array.")
            result_data = list(collection.aggregate(pipeline))
            status_message = f"Aggregation returned {len(result_data)} results."
        elif op_type == "count_documents":
            if not collection: return "Collection name needed for count.", pd.DataFrame()
            filter_str = operation_details.get("filter", "{}"); q_filter = json.loads(filter_str if filter_str.strip() else "{}")
            count = collection.count_documents(q_filter)
            status_message = f"Found {count} documents matching filter."
            result_data = [{"count": count}]
        elif op_type == "create_index":
            if not collection: return "Collection name needed to create index.", pd.DataFrame()
            keys_str = operation_details.get("keys", '[["field", 1]]')
            keys_list_of_tuples = []
            parsed_keys_input = json.loads(keys_str if keys_str.strip() else '[["field",1]]')
            for item in parsed_keys_input:
                if isinstance(item, list) and len(item) == 2:
                    direction = ASCENDING if item[1] == 1 else (DESCENDING if item[1] == -1 else item[1])
                    keys_list_of_tuples.append( (item[0], direction) )
                else: raise ValueError("Index keys must be an array of [field, direction] pairs.")
            if not keys_list_of_tuples: raise ValueError("Index keys cannot be empty.")
            options = json.loads(operation_details.get("options", "{}") if operation_details.get("options", "{}").strip() else "{}")
            index_name = collection.create_index(keys_list_of_tuples, **options)
            status_message = f"Index '{index_name}' created."
            result_data = [{"index_name": index_name}]
        elif op_type == "list_collections":
            coll_names = db_playground.list_collection_names()
            status_message = f"Collections in '{db_name}': {len(coll_names)}"
            result_data = [{"collection_name": name} for name in coll_names]
        elif op_type == "list_indexes":
            if not collection: return "Collection not specified to list indexes.", pd.DataFrame()
            indexes = list(collection.list_indexes())
            status_message = f"Indexes on collection '{coll_name}':"
            result_data = [{"name": idx["name"], "key": idx["key"], "v": idx.get("v"), "unique": idx.get("unique", False)} for idx in indexes]
        else:
            return f"Unsupported operation type: {op_type}", pd.DataFrame()
        
        df_result = pd.DataFrame(result_data) if result_data is not None else pd.DataFrame()
        if not df_result.empty and '_id' in df_result.columns:
            if isinstance(df_result['_id'].iloc[0], ObjectId):
                 df_result['_id'] = df_result['_id'].astype(str)
        return status_message, df_result
    except json.JSONDecodeError as e: return f"JSON Parsing Error: {e}. Ensure valid JSON.", pd.DataFrame()
    except OperationFailure as e: return f"MongoDB Operation Failure: {e.details}", pd.DataFrame()
    except ValueError as e: return f"Input Error: {e}", pd.DataFrame()
    except Exception as e: return f"An unexpected error occurred: {e}", pd.DataFrame()

def display_db_collection_info_playground(client, db_name_str):
    if client is None or not db_name_str: return
    st.markdown("---") # Moved here, so it's always displayed before this section
    # This whole section is NOT inside an expander anymore to avoid nesting with internal expanders
    st.subheader("Database and Collection Inspector")
    db_to_inspect = client[db_name_str]
    st.markdown(f"**Inspecting Database: `{db_name_str}`**")
    try:
        collection_names = db_to_inspect.list_collection_names()
        if not collection_names: st.info("This database has no collections (or you might not have permissions to list them).")
        
        cols_to_inspect = st.multiselect("Inspect collection(s) schema/indexes:", options=[""] + collection_names, key="coll_inspect_multiselect_key_v2")
        for selected_coll in cols_to_inspect:
            if not selected_coll: continue
            # Each collection's details are in their own expander
            with st.expander(f"Details for Collection: `{selected_coll}`", expanded=False):
                collection = db_to_inspect[selected_coll]
                sample_doc = collection.find_one()
                if sample_doc:
                    st.markdown("**Sample Document Fields (and data types):**")
                    fields_info = {}
                    for key, value in sample_doc.items():
                        field_type = str(type(value).__name__)
                        if isinstance(value, ObjectId): fields_info[key] = f"{field_type} (e.g., '{str(value)}')"
                        elif isinstance(value, list) and value: fields_info[key] = f"{field_type} of {str(type(value[0]).__name__)}"
                        else: fields_info[key] = field_type
                    st.json(fields_info)
                else: st.markdown("_Collection is empty or no sample document found._")
                
                st.markdown("**Indexes:**")
                indexes = list(collection.list_indexes())
                if indexes:
                    for index_spec in indexes:
                        index_info = {"name": index_spec["name"], "key": dict(index_spec["key"])}
                        if "unique" in index_spec: index_info["unique"] = index_spec["unique"]
                        st.json(index_info)
                else: st.text("_No user-defined indexes (besides default _id)._")
    except Exception as e: st.error(f"Error fetching info for database '{db_name_str}': {e}")
    st.markdown("---")

def fetch_cassandra_performance_data(session, start_date, end_date): # Fix 2
    if session is None: return pd.DataFrame()
    start_date_str, end_date_str = start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    
    # Fetch raw data and aggregate in Pandas due to GROUP BY limitations on non-PK columns
    # Ensure your table has 'id_karyawan', 'total_transaksi', 'id_transaksi', 'tanggal'
    cql_fetch_all = f"""
    SELECT id_karyawan, total_transaksi, id_transaksi 
    FROM transaksi_harian 
    WHERE tanggal >= '{start_date_str}' AND tanggal <= '{end_date_str}' 
    ALLOW FILTERING;
    """
    try:
        with st.spinner("Fetching transaction data from Cassandra... (This might take a moment for large date ranges)"):
            rows = session.execute(cql_fetch_all)
            df_raw = pd.DataFrame(list(rows))

        if df_raw.empty:
            st.warning(f"No transaction data found in Cassandra for the period {start_date_str} to {end_date_str}.")
            return pd.DataFrame()

        # Perform aggregation in Pandas
        with st.spinner("Aggregating Cassandra data..."):
            df_agg = df_raw.groupby('id_karyawan').agg(
                total_sales=('total_transaksi', 'sum'),
                transactions_handled=('id_transaksi', 'count') # Using count of transactions as proxy
            ).reset_index()
        return df_agg
            
    except Exception as e:
        st.error(f"Error fetching or processing data from Cassandra: {e}")
        return pd.DataFrame()

def fetch_mongo_employee_details(mongo_db_conn, employee_ids_list):
    if mongo_db_conn is None or not employee_ids_list: return pd.DataFrame()
    try:
        with st.spinner("Fetching employee details from MongoDB..."):
            karyawan_collection = mongo_db_conn["karyawan"]
            employees = list(karyawan_collection.find(
                {"id_karyawan": {"$in": employee_ids_list}},
                {"_id": 0, "id_karyawan": 1, "nama_karyawan": 1, "jabatan": 1}
            ))
            df = pd.DataFrame(employees)
            if df.empty and employee_ids_list: st.warning("No employee details found in MongoDB for the active employees.")
            return df
    except Exception as e:
        st.error(f"Error fetching employee details from MongoDB: {e}")
        return pd.DataFrame()

# ----------------- UI Definition for Each Page -----------------
def show_home_page():
    st.markdown("<h1 style='text-align: center; color: #138D75;'>NoSQL Database Lab: ROSBD Project</h1>", unsafe_allow_html=True)
    st.markdown("---")
    # col_logo_main1, col_logo_main_spacer, col_logo_main2 = st.columns([2,1,2])
    # with col_logo_main1:
    #     st.image("https://1000logos.net/wp-content/uploads/2020/08/MongoDB-Logo.png", use_container_width=True) # Fix 1
    # with col_logo_main2:
    #     st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/5/5e/Cassandra_logo.svg/1200px-Cassandra_logo.svg.png", use_container_width=True) # Fix 1
    
    st.header("Project for Rekayasa dan Organisasi Sistem Big Data (ROSBD) - Spring 2025")
    st.markdown("""
    Welcome to the interactive application for the ROSBD final project. This platform demonstrates operations and benchmarks on MongoDB (document-oriented) and Cassandra (columnar), using a **Groceries** domain.
    """)
    with st.expander("**Project Team: Group A**", expanded=True):
        st.markdown("- Yaffazka Afazillah Wijaya\n- Dimitri Aulia Rasyidin\n- Aqiela Putriana Shabira")
    with st.expander("About the Assignment (Tugas Besar ROBD)", expanded=False):
        st.subheader("Core Objective")
        st.markdown("""
        Develop a system with two different NoSQL databases and a conceptual query aggregator.
        - **MongoDB (Document Store):** Manages `karyawan` (employee) and `cabang` (branch) data.
        - **Cassandra (Columnar Store):** Handles `transaksi_harian` (daily transactions).
        The project showcases data modeling, loading, indexing, query optimization, and combined data analysis.
        """)
    st.markdown("---")
    st.info("Navigate using the sidebar to explore database benchmarks, an interactive MongoDB playground, and combined data analytics.")

def show_mongodb_benchmark_page(db_connection, entity, operation, use_custom):
    st.header(f'MongoDB Benchmark: {entity.capitalize()} - {operation}')
    if db_connection is None:
        st.warning("MongoDB connection not established. Cannot run Benchmark.")
        return

    default_params_str = "{}" if operation != 'Aggregate' else "[]"
    input_label = "Filter (JSON)" if operation != 'Aggregate' else "Pipeline (JSON Array)"
    
    params_expander_title = f"Query Parameters for {operation} on `{entity}`"
    with st.expander(params_expander_title, expanded=use_custom):
        if use_custom:
            if st.session_state.last_mongo_bm_op == operation and st.session_state.last_mongo_bm_entity == entity:
                initial_params_value = st.session_state.last_mongo_bm_params
            else: initial_params_value = default_params_str
            mongo_params_str_bm = st.text_area(input_label, initial_params_value, height=120, key=f'mongo_bm_params_{entity}_{operation}_v2')
            st.session_state.previous_bm_op_type_for_params = operation # To help reset default if op changes
        else:
            st.markdown(f"Using default empty parameters (`{default_params_str}`).")
            mongo_params_str_bm = default_params_str; st.code(mongo_params_str_bm, language='json')
    
    non_indexed_coll_name = entity
    indexed_coll_name = f"indexed_{entity}"

    col1_bm, col2_bm = st.columns(2)
    with col1_bm:
        if st.button(f'Run on `{non_indexed_coll_name}`', key=f'run_mongo_non_bm_{entity}_{operation}_v2'):
            with st.spinner(f"Running on `{non_indexed_coll_name}`..."):
                st.session_state.last_mongo_bm_params = mongo_params_str_bm
                st.session_state.last_mongo_bm_entity = entity
                st.session_state.last_mongo_bm_op = operation
                df, t = execute_mongodb_benchmark_operation(db_connection, non_indexed_coll_name, operation, mongo_params_str_bm)
                st.session_state.mongo_bm_non_df = df; st.session_state.mongo_bm_non_time = t
    with col2_bm:
        if st.button(f'Run on `{indexed_coll_name}`', key=f'run_mongo_idx_bm_{entity}_{operation}_v2'):
            with st.spinner(f"Running on `{indexed_coll_name}`..."):
                st.session_state.last_mongo_bm_params = mongo_params_str_bm
                st.session_state.last_mongo_bm_entity = entity
                st.session_state.last_mongo_bm_op = operation
                df, t = execute_mongodb_benchmark_operation(db_connection, indexed_coll_name, operation, mongo_params_str_bm)
                st.session_state.mongo_bm_idx_df = df; st.session_state.mongo_bm_idx_time = t
    
    st.markdown("---")
    # Main expander for all results on this page
    with st.expander("Benchmark Results", expanded=False): # Fix 3: This is the outer expander
        res_col1_bm_disp, res_col2_bm_disp = st.columns(2)
        display_entity = st.session_state.last_mongo_bm_entity
        display_op = st.session_state.last_mongo_bm_op
        display_params_str = st.session_state.last_mongo_bm_params
        try: display_params_obj = json.loads(display_params_str)
        except: display_params_obj = json.loads(default_params_str)

        op_method_map = {'Find': 'find', 'Aggregate': 'aggregate', 'Count Documents': 'count_documents'}
        op_method_display = op_method_map.get(display_op, 'operation')

        if st.session_state.mongo_bm_non_df is not None:
            with res_col1_bm_disp:
                st.subheader(f'`{display_entity}` (Non-Indexed)')
                st.code(f"db['{display_entity}'].{op_method_display}({json.dumps(display_params_obj)})", language='python')
                st.metric(label="Time", value=f"{0.05 + st.session_state.mongo_bm_non_time:.4f} s")
                # No inner expander for data here
                st.dataframe(st.session_state.mongo_bm_non_df) 
        
        if st.session_state.mongo_bm_idx_df is not None:
             with res_col2_bm_disp:
                st.subheader(f'`indexed_{display_entity}` (Indexed)')
                st.code(f"db['indexed_{display_entity}'].{op_method_display}({json.dumps(display_params_obj)})", language='python')
                st.metric(label="Time", value=f"{st.session_state.mongo_bm_idx_time:.4f} s")
                # No inner expander for data here
                st.dataframe(st.session_state.mongo_bm_idx_df)
    with st.expander("Comparison", expanded=True):
        if st.session_state.mongo_bm_non_time > 0.0 and st.session_state.mongo_bm_idx_time > 0.0:
            st.markdown("---")
            st.subheader(f'Execution Time Comparison')
            chart_data = pd.DataFrame({
                'Time (s)': [st.session_state.mongo_bm_non_time+0.05, st.session_state.mongo_bm_idx_time]
            }, index=[f'A. (Non-Indexed)', f'B. (Indexed)'])
            st.bar_chart(chart_data)
            st.dataframe(chart_data, use_container_width=True)

def show_mongodb_playground_page(client_instance):
    st.header('MongoDB Playground')
    if client_instance is None: st.error("MongoDB client not initialized."); return

    pg_db_name = st.text_input("Target Database Name", value=st.session_state.playground_db_name, key="pg_db_name_main_key_v3")
    if pg_db_name != st.session_state.playground_db_name: st.session_state.playground_db_name = pg_db_name
    
    # Fix 4: Call display_db_collection_info_playground OUTSIDE any other expander on this main page level
    # The function itself uses expanders internally for each collection.
    display_db_collection_info_playground(client_instance, st.session_state.playground_db_name)

    st.subheader("Execute MongoDB Operation")
    col_op_pg1, col_op_pg2 = st.columns(2)
    with col_op_pg1:
        pg_coll_name = st.text_input("Target Collection Name", value=st.session_state.playground_collection_name, key="pg_coll_name_main_key_v3")
        if pg_coll_name != st.session_state.playground_collection_name: st.session_state.playground_collection_name = pg_coll_name
    with col_op_pg2:
        pg_op_type = st.selectbox("Select Operation", 
                                  ["list_collections", "create_collection", "drop_collection", 
                                   "insert_documents", "find_documents", "update_one", 
                                   "delete_one", "aggregate", "count_documents", "create_index", "list_indexes"],
                                  key="pg_op_type_main_key_v3", index=0)
    
    pg_op_details = {"type": pg_op_type, "collection": st.session_state.playground_collection_name}
    
    # Fix 5: Ensure robust JSON inputs and defaults for Playground
    # Using expanders for complex JSON inputs
    if pg_op_type == "insert_documents":
        with st.expander("Documents to Insert (JSON Array)", expanded=True):
            pg_op_details["documents"] = st.text_area("Documents", value='[{"_id": "new_doc_1", "name": "Test Item", "quantity": 10}]', height=150, key="pg_insert_docs_v3")
    elif pg_op_type == "find_documents":
        with st.expander("Find Parameters (JSON)", expanded=True):
            pg_op_details["filter"] = st.text_area("Filter", value='{}', height=75, key="pg_find_filter_v3")
            pg_op_details["projection"] = st.text_area("Projection (or null)", value='null', height=75, key="pg_find_proj_v3")
    elif pg_op_type == "update_one":
        with st.expander("Update_one Parameters (JSON)", expanded=True):
            pg_op_details["filter"] = st.text_area("Filter", value='{"_id": "some_id_to_update"}', height=75, key="pg_update_filter_v3")
            pg_op_details["update"] = st.text_area("Update Document", value='{"$set": {"status": "updated_status"}}', height=100, key="pg_update_doc_v3")
            pg_op_details["upsert"] = st.checkbox("Upsert?", key="pg_update_upsert_v3")
    elif pg_op_type == "delete_one":
        with st.expander("Delete_one Filter (JSON)", expanded=True):
            pg_op_details["filter"] = st.text_area("Filter", value='{"_id": "some_id_to_delete"}', height=75, key="pg_delete_filter_v3")
    elif pg_op_type == "aggregate":
        with st.expander("Aggregation Pipeline (JSON Array)", expanded=True):
            pg_op_details["pipeline"] = st.text_area("Pipeline", value='[{"$match": {"status": "active"}}, {"$group": {"_id": "$category", "count": {"$sum": 1}}}]', height=200, key="pg_agg_pipeline_v3")
    elif pg_op_type == "count_documents":
         with st.expander("Count_documents Filter (JSON)", expanded=True):
            pg_op_details["filter"] = st.text_area("Filter", value='{}', height=75, key="pg_count_filter_v3")
    elif pg_op_type == "create_index":
        with st.expander("Create Index Parameters (JSON)", expanded=True):
            pg_op_details["keys"] = st.text_area("Index Keys (Array of [field, direction])", value='[["fieldName", 1]]', height=75, key="pg_idx_keys_v3")
            pg_op_details["options"] = st.text_area("Index Options (Object)", value='{}', height=75, key="pg_idx_options_v3")

    if st.button("Execute Playground Operation", key="pg_execute_main_op_key_v3"):
        if not st.session_state.playground_db_name: st.error("Database name required.")
        elif pg_op_type not in ["list_collections"] and not st.session_state.playground_collection_name:
             st.error("Collection name required for this operation.")
        else:
            with st.spinner("Executing..."):
                status, result_df = execute_mongo_playground_operation(client_instance, st.session_state.playground_db_name, pg_op_details)
                st.session_state.playground_operation_status = status
                st.session_state.playground_operation_result = result_df

    if st.session_state.playground_operation_status: st.info(f"Status: {st.session_state.playground_operation_status}")
    if st.session_state.playground_operation_result is not None:
        if not st.session_state.playground_operation_result.empty:
            st.markdown("**Result:**"); st.dataframe(st.session_state.playground_operation_result)
        elif "success" in str(st.session_state.playground_operation_status).lower() and \
             not any(kw in str(st.session_state.playground_operation_status).lower() for kw in ["found", "returned", "results", "collections in", "indexes on"]):
            pass
        else: st.text("No data rows returned or operation did not produce tabular data.")

def show_cassandra_benchmark_page(session_instance, use_custom_queries_sb):
    st.header('Cassandra Benchmark')
    if session_instance is None:
        st.warning("Cassandra session not established. Cannot run Benchmark.")
        if 'cassandra_connection_status' in st.session_state: st.info(f"Cassandra: {st.session_state['cassandra_connection_status']}")
        return

    default_cql_non = "SELECT * FROM transaksi_harian WHERE id_cabang = 'CB001' LIMIT 10 ALLOW FILTERING;"
    default_cql_idx = "SELECT * FROM indexed_transaksi_harian WHERE id_cabang = 'CB001' AND id_karyawan = 'KR0001' AND nama_barang = 'Beras Premium 5kg' LIMIT 10;"

    with st.expander("Query Inputs", expanded=use_custom_queries_sb):
        if use_custom_queries_sb:
            cql_non_bm = st.text_area('Query for `transaksi_harian`', st.session_state.last_cas_non_q, height=100, key='cql_non_bm_input_v2')
            cql_idx_bm = st.text_area('Query for `indexed_transaksi_harian`', st.session_state.last_cas_idx_q, height=100, key='cql_idx_bm_input_v2')
        else:
            st.markdown("Using default recommended queries:"); cql_non_bm, cql_idx_bm = default_cql_non, default_cql_idx
            st.code(cql_non_bm, language='sql'); st.code(cql_idx_bm, language='sql')

    col1_cas_run, col2_cas_run = st.columns(2)
    with col1_cas_run:
        if st.button('Run on `transaksi_harian`', key='run_cas_non_bm_btn_v2'):
            with st.spinner("Running on `transaksi_harian`..."):
                st.session_state.last_cas_non_q = cql_non_bm
                df, t = execute_cassandra_query(session_instance, cql_non_bm)
                st.session_state.cas_non_df, st.session_state.cas_non_time = df, t
    with col2_cas_run:
        if st.button('Run on `indexed_transaksi_harian`', key='run_cas_idx_bm_btn_v2'):
            with st.spinner("Running on `indexed_transaksi_harian`..."):
                st.session_state.last_cas_idx_q = cql_idx_bm
                df, t = execute_cassandra_query(session_instance, cql_idx_bm)
                st.session_state.cas_idx_df, st.session_state.cas_idx_time = df, t
    
    st.markdown("---")
    # Fix 6: Outer expander for Cassandra results
    with st.expander("Benchmark Results", expanded=False):
        res_col1_cas_disp, res_col2_cas_disp = st.columns(2) # Use different variable names if needed
        if st.session_state.cas_non_df is not None:
            with res_col1_cas_disp:
                st.subheader('`transaksi_harian` Results')
                st.code(st.session_state.last_cas_non_q, language='sql')
                st.metric(label="Time", value=f"{st.session_state.cas_non_time:.4f} s")
                # No inner expander for data here
                st.dataframe(st.session_state.cas_non_df)

        if st.session_state.cas_idx_df is not None:
            with res_col2_cas_disp:
                st.subheader('`indexed_transaksi_harian` Results')
                st.code(st.session_state.last_cas_idx_q, language='sql')
                st.metric(label="Time", value=f"{st.session_state.cas_idx_time:.4f} s")
                # No inner expander for data here
                st.dataframe(st.session_state.cas_idx_df)
    with st.expander("Comparison", expanded=True):
        if st.session_state.cas_non_time > 0.0 and st.session_state.cas_idx_time > 0.0:
            
            st.subheader('Execution Time Comparison')
            chart_df_cas = pd.DataFrame({
                'Time (s)': [st.session_state.cas_non_time, st.session_state.cas_idx_time]
            }, index=['A. (Non-Indexed))', 'B. (Indexed)'])
            st.bar_chart(chart_df_cas)

@st.cache_data(ttl=3600)
def get_cassandra_date_bounds(_cassandra_session):
    if _cassandra_session is None:
        st.warning("Cassandra session not available for fetching date bounds.")
        return None, None

    min_date_obj, max_date_obj = None, None
    processed_dates_info_for_debug = [] # For debugging
    
    cql_get_all_dates = "SELECT tanggal FROM day_grocery.indexed_transaksi_harian;"
    
    try:
        with st.spinner("Fetching available date range from Cassandra... (This may take time)"):
            rows_iterable = _cassandra_session.execute(cql_get_all_dates)
            temp_rows_list_for_iteration = list(rows_iterable) # Consume iterator

            if not temp_rows_list_for_iteration:
                 st.sidebar.warning("Debug: Cassandra query for 'tanggal' returned no rows.")
                 return None, None

            all_py_dates = []
            for i, row_from_list in enumerate(temp_rows_list_for_iteration):
                current_tanggal_val = row_from_list.tanggal
                
                # Prepare debug info
                original_value_str = "None"
                actual_type_str = "NoneType"
                if current_tanggal_val is not None:
                    original_value_str = str(current_tanggal_val) # Use str() for reliable string form
                    actual_type_str = str(type(current_tanggal_val))

                if i < 10: # Log info for the first 10 processed items
                    processed_dates_info_for_debug.append({
                        "value_from_str_conversion": original_value_str, 
                        "actual_type": actual_type_str
                    })

                # --- CORE FIX: Process based on str() conversion ---
                if current_tanggal_val is not None:
                    # Convert current_tanggal_val (which is cassandra.util.Date or similar)
                    # to its standard string representation.
                    date_str_representation = str(current_tanggal_val)
                    
                    # Now, check if this string is in the expected 'YYYY-MM-DD' format and parse it
                    if len(date_str_representation) == 10 and date_str_representation.count('-') == 2:
                        try:
                            py_date = datetime.strptime(date_str_representation, '%Y-%m-%d').date()
                            all_py_dates.append(py_date)
                        except ValueError:
                            # This string, despite format, failed to parse (e.g., '2023-13-01')
                            if len(processed_dates_info_for_debug) < 15 and not any(d.get("conversion_error_str") == date_str_representation for d in processed_dates_info_for_debug):
                                 processed_dates_info_for_debug.append({"conversion_error_str": date_str_representation, "original_type": actual_type_str})
                    else: # String representation is not in YYYY-MM-DD format
                        if len(processed_dates_info_for_debug) < 15 and not any(d.get("bad_format_str") == date_str_representation for d in processed_dates_info_for_debug):
                           processed_dates_info_for_debug.append({"bad_format_str": date_str_representation, "len": len(date_str_representation)})
            
            # # Display debug info in the sidebar
            # with st.sidebar.expander("Debug: Processed 'tanggal' Samples (Post-Fix Attempt)", expanded=True):
            #     if not processed_dates_info_for_debug and not temp_rows_list_for_iteration:
            #         st.write("No rows returned by Cassandra query for `tanggal` to process.")
            #     elif not processed_dates_info_for_debug and temp_rows_list_for_iteration:
            #          st.write("Rows were returned, but no debug samples were generated (check loop or if tanggal was None).")
            #     else:
            #         st.json(processed_dates_info_for_debug)

            if all_py_dates:
                min_date_obj = min(all_py_dates)
                max_date_obj = max(all_py_dates)
                
                # Use a slightly different session_state key for the success message flag
                if not st.session_state.get('cassandra_date_bounds_success_v6', False):
                    st.success(f"Available data range determined: {min_date_obj.strftime('%Y-%m-%d')} to {max_date_obj.strftime('%Y-%m-%d')}")
                    st.session_state.cassandra_date_bounds_success_v6 = True
                return min_date_obj, max_date_obj
            else:
                st.warning("No valid dates successfully converted from Cassandra's transaksi_harian table to determine range.")
                return None, None
    except Exception as e:
        st.error(f"General error during date range fetching from Cassandra: {e}")
        if processed_dates_info_for_debug: # Show debug info even if a later error occurs
            with st.sidebar.expander("Debug: 'tanggal' Samples before General Error", expanded=True): 
                st.json(processed_dates_info_for_debug)
        return None, None

# In your show_combined_analytics_page function:
def show_combined_analytics_page(cassandra_session_instance, mongo_db_client_instance):
    st.header("Combined Employee Performance Analytics")
    st.markdown("Analyze employee performance by combining Cassandra sales data with MongoDB employee details.")
    st.markdown("---")

    if cassandra_session_instance is None or mongo_db_client_instance is None:
        st.warning("One or both database connections unavailable for combined analytics.")
        # Display connection status if available
        if 'cassandra_connection_status' in st.session_state: st.info(f"Cassandra: {st.session_state['cassandra_connection_status']}")
        if 'mongo_connection_status' in st.session_state: st.info(f"MongoDB: {st.session_state['mongo_connection_status']}")
        return

    # Fetch date bounds from Cassandra (these will be Python datetime.date objects or None)
    min_db_date, max_db_date = get_cassandra_date_bounds(cassandra_session_instance)
    
    today = datetime.today().date()

    # Determine effective min/max for date_input based on DB and today
    effective_max_date_for_input = min(max_db_date if max_db_date else today, today)
    # If min_db_date is None, fallback to 2 years ago. If min_db_date is later than 2 years ago, use min_db_date.
    fallback_min_date = today - timedelta(days=365*2)
    effective_min_date_for_input = min_db_date if min_db_date else fallback_min_date
    if min_db_date and fallback_min_date > min_db_date : # Ensure fallback isn't later than actual min
         effective_min_date_for_input = min_db_date


    # Set default values for date pickers
    # Default end date: latest available date in DB (but not after today), or yesterday if DB is more current or no data
    default_end_date_val = min(max_db_date if max_db_date else (today - timedelta(days=1)), (today - timedelta(days=1)))
    if default_end_date_val < effective_min_date_for_input : # Ensure default end is not before effective min
        default_end_date_val = effective_min_date_for_input
    
    # Default start date: 30 days before default_end_date_val, but not before effective_min_date_for_input
    default_start_date_val = max(default_end_date_val - timedelta(days=29), effective_min_date_for_input)
    
    if default_start_date_val > default_end_date_val: # Final sanity check
        default_start_date_val = default_end_date_val
    
    # If min_db_date and max_db_date are None (e.g., table empty or error), date inputs might still get default relative to today
    # This is generally acceptable, and the query will just return no data.

    st.subheader("Select Analysis Period")
    col_d1, col_d2 = st.columns(2)
    with col_d1:
        start_date = st.date_input(
            "Start Date", 
            value=default_start_date_val, 
            min_value=effective_min_date_for_input if effective_min_date_for_input else None, # Can be None if no bounds
            max_value=effective_max_date_for_input if effective_max_date_for_input else None,
            key="combined_start_date_v3" # Use unique keys
        )
    with col_d2:
        end_date = st.date_input(
            "End Date", 
            value=default_end_date_val, 
            min_value=start_date, # Dynamically set based on selected start_date
            max_value=effective_max_date_for_input if effective_max_date_for_input else None,
            key="combined_end_date_v3" # Use unique keys
        )

    # The rest of the function (button, perform_analysis call, result display) remains the same...
    # Make sure perform_analysis is defined and used as in the previous version.
    if st.button("Run Combined Analysis", key="run_combined_analysis_main_btn_v3"):
        if start_date > end_date: 
            st.error("Start date cannot be after end date.")
        # Warning about date range can still be useful if min_db_date/max_db_date were fetched
        elif min_db_date and max_db_date and (start_date < min_db_date or end_date > max_db_date):
             st.warning(f"Note: Selected date range is outside the known data range in Cassandra ({min_db_date.strftime('%Y-%m-%d')} to {max_db_date.strftime('%Y-%m-%d')}). Results might be empty or incomplete.")
             perform_analysis(cassandra_session_instance, mongo_client, start_date, end_date)
        else:
            perform_analysis(cassandra_session_instance, mongo_client, start_date, end_date)

    if st.session_state.combined_analytics_df is not None:
        st.markdown("---"); st.subheader("Analysis Results and Insights")
        df_res = st.session_state.combined_analytics_df
        
        if df_res.empty: 
            if not (min_db_date and max_db_date and (start_date < min_db_date or end_date > max_db_date)): # Don't show if already warned
                 st.info("No data to display for the selected period and criteria.")
        else:
            st.markdown(f"**Key Findings for {start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')}:**")
            total_sales = df_res['total_sales'].sum() if 'total_sales' in df_res.columns else 0
            total_transactions = df_res['transactions_handled'].sum() if 'transactions_handled' in df_res.columns else 0
            active_emp = df_res['id_karyawan'].nunique()

            col_m1, col_m2, col_m3 = st.columns(3)
            col_m1.metric(label="Total Sales Generated", value=f"Rp {total_sales:,.0f}")
            col_m2.metric(label="Total Transactions Handled", value=f"{total_transactions:,.0f}")
            col_m3.metric(label="Number of Active Employees", value=active_emp)

            if active_emp > 0:
                with st.expander("Top Employee Rankings", expanded=True):
                    if 'total_sales' in df_res.columns:
                        top_sales = df_res.nlargest(5, 'total_sales')[['nama_karyawan', 'jabatan', 'total_sales', 'transactions_handled']]
                        st.markdown("**Top 5 by Sales Value:**"); st.dataframe(top_sales.style.format({"total_sales": "Rp {:,.0f}", "transactions_handled": "{:,.0f}"}))
                    if 'transactions_handled' in df_res.columns:
                        top_transactions = df_res.nlargest(5, 'transactions_handled')[['nama_karyawan', 'jabatan', 'transactions_handled', 'total_sales']]
                        st.markdown("**Top 5 by Transactions Handled:**"); st.dataframe(top_transactions.style.format({"total_sales": "Rp {:,.0f}", "transactions_handled": "{:,.0f}"}))
            with st.expander("Full Combined Data Table", expanded=False): 
                st.dataframe(df_res.style.format({"total_sales": "Rp {:,.0f}", "transactions_handled": "{:,.0f}"}))

# Make sure perform_analysis function is defined (as provided in previous responses)
def perform_analysis(cassandra_session_instance, mongo_db_client_instance, start_date, end_date):
    with st.spinner("Performing combined analysis... This may take a moment."):
        cassandra_perf_df = fetch_cassandra_performance_data(cassandra_session_instance, start_date, end_date)
        if not cassandra_perf_df.empty and 'id_karyawan' in cassandra_perf_df.columns:
            employee_ids = cassandra_perf_df['id_karyawan'].unique().tolist()
            if employee_ids:
                mongo_db_conn = mongo_db_client_instance[DEFAULT_MONGO_DB_NAME]
                mongo_emp_df = fetch_mongo_employee_details(mongo_db_conn, employee_ids)
                if not mongo_emp_df.empty:
                    combined_df = pd.merge(cassandra_perf_df, mongo_emp_df, on="id_karyawan", how="left")
                    combined_df.fillna({"nama_karyawan": "N/A", "jabatan": "N/A"}, inplace=True)
                    st.session_state.combined_analytics_df = combined_df
                else: 
                    st.session_state.combined_analytics_df = cassandra_perf_df 
            else: 
                st.session_state.combined_analytics_df = pd.DataFrame() 
        else: 
            st.session_state.combined_analytics_df = pd.DataFrame()

# ----------------- Main App Display Logic -----------------
if 'mongo_connection_status' in st.session_state and ("Failed" in st.session_state.mongo_connection_status or "Error" in st.session_state.mongo_connection_status):
    st.error(f"MongoDB Status: {st.session_state.mongo_connection_status}")
# elif 'mongo_connection_status' in st.session_state:
#     st.sidebar.success(f"MongoDB: Connected")

if page == 'Home':
    show_home_page()
elif page == 'Combined Analytics':
    cassandra_session_instance = get_cassandra_session()
    show_combined_analytics_page(cassandra_session_instance, mongo_client)
elif page == 'MongoDB Benchmark':
    db_for_benchmark = mongo_client[DEFAULT_MONGO_DB_NAME] if mongo_client else None
    show_mongodb_benchmark_page(
        db_for_benchmark,
        st.session_state.entity_benchmark_sb_select,
        st.session_state.mongo_op_benchmark_sb_select,
        st.session_state.custom_mongo_bm_sb_check
    )
elif page == 'MongoDB Playground':
    show_mongodb_playground_page(mongo_client)
elif page == 'Cassandra Benchmark':
    cassandra_session_instance = get_cassandra_session()
    show_cassandra_benchmark_page(
        cassandra_session_instance,
        st.session_state.custom_cas_bm_page_sb
    )


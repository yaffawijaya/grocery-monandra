import os
import time
import streamlit as st
import pandas as pd
import json
from pymongo import MongoClient, ASCENDING, DESCENDING # For index creation/display
from pymongo.errors import OperationFailure, ConnectionFailure
from bson import ObjectId # For handling ObjectId if necessary, though we mostly convert to str

from dotenv import load_dotenv
from utils.cassandra_utils import get_cassandra_session

# Page config MUST be the first Streamlit command
st.set_page_config(page_title='DB Interaction & Monitor', layout='wide')

# ----------------- Configuration & Initial Setup -----------------
DEFAULT_MONGO_DB_NAME = "grocery_store_db" # Default for benchmark
ENV_PATH = os.path.join('.env')

# Initialize Cassandra session
@st.cache_resource
def init_mongo_client(): # Changed to return client
    """
    Initializes and returns a MongoDB client instance.
    Caches the client for reuse.
    """
    load_dotenv(ENV_PATH)
    conn_str = os.getenv('CONNECTION_STRING')
    if not conn_str:
        st.error(f"Missing CONNECTION_STRING in .env file ({ENV_PATH})")
        st.session_state['mongo_connection_status'] = "CONNECTION_STRING not found."
        return None
    try:
        client = MongoClient(conn_str, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') # Verify connection
        st.session_state['mongo_connection_status'] = "Successfully connected to MongoDB."
        return client
    except ConnectionFailure as e: # More specific exception
        st.session_state['mongo_connection_status'] = f"MongoDB Connection Failed: {e}"
        st.error(f"MongoDB Connection Failed: {e}")
        return None
    except Exception as e: # Catch other potential errors like config error
        st.session_state['mongo_connection_status'] = f"MongoDB Initialization Error: {e}"
        st.error(f"MongoDB Initialization Error: {e}")
        return None

# Initialize client
mongo_client = init_mongo_client()

st.title('Database Interaction & Performance Monitor')

if 'mongo_connection_status' in st.session_state:
    st.info(f"MongoDB Status: {st.session_state['mongo_connection_status']}")

# ----------------- Sidebar -----------------
page_options = ['MongoDB Benchmark', 'MongoDB Playground', 'Cassandra Benchmark'] # Added Playground
page = st.sidebar.radio('Select Page', page_options)
st.sidebar.markdown('---')

# Sidebar content specific to pages (conditional display)
if page == 'Cassandra Benchmark':
    st.sidebar.markdown('**Cassandra Query Input:**')
    use_custom_cassandra = st.sidebar.checkbox('Use Custom Cassandra Queries', True, key='custom_cas')
    # ... (Cassandra sidebar content as before) ...
elif page == 'MongoDB Benchmark':
    st.sidebar.markdown('**MongoDB Benchmark Filter Input:**')
    use_custom_mongo_benchmark = st.sidebar.checkbox('Use Custom MongoDB Filter', True, key='custom_mongo_benchmark')
    selected_entity_benchmark = st.sidebar.selectbox('Select Entity for Benchmark', ['karyawan', 'cabang'], key='entity_benchmark')
    # ... (MongoDB Benchmark sidebar content as before) ...
elif page == 'MongoDB Playground':
    st.sidebar.markdown("**MongoDB Playground Guide**")
    st.sidebar.info("Interact directly with MongoDB. Operations performed here will modify the database.")
    st.sidebar.markdown("Enter database name, collection, and operation details on the main page.")

# ----------------- State Initialization -----------------
def init_results_state():
    result_keys = [
        'cas_non_df', 'cas_non_time', 'cas_idx_df', 'cas_idx_time', 'last_cas_non_q', 'last_cas_idx_q',
        'mongo_bm_non_df', 'mongo_bm_non_time', 'mongo_bm_idx_df', 'mongo_bm_idx_time', 
        'last_mongo_bm_filter_str', 'last_mongo_bm_entity',
        # For Playground
        'playground_db_name', 'playground_collection_name', 'playground_operation_result', 'playground_operation_status'
    ]
    for key in result_keys:
        if key not in st.session_state:
            st.session_state[key] = None
    if 'playground_db_name' not in st.session_state or st.session_state.playground_db_name is None:
        st.session_state.playground_db_name = DEFAULT_MONGO_DB_NAME # Default for playground
init_results_state()

# ----------------- Helper Functions -----------------
# (execute_cassandra_query remains the same)
def execute_cassandra_query(session, cql_query: str):
    if session is None:
        st.error("Cassandra session not available. Connection might have failed.")
        return pd.DataFrame(), 0
    if not cql_query or not cql_query.strip():
        st.warning("Cassandra query is empty. Please enter a valid CQL query.")
        return pd.DataFrame(), 0
    try:
        start_time = time.perf_counter()
        rows = session.execute(str(cql_query))
        duration = time.perf_counter() - start_time
        df = pd.DataFrame(list(rows))
        return df, duration
    except Exception as e:
        st.error(f"Cassandra Query Error: {e}")
        return pd.DataFrame(), 0

# Renamed for benchmark page
def execute_mongodb_benchmark_query(db_connection, collection_name: str, mongo_filter: dict):
    if db_connection is None:
        st.error(f"MongoDB_Benchmark: Database connection to '{DEFAULT_MONGO_DB_NAME}' not available.")
        return pd.DataFrame(), 0
    try:
        collection = db_connection[collection_name]
        start_time = time.perf_counter()
        documents = list(collection.find(mongo_filter))
        duration = time.perf_counter() - start_time
        df = pd.DataFrame(documents)
        if not df.empty and '_id' in df.columns:
            first_id = df['_id'].iloc[0]
            if not isinstance(first_id, str) and not pd.api.types.is_numeric_dtype(first_id):
                df['_id'] = df['_id'].astype(str)
        return df, duration
    except Exception as e:
        st.error(f"MongoDB Benchmark Query Error on '{collection_name}': {e}")
        return pd.DataFrame(), 0

def execute_mongo_playground_operation(client, db_name: str, operation_details: dict):
    if client is None:
        return "MongoDB client not available.", None
    try:
        db_playground = client[db_name] # Get the specific DB for playground
        op_type = operation_details.get("type")
        coll_name = operation_details.get("collection")
        
        # Ensure collection is specified for operations that need it
        if op_type not in ["create_database", "list_collections", "drop_database"] and not coll_name: # drop_database is admin, list_collections can be on db
            return "Collection name is required for this operation.", None

        collection = db_playground[coll_name] if coll_name else None
        result_data = None
        status_message = "Operation successful."

        if op_type == "create_collection":
            # PyMongo creates collection on first write or explicit command.
            # We can use create_collection for options, or just let it be created by an insert.
            # For this playground, let's be explicit if user wants to create it empty.
            db_playground.create_collection(coll_name) # Add try-except for existing collection if needed
            status_message = f"Collection '{coll_name}' created or already exists in database '{db_name}'."
        
        elif op_type == "drop_collection":
            collection.drop()
            status_message = f"Collection '{coll_name}' dropped from database '{db_name}'."

        elif op_type == "insert_documents": # insertMany
            docs_str = operation_details.get("documents", "[]")
            docs = json.loads(docs_str)
            if not isinstance(docs, list): raise ValueError("Documents must be a JSON array.")
            if not docs: raise ValueError("Documents list is empty.")
            result = collection.insert_many(docs)
            status_message = f"Inserted {len(result.inserted_ids)} documents into '{coll_name}'."
            result_data = [{"inserted_ids": [str(id_val) for id_val in result.inserted_ids]}]

        elif op_type == "find_documents":
            filter_str = operation_details.get("filter", "{}")
            projection_str = operation_details.get("projection", "null") # null for no projection
            
            query_filter = json.loads(filter_str)
            projection = json.loads(projection_str) # PyMongo projection can be None

            docs = list(collection.find(query_filter, projection))
            result_data = docs
            status_message = f"Found {len(docs)} documents in '{coll_name}'."

        elif op_type == "update_one":
            filter_str = operation_details.get("filter", "{}")
            update_str = operation_details.get("update", "{}")
            upsert = operation_details.get("upsert", False)
            query_filter = json.loads(filter_str)
            update_doc = json.loads(update_str)
            if not update_doc: raise ValueError("Update document cannot be empty.")

            result = collection.update_one(query_filter, update_doc, upsert=upsert)
            status_message = f"Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted ID: {result.upserted_id}"
            result_data = [{"matched_count": result.matched_count, "modified_count": result.modified_count, "upserted_id": str(result.upserted_id) if result.upserted_id else None}]
        
        elif op_type == "delete_one":
            filter_str = operation_details.get("filter", "{}")
            query_filter = json.loads(filter_str)
            result = collection.delete_one(query_filter)
            status_message = f"Deleted {result.deleted_count} document from '{coll_name}'."
            result_data = [{"deleted_count": result.deleted_count}]

        elif op_type == "aggregate":
            pipeline_str = operation_details.get("pipeline", "[]")
            pipeline = json.loads(pipeline_str)
            if not isinstance(pipeline, list): raise ValueError("Aggregation pipeline must be a JSON array.")
            docs = list(collection.aggregate(pipeline))
            result_data = docs
            status_message = f"Aggregation returned {len(docs)} results from '{coll_name}'."
        
        elif op_type == "count_documents":
            filter_str = operation_details.get("filter", "{}")
            query_filter = json.loads(filter_str)
            count = collection.count_documents(query_filter)
            status_message = f"Found {count} documents matching filter in '{coll_name}'."
            result_data = [{"count": count}]

        elif op_type == "create_index":
            keys_str = operation_details.get("keys", "[]") # e.g., '[["field1", 1], ["field2", -1]]'
            keys = json.loads(keys_str)
            if not isinstance(keys, list): raise ValueError("Index keys must be a JSON array of [field, direction] pairs.")
            index_options_str = operation_details.get("options", "{}") # e.g., '{"name": "myindex", "unique": true}'
            index_options = json.loads(index_options_str)
            
            # Convert direction to PyMongo constants if provided as strings
            parsed_keys = []
            for key_pair in keys:
                if isinstance(key_pair, list) and len(key_pair) == 2:
                    field, direction = key_pair
                    if direction == -1 or str(direction).lower() == "descending":
                        parsed_keys.append((field, DESCENDING))
                    else: # Default to ASCENDING for 1 or other values
                        parsed_keys.append((field, ASCENDING))
                else:
                    raise ValueError("Each key in index definition must be a [field, direction] pair.")

            index_name = collection.create_index(parsed_keys, **index_options)
            status_message = f"Index '{index_name}' created on '{coll_name}'."
            result_data = [{"index_name": index_name}]

        elif op_type == "list_collections":
            coll_names = db_playground.list_collection_names()
            status_message = f"Collections in database '{db_name}': {len(coll_names)}"
            result_data = [{"collection_name": name} for name in coll_names]

        elif op_type == "list_indexes":
            if not collection: return "Collection not specified to list indexes.", None
            indexes = list(collection.list_indexes())
            status_message = f"Indexes on collection '{coll_name}':"
            result_data = []
            for index in indexes:
                # Convert ObjectId in key to str if it exists, for display
                key_info = {k: str(v) if isinstance(v, ObjectId) else v for k,v in index['key'].items()}
                result_data.append({"name": index["name"], "key": key_info, "v": index.get("v"), "unique": index.get("unique", False)})
        else:
            return f"Unsupported operation type: {op_type}", None

        return status_message, pd.DataFrame(result_data) if result_data is not None else pd.DataFrame()

    except json.JSONDecodeError as e:
        return f"JSON Parsing Error: {e}", None
    except OperationFailure as e: # PyMongo specific operation errors
        return f"MongoDB Operation Failure: {e.details}", None
    except ValueError as e: # For custom validation
        return f"Input Error: {e}", None
    except Exception as e:
        return f"An unexpected error occurred: {e}", None

def display_db_collection_info_playground(client, db_name):
    if client is None: return
    st.header("Database and Collections Guide")
    if not db_name:
        st.warning("Please enter a database name to see its collections.")
        return
    try:
        db_playground = client[db_name]
        st.markdown(f"**Inspecting Database: `{db_name}`**")
        collection_names = db_playground.list_collection_names()
        if not collection_names:
            st.info("This database has no collections yet.")
        
        selected_coll_to_inspect = st.selectbox("Select a collection to inspect its schema/indexes:", options=["None"] + collection_names, key="coll_inspect_select")

        if selected_coll_to_inspect and selected_coll_to_inspect != "None":
            collection = db_playground[selected_coll_to_inspect]
            st.subheader(f"Info for Collection: `{selected_coll_to_inspect}`")
            
            sample_doc = collection.find_one()
            if sample_doc:
                st.markdown("**Sample Document Fields (and data types from one document):**")
                fields_info = {key: str(type(value).__name__) for key, value in sample_doc.items()}
                # Convert ObjectId for display
                if '_id' in fields_info and isinstance(sample_doc['_id'], ObjectId):
                    fields_info['_id'] += f" (e.g., '{str(sample_doc['_id'])}')"
                st.json(fields_info, expanded=False)
            else:
                st.markdown("Collection is empty or no sample document found.")
            
            st.markdown("**Indexes on this collection:**")
            indexes = list(collection.list_indexes())
            if indexes:
                for index_spec in indexes:
                    index_info = {"name": index_spec["name"], "key": index_spec["key"]}
                    if "unique" in index_spec: index_info["unique"] = index_spec["unique"]
                    st.json(index_info, expanded=False) # Displaying keys as dict is fine
            else:
                st.text("No user-defined indexes found (besides default _id).")
        st.markdown("---")
    except Exception as e:
        st.error(f"Error fetching database/collection info for '{db_name}': {e}")


# ----------------- Page Implementations -----------------

if page == 'MongoDB Benchmark':
    st.header('MongoDB Benchmark (Find Queries)')
    # Use a dedicated db object for benchmark, connected to the default DB name
    db_for_benchmark = mongo_client[DEFAULT_MONGO_DB_NAME] if mongo_client else None

    if db_for_benchmark is not None:
        # ... (MongoDB Benchmark page logic as largely provided by user, using execute_mongodb_benchmark_query) ...
        # Ensure to use 'selected_entity_benchmark' and 'use_custom_mongo_benchmark'
        default_mongo_filter_str = '{}'
        
        if use_custom_mongo_benchmark:
            initial_text_area_value = st.session_state.get('last_mongo_bm_filter_str')
            if initial_text_area_value is None:
                initial_text_area_value = default_mongo_filter_str
            
            mongo_filter_str_bm = st.text_area(
                f"MongoDB Filter for `{selected_entity_benchmark}` (JSON format)",
                initial_text_area_value, height=100, key='mongo_bm_filter'
            )
        else:
            st.markdown(f"Using default empty filter (`{{}}`) for `{selected_entity_benchmark}` collections.")
            mongo_filter_str_bm = default_mongo_filter_str
            st.code(mongo_filter_str_bm, language='json')
        
        try:
            current_mongo_filter_bm = json.loads(mongo_filter_str_bm)
            if not isinstance(current_mongo_filter_bm, dict):
                st.error("Filter must be a valid JSON object (dictionary). Using empty filter {} instead.")
                current_mongo_filter_bm = {}
        except Exception as e:
            st.error(f"Invalid JSON filter for benchmark: {e}. Using empty filter {{}} instead.")
            current_mongo_filter_bm = {}
            mongo_filter_str_bm = default_mongo_filter_str

        non_indexed_coll_name = selected_entity_benchmark
        indexed_coll_name = f"indexed_{selected_entity_benchmark}"

        col1_mongo_bm, col2_mongo_bm = st.columns(2)
        with col1_mongo_bm:
            if st.button(f'Run on `{non_indexed_coll_name}` (Benchmark)'):
                st.session_state.last_mongo_bm_filter_str = mongo_filter_str_bm
                st.session_state.last_mongo_bm_entity = selected_entity_benchmark
                df, t = execute_mongodb_benchmark_query(db_for_benchmark, non_indexed_coll_name, current_mongo_filter_bm)
                st.session_state.mongo_bm_non_df = df
                st.session_state.mongo_bm_non_time = t
        with col2_mongo_bm:
            if st.button(f'Run on `{indexed_coll_name}` (Benchmark)'):
                st.session_state.last_mongo_bm_filter_str = mongo_filter_str_bm
                st.session_state.last_mongo_bm_entity = selected_entity_benchmark
                df, t = execute_mongodb_benchmark_query(db_for_benchmark, indexed_coll_name, current_mongo_filter_bm)
                st.session_state.mongo_bm_idx_df = df
                st.session_state.mongo_bm_idx_time = t
        
        st.markdown("---")
        # ... (Rest of benchmark display logic, ensure keys are mongo_bm_... )
        results_col1_mongo_bm, results_col2_mongo_bm = st.columns(2)
        display_entity_bm = st.session_state.get('last_mongo_bm_entity', selected_entity_benchmark)
        display_filter_str_bm = st.session_state.get('last_mongo_bm_filter_str', mongo_filter_str_bm)
        try:
            display_filter_obj_bm = json.loads(display_filter_str_bm) if display_filter_str_bm else {}
        except: display_filter_obj_bm = {}

        with results_col1_mongo_bm:
            if st.session_state.mongo_bm_non_df is not None:
                st.subheader(f'`{display_entity_bm}` Benchmark Results (Non-Indexed)')
                st.code(f"db['{display_entity_bm}'].find({json.dumps(display_filter_obj_bm)})", language='python')
                st.write(f'Execution Time: {st.session_state.mongo_bm_non_time:.4f} seconds')
                st.dataframe(st.session_state.mongo_bm_non_df)
        with results_col2_mongo_bm:
            if st.session_state.mongo_bm_idx_df is not None:
                st.subheader(f'`indexed_{display_entity_bm}` Benchmark Results (Indexed)')
                st.code(f"db['indexed_{display_entity_bm}'].find({json.dumps(display_filter_obj_bm)})", language='python')
                st.write(f'Execution Time: {st.session_state.mongo_bm_idx_time:.4f} seconds')
                st.dataframe(st.session_state.mongo_bm_idx_df)

        if st.session_state.mongo_bm_non_time is not None and st.session_state.mongo_bm_idx_time is not None:
            st.markdown("---")
            # ... (Comparison chart for benchmark)
            st.subheader(f'Execution Time Comparison (MongoDB Benchmark - {display_entity_bm})')
            comp_data_mongo = {
                'Collection Type': [f'`{display_entity_bm}` (Non-Indexed)', f'`indexed_{display_entity_bm}` (Indexed)'],
                'Time (s)': [st.session_state.mongo_bm_non_time, st.session_state.mongo_bm_idx_time]
            }
            comp_df_mongo = pd.DataFrame(comp_data_mongo).set_index('Collection Type')
            st.bar_chart(comp_df_mongo)

    else:
        st.warning("MongoDB connection could not be established for Benchmark. Please check .env file and console for errors.")


elif page == 'MongoDB Playground':
    st.header('MongoDB Playground')
    if mongo_client is None: # Check if the client itself is None
        st.error("MongoDB client not initialized. Cannot proceed with Playground.")
    else:
        # --- Database and Collection Info ---
        playground_db_name_input = st.text_input("Target Database Name", 
                                                 value=st.session_state.get('playground_db_name', DEFAULT_MONGO_DB_NAME), 
                                                 key="pg_db_name_input")
        
        # Update session state if input changes (debouncing might be needed for many rapid changes, but usually ok)
        if playground_db_name_input != st.session_state.playground_db_name:
             st.session_state.playground_db_name = playground_db_name_input
        
        if st.button("Load/Refresh Database Info", key="load_db_info_pg"):
            # This button isn't strictly necessary if info updates on db name change, but can be explicit
            pass # display_db_collection_info_playground will use current session_state.playground_db_name

        display_db_collection_info_playground(mongo_client, st.session_state.playground_db_name)

        # --- Operation Selection ---
        st.subheader("Execute MongoDB Operation")
        
        # Use columns for better layout
        col_op1, col_op2 = st.columns(2)
        with col_op1:
            selected_collection_pg = st.text_input("Target Collection Name (for chosen DB)", 
                                                   key="pg_coll_name", 
                                                   help="Required for most operations.")
        with col_op2:
            operation_type = st.selectbox("Select Operation", 
                                          ["list_collections", "create_collection", "drop_collection", 
                                           "insert_documents", "find_documents", "update_one", 
                                           "delete_one", "aggregate", "count_documents", "create_index", "list_indexes"],
                                          key="pg_op_type")
        
        # --- Dynamic Inputs based on Operation ---
        op_details = {"type": operation_type, "collection": selected_collection_pg}
        
        if operation_type == "create_collection":
            st.caption(f"This will create collection '{selected_collection_pg}' in database '{st.session_state.playground_db_name}'.")
        elif operation_type == "drop_collection":
            st.caption(f"Warning: This will drop collection '{selected_collection_pg}' from database '{st.session_state.playground_db_name}'.")
        elif operation_type == "insert_documents":
            op_details["documents"] = st.text_area("Documents (JSON Array of objects)", height=150, key="pg_insert_docs", 
                                                   value='[{"example_field": "example_value"}]',
                                                   help='Example: [{"name": "Product A", "price": 100}, {"name": "Product B", "price": 150}]')
        elif operation_type == "find_documents":
            op_details["filter"] = st.text_area("Filter (JSON Object)", value="{}", height=75, key="pg_find_filter")
            op_details["projection"] = st.text_area("Projection (JSON Object or null for all fields)", value="null", height=75, key="pg_find_projection", help='Example: {"name": 1, "_id": 0} or null')
        elif operation_type == "update_one":
            op_details["filter"] = st.text_area("Filter (JSON Object to find document)", value="{}", height=75, key="pg_update_filter")
            op_details["update"] = st.text_area("Update Document (JSON Object, e.g., using $set)", value='{"$set": {"status": "updated"}}', height=100, key="pg_update_doc")
            op_details["upsert"] = st.checkbox("Upsert (insert if not found?)", key="pg_update_upsert")
        elif operation_type == "delete_one":
            op_details["filter"] = st.text_area("Filter (JSON Object for document to delete)", value="{}", height=75, key="pg_delete_filter")
        elif operation_type == "aggregate":
            op_details["pipeline"] = st.text_area("Aggregation Pipeline (JSON Array of stages)", height=200, key="pg_agg_pipeline",
                                                  value='[{"$match": {"status": "active"}}, {"$group": {"_id": "$category", "count": {"$sum": 1}}}]',
                                                  help='Example: [{"$match": {"field": "value"}}, {"$group": {"_id": "$groupField", "total": {"$sum": "$sumField"}}}]')
        elif operation_type == "count_documents":
            op_details["filter"] = st.text_area("Filter (JSON Object)", value="{}", height=75, key="pg_count_filter")
        elif operation_type == "create_index":
            op_details["keys"] = st.text_area("Index Keys (JSON Array of [field, direction] pairs)", 
                                              value='[["fieldName", 1]]', height=75, key="pg_create_idx_keys",
                                              help='Example: [["field1", 1], ["field2", -1]] where 1 is ASC, -1 is DESC.')
            op_details["options"] = st.text_area("Index Options (JSON Object, optional)", 
                                                 value='{}', height=75, key="pg_create_idx_options",
                                                 help='Example: {"name": "my_custom_index_name", "unique": true}')
        # list_collections and list_indexes don't need more specific inputs here beyond db/collection name.

        if st.button("Execute MongoDB Operation", key="pg_execute_op"):
            if not st.session_state.playground_db_name:
                st.error("Database name cannot be empty for Playground operations.")
            else:
                status, result_df = execute_mongo_playground_operation(mongo_client, st.session_state.playground_db_name, op_details)
                st.session_state.playground_operation_status = status
                st.session_state.playground_operation_result = result_df
        
        # --- Display Playground Operation Results ---
        if 'playground_operation_status' in st.session_state and st.session_state.playground_operation_status:
            st.info(f"Status: {st.session_state.playground_operation_status}")
        if 'playground_operation_result' in st.session_state and st.session_state.playground_operation_result is not None:
            if not st.session_state.playground_operation_result.empty:
                st.markdown("**Result Data:**")
                st.dataframe(st.session_state.playground_operation_result)
            elif "success" in str(st.session_state.playground_operation_status).lower() and not "found" in str(st.session_state.playground_operation_status).lower() and not "returned" in str(st.session_state.playground_operation_status).lower() :
                 pass # Don't show empty df if it was a successful DML/DDL with no data to return (e.g. create coll)
            else:
                st.text("No data returned or operation did not produce tabular data.")


elif page == 'Cassandra Benchmark': # Keep existing Cassandra Benchmark page
    # ... (Cassandra Benchmark page logic as previously corrected and provided) ...
    # Ensure variable names like use_custom_cassandra are unique or correctly scoped if reused from sidebar
    st.header('Cassandra Benchmark')
    cassandra_session = get_cassandra_session() 

    if cassandra_session is not None: 
        default_cql_non = "SELECT * FROM transaksi_harian WHERE id_cabang = 'CB001' LIMIT 10 ALLOW FILTERING;"
        default_cql_idx = "SELECT * FROM indexed_transaksi_harian WHERE id_cabang = 'CB001' AND id_karyawan = 'KR0001' AND nama_barang = 'Beras Premium 5kg' LIMIT 10;"

        # Assuming use_custom_cassandra is defined from sidebar for this page
        current_use_custom_cassandra = st.session_state.get('custom_cas', True) # Get value from key set in sidebar

        if current_use_custom_cassandra:
            cql_non_indexed = st.text_area('Query for `transaksi_harian` (Non-Indexed Nature)', st.session_state.get('last_cas_non_q', default_cql_non), height=100, key='cql_non_bm') # Unique key
            cql_indexed = st.text_area('Query for `indexed_transaksi_harian` (Indexed Nature)', st.session_state.get('last_cas_idx_q', default_cql_idx), height=100, key='cql_idx_bm') # Unique key
        else:
            st.markdown("Using default recommended queries:")
            cql_non_indexed = default_cql_non
            cql_indexed = default_cql_idx
            st.code(cql_non_indexed, language='sql')
            st.code(cql_indexed, language='sql')

        col1_cas_bm, col2_cas_bm = st.columns(2)
        with col1_cas_bm:
            if st.button('Run on `transaksi_harian`', key='run_cas_non_bm'):
                st.session_state.last_cas_non_q = cql_non_indexed
                df_res, t_res = execute_cassandra_query(cassandra_session, cql_non_indexed)
                st.session_state.cas_non_df = df_res
                st.session_state.cas_non_time = t_res
        with col2_cas_bm:
            if st.button('Run on `indexed_transaksi_harian`', key='run_cas_idx_bm'):
                st.session_state.last_cas_idx_q = cql_indexed
                df_res, t_res = execute_cassandra_query(cassandra_session, cql_indexed)
                st.session_state.cas_idx_df = df_res
                st.session_state.cas_idx_time = t_res
        
        st.markdown("---")
        results_col1_cas_bm, results_col2_cas_bm = st.columns(2)
        with results_col1_cas_bm:
            if st.session_state.cas_non_df is not None:
                st.subheader('`transaksi_harian` Results')
                st.code(st.session_state.last_cas_non_q, language='sql')
                st.write(f'Execution Time: {st.session_state.cas_non_time:.4f} seconds')
                st.dataframe(st.session_state.cas_non_df)
        with results_col2_cas_bm:
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
        st.warning("Cassandra session could not be established. Cannot run Cassandra Benchmark queries.")
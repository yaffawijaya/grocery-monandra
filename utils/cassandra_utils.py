# utils/cassandra_utils.py
import streamlit as st
from cassandra.cluster import Cluster

CASSANDRA_CONTACT_POINTS = ["127.0.0.1"]
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "day_grocery" # As defined in your notebook

@st.cache_resource # Cache the session across reruns for the app's lifetime
def init_cassandra_connection():
    """
    Initializes and returns a Cassandra session.
    Caches the session object for reuse.
    """
    try:
        cluster = Cluster(contact_points=CASSANDRA_CONTACT_POINTS, port=CASSANDRA_PORT)
        # Connect without specifying keyspace first to ensure keyspace creation can happen
        session = cluster.connect()
        # Create keyspace if it doesn't exist (idempotent)
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
            WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
        """)
        # Switch to the desired keyspace
        session.set_keyspace(CASSANDRA_KEYSPACE)
        # Store status in session_state to inform the user, not strictly necessary for functionality
        st.session_state['cassandra_connection_status'] = f"Successfully connected to Cassandra keyspace: {CASSANDRA_KEYSPACE}"
        return session
    except Exception as e:
        st.session_state['cassandra_connection_status'] = f"Failed to connect to Cassandra: {e}"
        # Display error in the app when connection fails
        st.error(f"Cassandra Connection Error: {e}")
        return None

def get_cassandra_session():
    """
    Retrieves the cached Cassandra session.
    If connection failed, it will return None and an error would have been displayed.
    """
    return init_cassandra_connection()

# Note: Streamlit's @st.cache_resource is designed to handle the lifecycle of resources,
# including cleanup like shutting down the cluster when the app session ends or script reruns.
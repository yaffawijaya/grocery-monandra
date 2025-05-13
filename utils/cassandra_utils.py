# utils/cassandra_utils.py
from cassandra.cluster import Cluster


def get_cassandra_session(keyspace: str = "groceries"):
    """
    Connect to Cassandra and set keyspace using default AsyncoreConnection (Python 3.11+).
    """
    cluster = Cluster(contact_points=["127.0.0.1"], port=9042)
    session = cluster.connect(keyspace)
    return session

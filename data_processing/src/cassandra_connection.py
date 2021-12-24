from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
import config


class Cassandra_connection:

    session = None

    def __init__(self):
        # Connecting to a Cassandra DB
        '''
        auth_provider = PlainTextAuthProvider(
            username=config.CASSANDRA_USER_NAME, password=config.CASSANDRA_PASSWORD)
        cluster = Cluster([config.CASSANDRA_HOST], load_balancing_policy=DCAwareRoundRobinPolicy(
            local_dc='US-WEST'), port=9042, auth_provider=auth_provider)
        '''
        cluster = Cluster(contact_points=[config.CASSANDRA_HOST])
        self.session = cluster.connect()

    def create_key_space(self, key_space_name):
        # Creating a Kesyspace and a table
        print('Creating a keyspace')
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS %s
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
            """ % key_space_name)

        print('setting a keyspace...')
        self.session.set_keyspace(key_space_name)

    def create_table(self, table_name):
        print('creating a table...')
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS %s (
                "item_id" int primary key,
                "item_name" text,
                "description" text,
                "price" float,
                "tax" float,
                "tax_with_price" float
            )
             """ % table_name)

    def close_connection(self):
        print('Shutting down Cassandra session')
        self.session.shutdown()

"""
Schema setup module for TPC-C Cassandra benchmark.
Handles keyspace and table creation.
"""

import logging
import os
from typing import Optional
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
import yaml

logger = logging.getLogger(__name__)


class SchemaSetup:
    """Handles Cassandra schema setup for TPC-C benchmark."""
    
    def __init__(self, config_path: str = "config/cassandra_config.yaml"):
        """
        Initialize schema setup with configuration.
        
        Args:
            config_path: Path to Cassandra configuration file
        """
        self.config = self._load_config(config_path)
        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def connect(self) -> Session:
        """
        Establish connection to Cassandra cluster.
        
        Returns:
            Cassandra session object
        """
        cassandra_config = self.config['cassandra']
        
        # Setup authentication if username provided
        auth_provider = None
        if cassandra_config.get('username'):
            auth_provider = PlainTextAuthProvider(
                username=cassandra_config['username'],
                password=cassandra_config.get('password', '')
            )
        
        # Create cluster connection
        self.cluster = Cluster(
            contact_points=cassandra_config['contact_points'],
            port=cassandra_config['port'],
            auth_provider=auth_provider,
            protocol_version=cassandra_config.get('protocol_version', 4)
        )
        
        self.session = self.cluster.connect()
        logger.info(f"Connected to Cassandra cluster at {cassandra_config['contact_points']}")
        
        return self.session
    
    def create_keyspace(self, replication_factor: int = 3) -> None:
        """
        Create TPC-C keyspace if it doesn't exist.
        
        Args:
            replication_factor: Replication factor for the keyspace
        """
        keyspace = self.config['cassandra']['keyspace']
        
        # For local testing, use SimpleStrategy with RF=1
        # For production, adjust based on cluster size
        create_keyspace_query = f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {replication_factor}}}
        """
        
        self.session.execute(create_keyspace_query)
        logger.info(f"Keyspace '{keyspace}' created/verified")
        
        # Use the keyspace
        self.session.set_keyspace(keyspace)
        logger.info(f"Using keyspace '{keyspace}'")
    
    def drop_keyspace(self) -> None:
        """Drop the TPC-C keyspace (use with caution)."""
        keyspace = self.config['cassandra']['keyspace']
        
        drop_query = f"DROP KEYSPACE IF EXISTS {keyspace}"
        self.session.execute(drop_query)
        logger.info(f"Keyspace '{keyspace}' dropped")
    
    def create_tables(self, schema_file: str = "schema/tpcc_schema.cql") -> None:
        """
        Create all TPC-C tables from schema file.
        
        Args:
            schema_file: Path to CQL schema file
        """
        # Read schema file
        with open(schema_file, 'r') as f:
            schema_content = f.read()
        
        # Split into individual statements
        statements = [stmt.strip() for stmt in schema_content.split(';') if stmt.strip()]
        
        # Execute each statement
        for statement in statements:
            # Skip comments and USE statements (already using keyspace)
            if statement.startswith('--') or statement.upper().startswith('USE'):
                continue
            
            # Skip DROP and CREATE KEYSPACE (handled separately)
            if 'DROP KEYSPACE' in statement.upper() or 'CREATE KEYSPACE' in statement.upper():
                continue
            
            try:
                self.session.execute(statement)
                # Extract table name for logging
                if 'CREATE TABLE' in statement.upper():
                    table_name = statement.split('TABLE')[1].split('(')[0].strip()
                    logger.info(f"Created/verified table: {table_name}")
                elif 'CREATE INDEX' in statement.upper():
                    index_name = statement.split('INDEX')[1].split('ON')[0].strip()
                    logger.info(f"Created/verified index: {index_name}")
            except Exception as e:
                logger.error(f"Error executing statement: {statement[:100]}...")
                logger.error(f"Error: {e}")
                raise
    
    def verify_schema(self) -> bool:
        """
        Verify that all expected tables exist.
        
        Returns:
            True if all tables exist, False otherwise
        """
        expected_tables = [
            'warehouse', 'district', 'customer', 'customer_by_name',
            'item', 'stock', 'orders', 'orders_by_customer',
            'new_order', 'order_line', 'history'
        ]
        
        keyspace = self.config['cassandra']['keyspace']
        
        for table in expected_tables:
            query = f"""
                SELECT table_name FROM system_schema.tables 
                WHERE keyspace_name = '{keyspace}' AND table_name = '{table}'
            """
            result = self.session.execute(query)
            if not result.one():
                logger.error(f"Table '{table}' not found in keyspace '{keyspace}'")
                return False
            logger.info(f"Verified table: {table}")
        
        logger.info("All tables verified successfully")
        return True
    
    def close(self) -> None:
        """Close Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")
    
    def setup_complete_schema(self, replication_factor: int = 1) -> None:
        """
        Complete schema setup: connect, create keyspace, and create tables.
        
        Args:
            replication_factor: Replication factor for the keyspace (default 1 for testing)
        """
        try:
            self.connect()
            self.create_keyspace(replication_factor=replication_factor)
            self.create_tables()
            self.verify_schema()
            logger.info("Schema setup completed successfully")
        except Exception as e:
            logger.error(f"Schema setup failed: {e}")
            raise
        finally:
            self.close()


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run schema setup
    setup = SchemaSetup()
    setup.setup_complete_schema()

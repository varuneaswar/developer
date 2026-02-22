"""
Schema setup module for TPC-E Cassandra benchmark.
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
    """Handles Cassandra schema setup for TPC-E benchmark."""

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

        auth_provider = None
        if cassandra_config.get('username'):
            auth_provider = PlainTextAuthProvider(
                username=cassandra_config['username'],
                password=cassandra_config.get('password', '')
            )

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
        Create TPC-E keyspace if it doesn't exist.

        Args:
            replication_factor: Replication factor for the keyspace
        """
        keyspace = self.config['cassandra']['keyspace']

        create_keyspace_query = f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {replication_factor}}}
        """

        self.session.execute(create_keyspace_query)
        logger.info(f"Keyspace '{keyspace}' created/verified")

        self.session.set_keyspace(keyspace)
        logger.info(f"Using keyspace '{keyspace}'")

    def drop_keyspace(self) -> None:
        """Drop the TPC-E keyspace (use with caution)."""
        keyspace = self.config['cassandra']['keyspace']
        self.session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        logger.info(f"Keyspace '{keyspace}' dropped")

    def create_tables(self, schema_file: str = "schema/tpce_schema.cql") -> None:
        """
        Create all TPC-E tables from schema file.

        Args:
            schema_file: Path to CQL schema file
        """
        with open(schema_file, 'r') as f:
            schema_content = f.read()

        statements = [stmt.strip() for stmt in schema_content.split(';') if stmt.strip()]

        for statement in statements:
            if statement.startswith('--') or statement.upper().startswith('USE'):
                continue
            if 'DROP KEYSPACE' in statement.upper() or 'CREATE KEYSPACE' in statement.upper():
                continue

            try:
                self.session.execute(statement)
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
        Verify that all expected TPC-E tables exist.

        Returns:
            True if all tables exist, False otherwise
        """
        expected_tables = [
            'customer', 'customer_account', 'broker', 'security', 'trade',
            'trade_history', 'settlement', 'company', 'exchange', 'industry',
            'sector', 'daily_market', 'financial', 'last_trade', 'news_item',
            'news_xref', 'holding', 'holding_summary', 'holding_history',
            'watch_list', 'watch_item', 'address', 'zip_code', 'status_type',
            'trade_type', 'charge', 'commission_rate', 'taxrate', 'customer_taxrate',
            'trade_by_account', 'trade_by_symbol', 'holding_by_account',
            'news_by_company', 'daily_market_by_symbol',
        ]

        keyspace = self.config['cassandra']['keyspace']

        for table in expected_tables:
            query = (
                f"SELECT table_name FROM system_schema.tables "
                f"WHERE keyspace_name = '{keyspace}' AND table_name = '{table}'"
            )
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
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    setup = SchemaSetup()
    setup.setup_complete_schema()

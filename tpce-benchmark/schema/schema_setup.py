"""
Schema setup module for TPC-E Cassandra benchmark.
Handles keyspace and table creation.
"""

import logging
import os
from typing import Optional
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.concurrent import execute_concurrent_with_args
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

    @classmethod
    def from_session(cls, session, config: dict) -> 'SchemaSetup':
        """
        Create a SchemaSetup instance that reuses an existing Cassandra session.

        Args:
            session: An already-connected Cassandra Session object
            config: Cassandra configuration dict (same structure as cassandra_config.yaml)

        Returns:
            SchemaSetup instance backed by the provided session
        """
        instance = cls.__new__(cls)
        instance.config = config
        instance.cluster = None
        instance.session = session
        return instance

    def snapshot_keyspace(self, source_keyspace: str, target_keyspace: str,
                          schema_file: str, replication_factor: int = 1) -> None:
        """
        Create an isolated snapshot of *source_keyspace* in *target_keyspace*.

        The snapshot contains an identical schema and a full copy of all table
        data.  Counter tables are excluded from the data copy because Cassandra
        does not allow INSERT on counter columns; those tables start at zero in
        the snapshot.

        Args:
            source_keyspace: Name of the keyspace to copy from
            target_keyspace: Name of the new snapshot keyspace
            schema_file: Path to the CQL schema file used to recreate tables
            replication_factor: Replication factor for the snapshot keyspace
        """
        # Create snapshot keyspace
        self.session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {target_keyspace} "
            f"WITH replication = {{'class': 'SimpleStrategy', "
            f"'replication_factor': {replication_factor}}}"
        )
        logger.info(f"Created snapshot keyspace: '{target_keyspace}'")

        # Create schema in snapshot keyspace
        self.session.set_keyspace(target_keyspace)
        self.create_tables(schema_file=schema_file)
        logger.info(f"Schema created in snapshot keyspace: '{target_keyspace}'")

        # Copy data row-by-row from source to snapshot
        self._copy_keyspace_data(source_keyspace, target_keyspace)
        logger.info(f"Snapshot keyspace '{target_keyspace}' is ready for benchmarking")

    def _copy_keyspace_data(self, source_keyspace: str, target_keyspace: str) -> None:
        """
        Copy all rows from every non-counter table in *source_keyspace* into
        the corresponding tables in *target_keyspace*.

        Counter tables are skipped because counter columns cannot be set via
        INSERT; they always start at zero in the snapshot keyspace.

        Args:
            source_keyspace: Name of the source keyspace
            target_keyspace: Name of the destination keyspace
        """
        tables_result = self.session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s",
            (source_keyspace,)
        )
        tables = [row.table_name for row in tables_result]

        for table in tables:
            # Retrieve column metadata for this table
            columns_result = self.session.execute(
                "SELECT column_name, type FROM system_schema.columns "
                "WHERE keyspace_name = %s AND table_name = %s",
                (source_keyspace, table)
            )
            columns = list(columns_result)

            # Skip counter tables — counter values cannot be INSERTed
            if any(col.type == 'counter' for col in columns):
                logger.info(f"Skipping counter table '{table}' (counter columns not copyable)")
                continue

            col_names = [col.column_name for col in columns]
            col_list = ', '.join(col_names)
            placeholders = ', '.join(['?' for _ in col_names])

            insert_stmt = self.session.prepare(
                f"INSERT INTO {target_keyspace}.{table} ({col_list}) "
                f"VALUES ({placeholders})"
            )

            rows = self.session.execute(
                f"SELECT {col_list} FROM {source_keyspace}.{table}"
            )
            params = [
                [getattr(row, col) for col in col_names]
                for row in rows
            ]

            if params:
                execute_concurrent_with_args(
                    self.session, insert_stmt, params, concurrency=50
                )

            logger.info(
                f"Copied {len(params)} rows: "
                f"{source_keyspace}.{table} -> {target_keyspace}.{table}"
            )

    def drop_snapshot_keyspace(self, keyspace: str) -> None:
        """
        Drop a snapshot keyspace created by :meth:`snapshot_keyspace`.

        Args:
            keyspace: Name of the snapshot keyspace to drop
        """
        self.session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        logger.info(f"Dropped snapshot keyspace: '{keyspace}'")

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

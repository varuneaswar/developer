#!/usr/bin/env python3
"""
Main entry point for Cassandra TPC-C Benchmark Framework.
Provides CLI interface for all benchmark operations.
"""

import logging
import sys
from pathlib import Path

import click

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

import yaml  # noqa: E402
from cassandra.auth import PlainTextAuthProvider  # noqa: E402
from cassandra.cluster import Cluster  # noqa: E402
from data_generator.data_loader import DataLoader  # noqa: E402
from data_generator.tpcc_data_generator import TPCCDataGenerator  # noqa: E402
from schema.schema_setup import SchemaSetup  # noqa: E402
from test_harness.benchmark_runner import BenchmarkRunner  # noqa: E402

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("benchmark.log")],
)

logger = logging.getLogger(__name__)


def load_cassandra_config(config_path: str = "config/cassandra_config.yaml") -> dict:
    """Load Cassandra configuration."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def load_benchmark_config(config_path: str = "config/benchmark_config.yaml") -> dict:
    """Load benchmark configuration."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def cli(verbose):
    """Cassandra TPC-C Benchmark Framework.

    A comprehensive benchmarking tool for Cassandra using TPC-C data model.
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")


@cli.command()
@click.option("--replication-factor", "-r", default=1, help="Replication factor (default: 1)")
@click.option(
    "--config", "-c", default="config/cassandra_config.yaml", help="Path to Cassandra config file"
)
def setup_schema(replication_factor, config):
    """Create keyspace and tables for TPC-C benchmark."""
    try:
        logger.info("Setting up Cassandra schema...")
        setup = SchemaSetup(config_path=config)
        setup.setup_complete_schema(replication_factor=replication_factor)
        logger.info("✓ Schema setup completed successfully")
    except Exception as e:
        logger.error(f"✗ Schema setup failed: {e}")
        sys.exit(1)


@cli.command()
@click.option(
    "--cassandra-config",
    "-c",
    default="config/cassandra_config.yaml",
    help="Path to Cassandra config file",
)
@click.option(
    "--benchmark-config",
    "-b",
    default="config/benchmark_config.yaml",
    help="Path to benchmark config file",
)
@click.option(
    "--sample-only", is_flag=True, help="Generate only a small sample of data for testing"
)
def generate_data(cassandra_config, benchmark_config, sample_only):
    """Generate and load TPC-C data into Cassandra."""
    try:
        logger.info("Starting data generation and loading...")

        # Load configurations
        cass_config = load_cassandra_config(cassandra_config)
        bench_config = load_benchmark_config(benchmark_config)

        # Get data generation parameters
        data_config = bench_config["data_generation"]

        if sample_only:
            logger.info("Sample mode: generating minimal data for testing")
            num_warehouses = 2
            num_districts = 2
            num_customers = 100
            num_items = 1000
        else:
            num_warehouses = data_config["num_warehouses"]
            num_districts = data_config["num_districts_per_warehouse"]
            num_customers = data_config["num_customers_per_district"]
            num_items = data_config["num_items"]

        # Create data generator
        generator = TPCCDataGenerator(
            num_warehouses=num_warehouses,
            num_districts_per_warehouse=num_districts,
            num_customers_per_district=num_customers,
            num_items=num_items,
        )

        # Display scale information
        scale_info = generator.get_scale_info()
        logger.info("Data scale:")
        logger.info(f"  Warehouses: {scale_info['warehouses']}")
        logger.info(f"  Districts: {scale_info['districts']}")
        logger.info(f"  Customers: {scale_info['customers']}")
        logger.info(f"  Items: {scale_info['items']}")
        logger.info(f"  Estimated total records: {scale_info['estimated_total_records']:,}")

        # Connect to Cassandra
        keyspace = cass_config["cassandra"]["keyspace"]
        auth_provider = None
        if cass_config["cassandra"].get("username"):
            auth_provider = PlainTextAuthProvider(
                username=cass_config["cassandra"]["username"],
                password=cass_config["cassandra"].get("password", ""),
            )

        cluster = Cluster(
            contact_points=cass_config["cassandra"]["contact_points"],
            port=cass_config["cassandra"]["port"],
            auth_provider=auth_provider,
        )
        session = cluster.connect()
        session.set_keyspace(keyspace)

        # Create data loader
        loader = DataLoader(session, generator)

        # Load data
        result = loader.load_all_data()

        logger.info("✓ Data generation and loading completed successfully")
        logger.info(f"  Warehouses loaded: {result['warehouses']}")
        logger.info(f"  Districts loaded: {result['districts']}")
        logger.info(f"  Customers loaded: {result['customers']}")
        logger.info(f"  Items loaded: {result['items']}")

        cluster.shutdown()

    except Exception as e:
        logger.error(f"✗ Data generation failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.option(
    "--cassandra-config",
    "-c",
    default="config/cassandra_config.yaml",
    help="Path to Cassandra config file",
)
@click.option(
    "--benchmark-config",
    "-b",
    default="config/benchmark_config.yaml",
    help="Path to benchmark config file",
)
@click.option("--dry-run", is_flag=True, help="Validate configuration without running")
def run_benchmark(cassandra_config, benchmark_config, dry_run):
    """Execute the full benchmark suite."""
    try:
        logger.info("Initializing benchmark runner...")

        runner = BenchmarkRunner(
            cassandra_config_path=cassandra_config, benchmark_config_path=benchmark_config
        )

        if dry_run:
            logger.info("Running in dry-run mode...")

        results = runner.run_benchmark(dry_run=dry_run)

        if not dry_run:
            logger.info("✓ Benchmark execution completed")
            logger.info(f"  Total queries executed: {results.get('total_queries', 0):,}")
            logger.info(f"  Average QPS: {results.get('queries_per_second', 0):.2f}")
            logger.info(f"  p95 latency: {results.get('p95_latency_ms', 0):.2f} ms")
            logger.info(f"  p99 latency: {results.get('p99_latency_ms', 0):.2f} ms")

        runner.close()

    except Exception as e:
        logger.error(f"✗ Benchmark execution failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.argument("query_type", type=click.Choice(["select", "insert", "update", "delete"]))
@click.option(
    "--cassandra-config",
    "-c",
    default="config/cassandra_config.yaml",
    help="Path to Cassandra config file",
)
@click.option("--iterations", "-n", default=100, help="Number of iterations (default: 100)")
def run_query(query_type, cassandra_config, iterations):
    """Execute a specific query type."""
    try:
        from benchmarks.query_definitions import QueryType

        logger.info(f"Running {query_type} queries ({iterations} iterations)...")

        runner = BenchmarkRunner(cassandra_config_path=cassandra_config)
        runner.initialize_components()

        # Get queries of specified type
        qt = QueryType(query_type.lower())
        queries = runner.query_definitions.get_queries_by_type(qt)

        if not queries:
            logger.error(f"No queries found for type: {query_type}")
            sys.exit(1)

        logger.info(f"Found {len(queries)} {query_type} queries")

        # Execute queries
        import random

        for i in range(iterations):
            query = random.choice(queries)
            runner.query_executor.execute_query(query)

            if (i + 1) % 10 == 0:
                logger.info(f"Executed {i + 1}/{iterations} queries")

        # Print metrics
        metrics = runner.query_executor.get_metrics_summary()
        logger.info("✓ Query execution completed")
        logger.info(f"  Success rate: {metrics['success_rate_percent']:.2f}%")
        logger.info(f"  Average latency: {metrics['average_latency_ms']:.2f} ms")

        runner.close()

    except Exception as e:
        logger.error(f"✗ Query execution failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.option(
    "--config", "-c", default="config/cassandra_config.yaml", help="Path to Cassandra config file"
)
@click.option("--force", is_flag=True, help="Force cleanup without confirmation")
def cleanup(config, force):
    """Drop keyspace and clean up all data."""
    if not force:
        click.confirm("This will DELETE ALL data in the TPC-C keyspace. Are you sure?", abort=True)

    try:
        logger.info("Starting cleanup...")
        setup = SchemaSetup(config_path=config)
        setup.connect()
        setup.drop_keyspace()
        setup.close()
        logger.info("✓ Cleanup completed successfully")
    except Exception as e:
        logger.error(f"✗ Cleanup failed: {e}")
        sys.exit(1)


@cli.command()
def info():
    """Display information about the benchmark framework."""
    from benchmarks.query_definitions import QueryDefinitions

    query_defs = QueryDefinitions()

    print("\n" + "=" * 80)
    print("Cassandra TPC-C Benchmark Framework")
    print("=" * 80)
    print("\nQuery Statistics:")
    print(f"  Total queries defined: {len(query_defs.get_all_queries())}")

    type_counts = query_defs.get_query_count_by_type()
    print("\n  By Type:")
    for query_type, count in type_counts.items():
        print(f"    {query_type.upper():10s}: {count}")

    complexity_counts = query_defs.get_query_count_by_complexity()
    print("\n  By Complexity:")
    for complexity, count in complexity_counts.items():
        print(f"    {complexity.capitalize():10s}: {count}")

    print("\nAvailable Commands:")
    print("  setup-schema    - Create keyspace and tables")
    print("  generate-data   - Generate and load test data")
    print("  run-benchmark   - Execute full benchmark suite")
    print("  run-query       - Execute specific query type")
    print("  cleanup         - Drop keyspace and clean up")
    print("  info            - Display this information")

    print("\nConfiguration Files:")
    print("  config/cassandra_config.yaml  - Cassandra connection settings")
    print("  config/benchmark_config.yaml  - Benchmark parameters")

    print("\nFor more information, use: python main.py --help")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    cli()

#!/usr/bin/env python3
"""
Main entry point for Cassandra TPC-E Benchmark Framework.
Provides CLI interface for all benchmark operations.
"""

import logging
import sys
from pathlib import Path

import click

sys.path.insert(0, str(Path(__file__).parent))

import yaml  # noqa: E402
from cassandra.auth import PlainTextAuthProvider  # noqa: E402
from cassandra.cluster import Cluster  # noqa: E402
from data_generator.data_loader import DataLoader  # noqa: E402
from data_generator.tpce_data_generator import TPCEDataGenerator  # noqa: E402
from schema.schema_setup import SchemaSetup  # noqa: E402
from test_harness.benchmark_runner import BenchmarkRunner  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("benchmark.log")],
)

logger = logging.getLogger(__name__)


def load_cassandra_config(config_path: str = "config/cassandra_config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def load_benchmark_config(config_path: str = "config/benchmark_config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def cli(verbose):
    """Cassandra TPC-E Benchmark Framework.

    A comprehensive benchmarking tool for Cassandra using TPC-E data model
    (brokerage firm OLTP workload).
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
    """Create keyspace and tables for TPC-E benchmark."""
    try:
        logger.info("Setting up Cassandra TPC-E schema...")
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
    """Generate and load TPC-E data into Cassandra."""
    try:
        logger.info("Starting TPC-E data generation and loading...")

        cass_config = load_cassandra_config(cassandra_config)
        bench_config = load_benchmark_config(benchmark_config)

        data_config = bench_config["data_generation"]

        if sample_only:
            logger.info("Sample mode: generating minimal data for testing")
            num_customers = 100
            num_brokers = 10
            num_securities = 500
            num_companies = 100
            num_trades = 1000
        else:
            num_customers = data_config["num_customers"]
            num_brokers = data_config["num_brokers"]
            num_securities = data_config["num_securities"]
            num_companies = data_config["num_companies"]
            num_trades = data_config["num_trades"]

        generator = TPCEDataGenerator(
            num_customers=num_customers,
            num_brokers=num_brokers,
            num_securities=num_securities,
            num_companies=num_companies,
            num_trades=num_trades,
        )

        scale_info = generator.get_scale_info()
        logger.info("Data scale:")
        logger.info(f"  Customers: {scale_info['customers']}")
        logger.info(f"  Brokers: {scale_info['brokers']}")
        logger.info(f"  Securities: {scale_info['securities']}")
        logger.info(f"  Companies: {scale_info['companies']}")
        logger.info(f"  Trades: {scale_info['trades']}")
        logger.info(f"  Estimated total records: {scale_info['estimated_total_records']:,}")

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

        loader = DataLoader(session, generator)
        result = loader.load_all_data()

        logger.info("✓ Data generation and loading completed successfully")
        for k, v in result.items():
            logger.info(f"  {k} loaded: {v}")

        cluster.shutdown()

    except Exception as e:
        logger.error(f"✗ Data generation failed: {e}")
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
@click.option("--dry-run", is_flag=True, help="Validate setup without running the full benchmark")
def run_benchmark(cassandra_config, benchmark_config, dry_run):
    """Run the TPC-E benchmark."""
    try:
        logger.info("Initializing TPC-E benchmark runner...")
        runner = BenchmarkRunner(
            cassandra_config_path=cassandra_config, benchmark_config_path=benchmark_config
        )

        result = runner.run_benchmark(dry_run=dry_run)

        if dry_run:
            logger.info("✓ Dry run completed successfully")
        else:
            logger.info("✓ Benchmark completed successfully")
            if result:
                logger.info(f"Results: {result}")

        runner.close()

    except Exception as e:
        logger.error(f"✗ Benchmark failed: {e}")
        sys.exit(1)


@cli.command()
def info():
    """Display TPC-E benchmark information."""
    from benchmarks.query_definitions import QueryDefinitions

    query_defs = QueryDefinitions()
    type_counts = query_defs.get_query_count_by_type()
    complexity_counts = query_defs.get_query_count_by_complexity()

    click.echo("\n" + "=" * 70)
    click.echo("Cassandra TPC-E Benchmark Framework")
    click.echo("=" * 70)
    click.echo("\nTPC-E models a brokerage firm's OLTP workload.")
    click.echo("\nKey entities: customer, customer_account, broker, security,")
    click.echo("  trade, holding, watch_list, daily_market, company, financial")

    click.echo("\nQuery Catalog:")
    click.echo(f"  Total queries: {sum(type_counts.values())}")
    for qt, cnt in type_counts.items():
        click.echo(f"    {qt.upper()}: {cnt}")

    click.echo("\nComplexity Distribution:")
    for cl, cnt in complexity_counts.items():
        click.echo(f"    {cl.capitalize()}: {cnt}")

    click.echo("\nAdvanced Cassandra Features Demonstrated:")
    features = [
        "Collections (set, list, map)",
        "User Defined Types (UDT)",
        "Counter columns",
        "Static columns",
        "TTL (time-to-live)",
        "Lightweight transactions (LWT / IF conditions)",
        "LOGGED and UNLOGGED batches",
        "Custom timestamps (USING TIMESTAMP)",
        "Secondary indexes",
        "Denormalized access pattern tables",
        "Token-based pagination",
        "Clustering key range queries",
    ]
    for f in features:
        click.echo(f"  • {f}")

    click.echo("\nTPC-E Tables: 29 core + 5 denormalized + 8 advanced = 42 total")
    click.echo("=" * 70 + "\n")


if __name__ == "__main__":
    cli()

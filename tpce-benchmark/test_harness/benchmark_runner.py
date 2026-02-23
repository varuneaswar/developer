"""
Benchmark runner for TPC-E Cassandra benchmark.
Orchestrates the entire benchmark execution.
"""

import datetime
import logging
import random
import time
from typing import Any, Dict, List, Optional

import yaml
from benchmarks.query_definitions import ComplexityLevel, QueryDefinitions, QueryType
from benchmarks.query_executor import QueryExecutor
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session
from test_harness.concurrency_manager import ConcurrencyManager, LoadPattern
from test_harness.metrics_collector import MetricsCollector

logger = logging.getLogger(__name__)


class BenchmarkRunner:
    """Main benchmark runner orchestrating all components."""

    def __init__(
        self,
        cassandra_config_path: str = "config/cassandra_config.yaml",
        benchmark_config_path: str = "config/benchmark_config.yaml",
    ):
        """
        Initialize benchmark runner.

        Args:
            cassandra_config_path: Path to Cassandra configuration
            benchmark_config_path: Path to benchmark configuration
        """
        self.cassandra_config = self._load_config(cassandra_config_path)
        self.benchmark_config = self._load_config(benchmark_config_path)

        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None
        self.query_definitions = QueryDefinitions()
        self.query_executor: Optional[QueryExecutor] = None
        self.concurrency_manager: Optional[ConcurrencyManager] = None
        self.metrics_collector: Optional[MetricsCollector] = None

        self.is_connected = False
        self._snapshot_keyspace_name: Optional[str] = None

    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        with open(config_path, "r") as f:
            return yaml.safe_load(f)

    def connect(self) -> None:
        """Establish connection to Cassandra cluster."""
        cassandra_config = self.cassandra_config["cassandra"]

        auth_provider = None
        if cassandra_config.get("username"):
            auth_provider = PlainTextAuthProvider(
                username=cassandra_config["username"], password=cassandra_config.get("password", "")
            )

        self.cluster = Cluster(
            contact_points=cassandra_config["contact_points"],
            port=cassandra_config["port"],
            auth_provider=auth_provider,
            protocol_version=cassandra_config.get("protocol_version", 4),
        )

        self.session = self.cluster.connect()
        self.session.set_keyspace(cassandra_config["keyspace"])

        logger.info(f"Connected to Cassandra at {cassandra_config['contact_points']}")
        logger.info(f"Using keyspace: {cassandra_config['keyspace']}")

        self.is_connected = True

    def initialize_components(self) -> None:
        """Initialize all benchmark components."""
        if not self.is_connected:
            self.connect()

        self.query_executor = QueryExecutor(self.session)

        concurrency = self.benchmark_config["benchmark"]["concurrency"]
        self.concurrency_manager = ConcurrencyManager(concurrency=concurrency)

        output_dir = self.benchmark_config["metrics"]["output_dir"]
        self.metrics_collector = MetricsCollector(output_dir=output_dir)

        logger.info("All benchmark components initialized")

    def _setup_benchmark_snapshot(self) -> str:
        """
        Create an isolated snapshot keyspace for this benchmark run.

        The base keyspace is left untouched.  All INSERT/UPDATE/DELETE
        operations during the benchmark will affect only the snapshot
        keyspace, so SELECT benchmarks see a stable dataset and no base
        data is lost.

        Returns:
            Name of the newly created snapshot keyspace
        """
        from schema.schema_setup import SchemaSetup

        base_keyspace = self.cassandra_config["cassandra"]["keyspace"]
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        snapshot_keyspace = f"{base_keyspace}_run_{timestamp}"
        schema_file = self.benchmark_config["benchmark"].get(
            "schema_file", "schema/tpce_schema.cql"
        )

        logger.info(f"Creating benchmark snapshot keyspace: '{snapshot_keyspace}'")
        setup = SchemaSetup.from_session(self.session, self.cassandra_config)
        setup.snapshot_keyspace(
            source_keyspace=base_keyspace,
            target_keyspace=snapshot_keyspace,
            schema_file=schema_file,
        )
        return snapshot_keyspace

    def _teardown_benchmark_snapshot(self, snapshot_keyspace: str) -> None:
        """
        Drop the snapshot keyspace created for this benchmark run.

        Args:
            snapshot_keyspace: Name of the snapshot keyspace to drop
        """
        from schema.schema_setup import SchemaSetup

        logger.info(f"Cleaning up benchmark snapshot keyspace: '{snapshot_keyspace}'")
        setup = SchemaSetup.from_session(self.session, self.cassandra_config)
        setup.drop_snapshot_keyspace(snapshot_keyspace)

    def select_queries_by_distribution(self) -> List[Any]:
        """
        Select queries based on configured distribution.

        Returns:
            List of query definitions
        """
        query_dist = self.benchmark_config["benchmark"]["query_distribution"]
        complexity_dist = self.benchmark_config["benchmark"]["complexity_distribution"]

        total_query_pct = sum(query_dist.values())
        total_complexity_pct = sum(complexity_dist.values())

        selected_queries = []

        for query_type_str, type_pct in query_dist.items():
            query_type = QueryType(query_type_str)
            type_queries = self.query_definitions.get_queries_by_type(query_type)

            for complexity_str, complexity_pct in complexity_dist.items():
                complexity = ComplexityLevel(complexity_str)
                matching_queries = [q for q in type_queries if q.complexity == complexity]

                if matching_queries:
                    weight = (type_pct / total_query_pct) * (complexity_pct / total_complexity_pct)
                    num_queries = max(1, int(weight * 20))

                    for _ in range(num_queries):
                        selected_queries.append(random.choice(matching_queries))

        logger.info(f"Selected {len(selected_queries)} queries based on distribution")
        return selected_queries

    def run_warmup(self) -> None:
        """Run warmup phase."""
        warmup_duration = self.benchmark_config["benchmark"]["warmup_duration"]

        if warmup_duration <= 0:
            logger.info("Skipping warmup phase")
            return

        logger.info(f"Starting warmup phase ({warmup_duration} seconds)...")

        all_queries = self.query_definitions.get_all_queries()
        warmup_queries = random.sample(all_queries, min(10, len(all_queries)))

        start_time = time.time()
        while (time.time() - start_time) < warmup_duration:
            query = random.choice(warmup_queries)
            try:
                self.query_executor.execute_query(query)
            except Exception as e:
                logger.debug(f"Warmup query error: {e}")

        self.query_executor.reset_metrics()
        self.metrics_collector = MetricsCollector(
            output_dir=self.benchmark_config["metrics"]["output_dir"]
        )

        logger.info("Warmup phase completed")

    def run_benchmark(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Run the complete benchmark suite.

        When ``snapshot_before_benchmark`` is enabled in the benchmark config,
        the runner first copies the base keyspace into an isolated snapshot
        keyspace and then directs all queries there.  This ensures:

        * INSERT/UPDATE/DELETE operations during the run do not alter the base
          tables, so SELECT results measure a stable, reproducible dataset.
        * The base data is preserved after the benchmark completes, eliminating
          the risk of accidental data loss from DELETE operations.

        After the run the snapshot keyspace is dropped automatically when
        ``cleanup_benchmark_keyspace`` is also enabled.

        Args:
            dry_run: If True, validate setup without executing benchmark

        Returns:
            Dict with benchmark results
        """
        if dry_run:
            logger.info("Dry run mode - validating configuration...")
            self.connect()
            logger.info("✓ Connection successful")
            logger.info(f"✓ Available queries: {len(self.query_definitions.get_all_queries())}")
            logger.info("Dry run completed successfully")
            return {"status": "dry_run_success"}

        logger.info("Starting benchmark execution...")

        # Connect first so we can create the snapshot before initialising
        # query handlers (prepared statements must target the right keyspace).
        if not self.is_connected:
            self.connect()

        bench_cfg = self.benchmark_config["benchmark"]
        snapshot_keyspace = None
        if bench_cfg.get("snapshot_before_benchmark", False):
            snapshot_keyspace = self._setup_benchmark_snapshot()
            self._snapshot_keyspace_name = snapshot_keyspace
            # Point the session at the snapshot so all subsequent prepared
            # statements and queries run against the isolated copy.
            self.session.set_keyspace(snapshot_keyspace)
            logger.info(f"Benchmark will run against snapshot keyspace: '{snapshot_keyspace}'")

        # Initialize query handlers *after* the keyspace switch so that
        # prepared statements are bound to the correct (snapshot) keyspace.
        self.initialize_components()
        self.run_warmup()

        selected_queries = self.select_queries_by_distribution()

        duration = bench_cfg["duration_seconds"]
        collection_interval = self.benchmark_config["metrics"]["collection_interval"]
        load_pattern_str = bench_cfg["load_pattern"]
        load_pattern = LoadPattern(load_pattern_str)

        logger.info(
            f"Running benchmark for {duration} seconds " f"with {load_pattern.value} load pattern"
        )

        def execute_query_task(query_def):
            result = self.query_executor.execute_query(query_def)
            self.metrics_collector.record_query_execution(result)
            return result

        start_time = time.time()
        last_collection = start_time
        results = []

        try:
            while (time.time() - start_time) < duration:
                batch_duration = min(collection_interval, duration - (time.time() - start_time))

                if batch_duration > 0:
                    batch_results = self.concurrency_manager.execute_concurrent(
                        task=execute_query_task,
                        task_args=selected_queries,
                        duration_seconds=int(batch_duration),
                        load_pattern=load_pattern,
                    )
                    results.extend(batch_results)

                if (time.time() - last_collection) >= collection_interval:
                    interval_metrics = self.metrics_collector.collect_interval_metrics()
                    logger.info(
                        f"Interval metrics - QPS: {interval_metrics['qps']:.2f}, "
                        f"p95: {interval_metrics['latency_p95_ms']:.2f}ms, "
                        f"p99: {interval_metrics['latency_p99_ms']:.2f}ms"
                    )
                    last_collection = time.time()

        except KeyboardInterrupt:
            logger.info("Benchmark interrupted by user")

        cooldown_duration = bench_cfg["cooldown_duration"]
        if cooldown_duration > 0:
            logger.info(f"Cooldown period: {cooldown_duration} seconds...")
            time.sleep(cooldown_duration)

        logger.info("Exporting metrics...")
        export_formats = self.benchmark_config["metrics"]["export_format"]

        if "json" in export_formats:
            self.metrics_collector.export_to_json()
        if "csv" in export_formats:
            self.metrics_collector.export_to_csv()

        self.metrics_collector.print_summary()
        logger.info("Benchmark execution completed")

        # Drop the snapshot keyspace to reclaim disk space
        if snapshot_keyspace and bench_cfg.get("cleanup_benchmark_keyspace", True):
            self._teardown_benchmark_snapshot(snapshot_keyspace)
            self._snapshot_keyspace_name = None

        return self.metrics_collector.get_aggregate_statistics()

    def close(self) -> None:
        """Close Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")

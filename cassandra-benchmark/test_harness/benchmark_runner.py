"""
Benchmark runner for TPC-C Cassandra benchmark.
Orchestrates the entire benchmark execution.
"""

import time
import logging
import random
import yaml
from typing import Dict, Any, List, Optional
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider

from benchmarks.query_definitions import QueryDefinitions, QueryType, ComplexityLevel
from benchmarks.query_executor import QueryExecutor
from test_harness.concurrency_manager import ConcurrencyManager, LoadPattern
from test_harness.metrics_collector import MetricsCollector

logger = logging.getLogger(__name__)


class BenchmarkRunner:
    """Main benchmark runner orchestrating all components."""
    
    def __init__(self, cassandra_config_path: str = "config/cassandra_config.yaml",
                 benchmark_config_path: str = "config/benchmark_config.yaml"):
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
    
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def connect(self) -> None:
        """Establish connection to Cassandra cluster."""
        cassandra_config = self.cassandra_config['cassandra']
        
        # Setup authentication
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
        self.session.set_keyspace(cassandra_config['keyspace'])
        
        logger.info(f"Connected to Cassandra at {cassandra_config['contact_points']}")
        logger.info(f"Using keyspace: {cassandra_config['keyspace']}")
        
        self.is_connected = True
    
    def initialize_components(self) -> None:
        """Initialize all benchmark components."""
        if not self.is_connected:
            self.connect()
        
        # Initialize components
        self.query_executor = QueryExecutor(self.session)
        
        concurrency = self.benchmark_config['benchmark']['concurrency']
        self.concurrency_manager = ConcurrencyManager(concurrency=concurrency)
        
        output_dir = self.benchmark_config['metrics']['output_dir']
        self.metrics_collector = MetricsCollector(output_dir=output_dir)
        
        logger.info("All benchmark components initialized")
    
    def select_queries_by_distribution(self) -> List[Any]:
        """
        Select queries based on configured distribution.
        
        Returns:
            List of query definitions
        """
        query_dist = self.benchmark_config['benchmark']['query_distribution']
        complexity_dist = self.benchmark_config['benchmark']['complexity_distribution']
        
        # Calculate total percentage
        total_query_pct = sum(query_dist.values())
        total_complexity_pct = sum(complexity_dist.values())
        
        selected_queries = []
        
        # For each query type
        for query_type_str, type_pct in query_dist.items():
            query_type = QueryType(query_type_str)
            type_queries = self.query_definitions.get_queries_by_type(query_type)
            
            # Distribute by complexity
            for complexity_str, complexity_pct in complexity_dist.items():
                complexity = ComplexityLevel(complexity_str)
                
                # Filter queries by both type and complexity
                matching_queries = [
                    q for q in type_queries 
                    if q.complexity == complexity
                ]
                
                if matching_queries:
                    # Calculate how many queries of this type/complexity
                    weight = (type_pct / total_query_pct) * (complexity_pct / total_complexity_pct)
                    num_queries = max(1, int(weight * 20))  # Scale to ~20 queries
                    
                    # Randomly select queries (with replacement for weight)
                    for _ in range(num_queries):
                        selected_queries.append(random.choice(matching_queries))
        
        logger.info(f"Selected {len(selected_queries)} queries based on distribution")
        return selected_queries
    
    def run_warmup(self) -> None:
        """Run warmup phase."""
        warmup_duration = self.benchmark_config['benchmark']['warmup_duration']
        
        if warmup_duration <= 0:
            logger.info("Skipping warmup phase")
            return
        
        logger.info(f"Starting warmup phase ({warmup_duration} seconds)...")
        
        # Select a subset of queries for warmup
        all_queries = self.query_definitions.get_all_queries()
        warmup_queries = random.sample(all_queries, min(10, len(all_queries)))
        
        start_time = time.time()
        while (time.time() - start_time) < warmup_duration:
            query = random.choice(warmup_queries)
            try:
                self.query_executor.execute_query(query)
            except Exception as e:
                logger.debug(f"Warmup query error: {e}")
        
        # Reset metrics after warmup
        self.query_executor.reset_metrics()
        self.metrics_collector = MetricsCollector(
            output_dir=self.benchmark_config['metrics']['output_dir']
        )
        
        logger.info("Warmup phase completed")
    
    def run_benchmark(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Run the complete benchmark suite.
        
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
            return {'status': 'dry_run_success'}
        
        logger.info("Starting benchmark execution...")
        
        # Initialize
        self.initialize_components()
        
        # Warmup
        self.run_warmup()
        
        # Select queries
        selected_queries = self.select_queries_by_distribution()
        
        # Get benchmark parameters
        duration = self.benchmark_config['benchmark']['duration_seconds']
        collection_interval = self.benchmark_config['metrics']['collection_interval']
        load_pattern_str = self.benchmark_config['benchmark']['load_pattern']
        load_pattern = LoadPattern(load_pattern_str)
        
        logger.info(f"Running benchmark for {duration} seconds with {load_pattern.value} load pattern")
        
        # Create task function
        def execute_query_task(query_def):
            result = self.query_executor.execute_query(query_def)
            self.metrics_collector.record_query_execution(result)
            return result
        
        # Run benchmark with periodic metrics collection
        start_time = time.time()
        last_collection = start_time
        results = []
        
        try:
            while (time.time() - start_time) < duration:
                # Execute a batch of queries
                batch_duration = min(collection_interval, duration - (time.time() - start_time))
                
                if batch_duration > 0:
                    batch_results = self.concurrency_manager.execute_concurrent(
                        task=execute_query_task,
                        task_args=selected_queries,
                        duration_seconds=int(batch_duration),
                        load_pattern=load_pattern
                    )
                    results.extend(batch_results)
                
                # Collect interval metrics
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
        
        # Cooldown
        cooldown_duration = self.benchmark_config['benchmark']['cooldown_duration']
        if cooldown_duration > 0:
            logger.info(f"Cooldown period: {cooldown_duration} seconds...")
            time.sleep(cooldown_duration)
        
        # Export metrics
        logger.info("Exporting metrics...")
        export_formats = self.benchmark_config['metrics']['export_format']
        
        if 'json' in export_formats:
            self.metrics_collector.export_to_json()
        if 'csv' in export_formats:
            self.metrics_collector.export_to_csv()
        
        # Print summary
        self.metrics_collector.print_summary()
        
        logger.info("Benchmark execution completed")
        
        return self.metrics_collector.get_aggregate_statistics()
    
    def close(self) -> None:
        """Close Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")

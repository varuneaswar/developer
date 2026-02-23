"""
Metrics collector for TPC-E benchmark.
Collects and aggregates performance metrics.
"""

import csv
import json
import logging
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import numpy as np

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collects and aggregates benchmark metrics."""

    def __init__(self, output_dir: str = "./results"):
        """
        Initialize metrics collector.

        Args:
            output_dir: Directory for output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Time-series data
        self.time_series_data: List[Dict[str, Any]] = []

        # Per-query metrics
        self.query_metrics: Dict[str, List[float]] = defaultdict(list)

        # Per-type metrics
        self.type_metrics: Dict[str, List[float]] = defaultdict(list)

        # Per-complexity metrics
        self.complexity_metrics: Dict[str, List[float]] = defaultdict(list)

        # Error tracking
        self.errors: List[Dict[str, Any]] = []

        # Start time
        self.start_time = time.time()
        self.last_collection_time = self.start_time

    def record_query_execution(self, result: Dict[str, Any]) -> None:
        """
        Record a single query execution result.

        Args:
            result: Query execution result dictionary
        """
        # Record latency by query ID
        if result["success"]:
            self.query_metrics[result["query_id"]].append(result["latency_ms"])
            self.type_metrics[result["query_type"]].append(result["latency_ms"])
            self.complexity_metrics[result["complexity"]].append(result["latency_ms"])
        else:
            # Record error
            self.errors.append(
                {
                    "query_id": result["query_id"],
                    "query_name": result["query_name"],
                    "timestamp": result["timestamp"],
                    "error": result["error"],
                }
            )

    def collect_interval_metrics(self) -> Dict[str, Any]:
        """
        Collect metrics for the current interval.

        Returns:
            Dict with interval metrics
        """
        current_time = time.time()
        interval = current_time - self.last_collection_time
        self.last_collection_time = current_time

        # Calculate total queries in interval
        total_queries = sum(len(latencies) for latencies in self.query_metrics.values())
        qps = total_queries / interval if interval > 0 else 0

        # Calculate overall latency percentiles
        all_latencies = []
        for latencies in self.query_metrics.values():
            all_latencies.extend(latencies)

        if all_latencies:
            p50 = np.percentile(all_latencies, 50)
            p95 = np.percentile(all_latencies, 95)
            p99 = np.percentile(all_latencies, 99)
            p999 = np.percentile(all_latencies, 99.9)
            max_latency = max(all_latencies)
            avg_latency = np.mean(all_latencies)
        else:
            p50 = p95 = p99 = p999 = max_latency = avg_latency = 0

        interval_metrics = {
            "timestamp": datetime.now().isoformat(),
            "elapsed_seconds": current_time - self.start_time,
            "interval_seconds": interval,
            "total_queries": total_queries,
            "qps": round(qps, 2),
            "latency_p50_ms": round(p50, 2),
            "latency_p95_ms": round(p95, 2),
            "latency_p99_ms": round(p99, 2),
            "latency_p999_ms": round(p999, 2),
            "latency_max_ms": round(max_latency, 2),
            "latency_avg_ms": round(avg_latency, 2),
            "error_count": len(self.errors),
        }

        self.time_series_data.append(interval_metrics)

        return interval_metrics

    def calculate_percentile(self, latencies: List[float], percentile: float) -> float:
        """
        Calculate percentile from latency list.

        Args:
            latencies: List of latency values
            percentile: Percentile to calculate (0-100)

        Returns:
            Percentile value
        """
        if not latencies:
            return 0.0
        return np.percentile(latencies, percentile)

    def get_query_statistics(self, query_id: str) -> Dict[str, Any]:
        """
        Get statistics for a specific query.

        Args:
            query_id: Query identifier

        Returns:
            Dict with query statistics
        """
        latencies = self.query_metrics.get(query_id, [])

        if not latencies:
            return {"query_id": query_id, "count": 0, "error": "No data collected"}

        return {
            "query_id": query_id,
            "count": len(latencies),
            "avg_latency_ms": round(np.mean(latencies), 2),
            "p50_latency_ms": round(self.calculate_percentile(latencies, 50), 2),
            "p95_latency_ms": round(self.calculate_percentile(latencies, 95), 2),
            "p99_latency_ms": round(self.calculate_percentile(latencies, 99), 2),
            "p999_latency_ms": round(self.calculate_percentile(latencies, 99.9), 2),
            "max_latency_ms": round(max(latencies), 2),
            "min_latency_ms": round(min(latencies), 2),
        }

    def get_aggregate_statistics(self) -> Dict[str, Any]:
        """
        Get aggregate statistics across all queries.

        Returns:
            Dict with aggregate statistics
        """
        all_latencies = []
        for latencies in self.query_metrics.values():
            all_latencies.extend(latencies)

        total_queries = len(all_latencies)
        elapsed = time.time() - self.start_time

        if not all_latencies:
            return {"total_queries": 0, "elapsed_seconds": elapsed, "error": "No data collected"}

        # Per-type statistics
        type_stats = {}
        for query_type, latencies in self.type_metrics.items():
            if latencies:
                type_stats[query_type] = {
                    "count": len(latencies),
                    "avg_latency_ms": round(np.mean(latencies), 2),
                    "p95_latency_ms": round(self.calculate_percentile(latencies, 95), 2),
                    "p99_latency_ms": round(self.calculate_percentile(latencies, 99), 2),
                }

        # Per-complexity statistics
        complexity_stats = {}
        for complexity, latencies in self.complexity_metrics.items():
            if latencies:
                complexity_stats[complexity] = {
                    "count": len(latencies),
                    "avg_latency_ms": round(np.mean(latencies), 2),
                    "p95_latency_ms": round(self.calculate_percentile(latencies, 95), 2),
                    "p99_latency_ms": round(self.calculate_percentile(latencies, 99), 2),
                }

        return {
            "total_queries": total_queries,
            "elapsed_seconds": round(elapsed, 2),
            "queries_per_second": round(total_queries / elapsed, 2),
            "avg_latency_ms": round(np.mean(all_latencies), 2),
            "p50_latency_ms": round(self.calculate_percentile(all_latencies, 50), 2),
            "p95_latency_ms": round(self.calculate_percentile(all_latencies, 95), 2),
            "p99_latency_ms": round(self.calculate_percentile(all_latencies, 99), 2),
            "p999_latency_ms": round(self.calculate_percentile(all_latencies, 99.9), 2),
            "max_latency_ms": round(max(all_latencies), 2),
            "min_latency_ms": round(min(all_latencies), 2),
            "total_errors": len(self.errors),
            "error_rate_percent": round(
                (len(self.errors) / (total_queries + len(self.errors)) * 100), 2
            ),
            "by_type": type_stats,
            "by_complexity": complexity_stats,
        }

    def export_to_json(self, filename: str = None) -> str:
        """
        Export metrics to JSON file.

        Args:
            filename: Output filename (auto-generated if None)

        Returns:
            Path to output file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"metrics_{timestamp}.json"

        filepath = self.output_dir / filename

        output = {
            "summary": self.get_aggregate_statistics(),
            "time_series": self.time_series_data,
            "per_query": {
                query_id: self.get_query_statistics(query_id)
                for query_id in self.query_metrics.keys()
            },
            "errors": self.errors,
        }

        with open(filepath, "w") as f:
            json.dump(output, f, indent=2)

        logger.info(f"Metrics exported to JSON: {filepath}")
        return str(filepath)

    def export_to_csv(self, filename: str = None) -> str:
        """
        Export time-series metrics to CSV file.

        Args:
            filename: Output filename (auto-generated if None)

        Returns:
            Path to output file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"metrics_{timestamp}.csv"

        filepath = self.output_dir / filename

        if not self.time_series_data:
            logger.warning("No time-series data to export")
            return str(filepath)

        # Write CSV
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.time_series_data[0].keys())
            writer.writeheader()
            writer.writerows(self.time_series_data)

        logger.info(f"Metrics exported to CSV: {filepath}")
        return str(filepath)

    def print_summary(self) -> None:
        """Print summary of metrics to console."""
        stats = self.get_aggregate_statistics()

        print("\n" + "=" * 80)
        print("BENCHMARK SUMMARY")
        print("=" * 80)
        print(f"Total Queries:        {stats.get('total_queries', 0):,}")
        print(f"Elapsed Time:         {stats.get('elapsed_seconds', 0):.2f} seconds")
        print(f"Queries per Second:   {stats.get('queries_per_second', 0):.2f}")
        print("\nLatency Metrics:")
        print(f"  Average:            {stats.get('avg_latency_ms', 0):.2f} ms")
        print(f"  p50:                {stats.get('p50_latency_ms', 0):.2f} ms")
        print(f"  p95:                {stats.get('p95_latency_ms', 0):.2f} ms")
        print(f"  p99:                {stats.get('p99_latency_ms', 0):.2f} ms")
        print(f"  p999:               {stats.get('p999_latency_ms', 0):.2f} ms")
        print(f"  Max:                {stats.get('max_latency_ms', 0):.2f} ms")
        print("\nError Statistics:")
        print(f"  Total Errors:       {stats.get('total_errors', 0)}")
        print(f"  Error Rate:         {stats.get('error_rate_percent', 0):.2f}%")
        print("=" * 80 + "\n")

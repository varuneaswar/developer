"""
Query executor for TPC-E benchmark.
Handles query execution with metrics collection.
"""

import time
import logging
from typing import Dict, Any, Optional, List
from cassandra.cluster import Session
from datetime import datetime

from queries.select_queries import SelectQueries
from queries.insert_queries import InsertQueries
from queries.update_queries import UpdateQueries
from queries.delete_queries import DeleteQueries
from benchmarks.query_definitions import QueryDefinition, QueryType

logger = logging.getLogger(__name__)


class QueryExecutor:
    """Executes benchmark queries and collects metrics."""

    def __init__(self, session: Session):
        """
        Initialize query executor with Cassandra session.

        Args:
            session: Active Cassandra session
        """
        self.session = session
        self.select_queries = SelectQueries(session)
        self.insert_queries = InsertQueries(session)
        self.update_queries = UpdateQueries(session)
        self.delete_queries = DeleteQueries(session)

        self.execution_count = 0
        self.success_count = 0
        self.error_count = 0
        self.total_latency = 0.0

    def execute_query(self, query_def: QueryDefinition,
                      params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a single query and collect metrics.

        Args:
            query_def: Query definition
            params: Query parameters (if None, use default from definition)

        Returns:
            Dict containing execution results and metrics
        """
        if params is None:
            params = query_def.params_generator()

        start_time = time.time()
        success = False
        error_message = None
        result = None

        try:
            handler = self._get_query_handler(query_def.query_type)
            method = getattr(handler, query_def.method_name)
            result = method(**params)
            success = True
        except AttributeError as e:
            error_message = f"Method {query_def.method_name} not found: {e}"
            logger.error(error_message)
        except Exception as e:
            error_message = f"Query execution error: {e}"
            logger.error(error_message)

        end_time = time.time()
        latency = (end_time - start_time) * 1000  # milliseconds

        self.execution_count += 1
        if success:
            self.success_count += 1
        else:
            self.error_count += 1
        self.total_latency += latency

        return {
            'query_id': query_def.query_id,
            'query_name': query_def.name,
            'query_type': query_def.query_type.value,
            'complexity': query_def.complexity.value,
            'success': success,
            'latency_ms': latency,
            'timestamp': datetime.now().isoformat(),
            'error': error_message,
            'result': result
        }

    def _get_query_handler(self, query_type: QueryType):
        """Get the appropriate query handler based on query type."""
        handlers = {
            QueryType.SELECT: self.select_queries,
            QueryType.INSERT: self.insert_queries,
            QueryType.UPDATE: self.update_queries,
            QueryType.DELETE: self.delete_queries
        }
        return handlers[query_type]

    def execute_queries_batch(self, query_defs: List[QueryDefinition],
                              iterations: int = 1) -> List[Dict[str, Any]]:
        """
        Execute a batch of queries multiple times.

        Args:
            query_defs: List of query definitions
            iterations: Number of iterations for each query

        Returns:
            List of execution results
        """
        results = []
        for _ in range(iterations):
            for query_def in query_defs:
                result = self.execute_query(query_def)
                results.append(result)
                if (len(results) % 100) == 0:
                    logger.info(f"Executed {len(results)} queries")
        return results

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of execution metrics."""
        avg_latency = (self.total_latency / self.execution_count
                       if self.execution_count > 0 else 0)
        success_rate = ((self.success_count / self.execution_count * 100)
                        if self.execution_count > 0 else 0)

        return {
            'total_executions': self.execution_count,
            'successful_executions': self.success_count,
            'failed_executions': self.error_count,
            'success_rate_percent': round(success_rate, 2),
            'average_latency_ms': round(avg_latency, 2),
            'total_latency_ms': round(self.total_latency, 2)
        }

    def reset_metrics(self) -> None:
        """Reset all metrics counters."""
        self.execution_count = 0
        self.success_count = 0
        self.error_count = 0
        self.total_latency = 0.0
        logger.info("Metrics reset")

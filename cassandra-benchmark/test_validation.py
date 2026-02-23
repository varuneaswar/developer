"""
Validation tests for Cassandra TPC-C Benchmark Framework.
Tests all components without requiring a running Cassandra instance.
"""

import sys
import unittest
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from benchmarks.query_definitions import (  # noqa: E402
    ComplexityLevel,
    QueryDefinition,
    QueryDefinitions,
    QueryType,
)
from data_generator.tpcc_data_generator import TPCCDataGenerator  # noqa: E402


class TestQueryDefinitions(unittest.TestCase):
    """Test query definitions and registry."""

    def setUp(self):
        """Set up test fixtures."""
        self.query_defs = QueryDefinitions()

    def test_total_queries(self):
        """Test that we have 80 queries defined (20 for each type)."""
        queries = self.query_defs.get_all_queries()
        self.assertEqual(len(queries), 80, f"Expected 80 queries (20 per type), got {len(queries)}")

    def test_query_types(self):
        """Test that all query types have exactly 20 queries."""
        for query_type in QueryType:
            queries = self.query_defs.get_queries_by_type(query_type)
            self.assertEqual(
                len(queries),
                20,
                f"Expected 20 queries for type {query_type.value}, got {len(queries)}",
            )

    def test_complexity_levels(self):
        """Test that all complexity levels are represented."""
        for complexity in ComplexityLevel:
            queries = self.query_defs.get_queries_by_complexity(complexity)
            self.assertGreater(
                len(queries), 0, f"No queries found for complexity {complexity.value}"
            )

    def test_query_counts_by_type(self):
        """Test query count breakdown by type."""
        counts = self.query_defs.get_query_count_by_type()

        # Check that we have queries for all types
        self.assertIn("select", counts)
        self.assertIn("insert", counts)
        self.assertIn("update", counts)
        self.assertIn("delete", counts)

        # Check that each type has exactly 20 queries
        for query_type, count in counts.items():
            self.assertEqual(count, 20, f"Expected 20 {query_type} queries, got {count}")

    def test_query_counts_by_complexity(self):
        """Test query count breakdown by complexity."""
        counts = self.query_defs.get_query_count_by_complexity()

        # Check that we have queries for all complexity levels
        self.assertIn("simple", counts)
        self.assertIn("medium", counts)
        self.assertIn("complex", counts)

        # Check minimum counts
        self.assertGreaterEqual(counts["simple"], 4)
        self.assertGreaterEqual(counts["medium"], 4)
        self.assertGreaterEqual(counts["complex"], 2)

    def test_query_structure(self):
        """Test that queries have required attributes."""
        queries = self.query_defs.get_all_queries()

        for query in queries:
            self.assertIsInstance(query, QueryDefinition)
            self.assertTrue(query.query_id)
            self.assertTrue(query.name)
            self.assertIsInstance(query.query_type, QueryType)
            self.assertIsInstance(query.complexity, ComplexityLevel)
            self.assertTrue(query.description)
            self.assertTrue(query.method_name)
            self.assertIsNotNone(query.params_generator)


class TestTPCCDataGenerator(unittest.TestCase):
    """Test TPC-C data generator."""

    def setUp(self):
        """Set up test fixtures."""
        self.generator = TPCCDataGenerator(
            num_warehouses=2,
            num_districts_per_warehouse=2,
            num_customers_per_district=100,
            num_items=1000,
        )

    def test_scale_info(self):
        """Test scale information calculation."""
        scale_info = self.generator.get_scale_info()

        self.assertEqual(scale_info["warehouses"], 2)
        self.assertEqual(scale_info["districts"], 4)
        self.assertEqual(scale_info["customers"], 400)
        self.assertEqual(scale_info["items"], 1000)

    def test_generate_warehouse(self):
        """Test warehouse data generation."""
        warehouse = self.generator.generate_warehouse(1)

        # Check required fields
        self.assertEqual(warehouse["w_id"], 1)
        self.assertTrue(warehouse["w_name"])
        self.assertTrue(warehouse["w_street_1"])
        self.assertTrue(warehouse["w_city"])
        self.assertTrue(warehouse["w_state"])
        self.assertTrue(warehouse["w_zip"])
        self.assertIsInstance(warehouse["w_tax"], float)
        self.assertIsInstance(warehouse["w_ytd"], float)

    def test_generate_district(self):
        """Test district data generation."""
        district = self.generator.generate_district(1, 1)

        # Check required fields
        self.assertEqual(district["d_id"], 1)
        self.assertEqual(district["d_w_id"], 1)
        self.assertTrue(district["d_name"])
        self.assertTrue(district["d_city"])
        self.assertIsInstance(district["d_tax"], float)
        self.assertIsInstance(district["d_next_o_id"], int)

    def test_generate_customer(self):
        """Test customer data generation."""
        customer = self.generator.generate_customer(1, 1, 1)

        # Check required fields
        self.assertEqual(customer["c_id"], 1)
        self.assertEqual(customer["c_d_id"], 1)
        self.assertEqual(customer["c_w_id"], 1)
        self.assertTrue(customer["c_first"])
        self.assertTrue(customer["c_last"])
        self.assertIn(customer["c_credit"], ["GC", "BC"])
        self.assertIsInstance(customer["c_balance"], float)

    def test_generate_item(self):
        """Test item data generation."""
        item = self.generator.generate_item(100)

        # Check required fields
        self.assertEqual(item["i_id"], 100)
        self.assertTrue(item["i_name"])
        self.assertIsInstance(item["i_price"], float)
        self.assertGreater(item["i_price"], 0)

    def test_generate_stock(self):
        """Test stock data generation."""
        stock = self.generator.generate_stock(100, 1)

        # Check required fields
        self.assertEqual(stock["s_i_id"], 100)
        self.assertEqual(stock["s_w_id"], 1)
        self.assertIsInstance(stock["s_quantity"], int)
        self.assertTrue(stock["s_dist_01"])

        # Check that all district fields exist
        for i in range(1, 11):
            key = f"s_dist_{i:02d}"
            self.assertIn(key, stock)

    def test_generate_order(self):
        """Test order data generation."""
        order = self.generator.generate_order(1000, 1, 1, 50)

        # Check required fields
        self.assertEqual(order["o_id"], 1000)
        self.assertEqual(order["o_d_id"], 1)
        self.assertEqual(order["o_w_id"], 1)
        self.assertEqual(order["o_c_id"], 50)
        self.assertIsInstance(order["o_ol_cnt"], int)

    def test_generate_order_line(self):
        """Test order line data generation."""
        order_line = self.generator.generate_order_line(1, 1000, 1, 1)

        # Check required fields
        self.assertEqual(order_line["ol_number"], 1)
        self.assertEqual(order_line["ol_o_id"], 1000)
        self.assertEqual(order_line["ol_d_id"], 1)
        self.assertEqual(order_line["ol_w_id"], 1)
        self.assertIsInstance(order_line["ol_quantity"], int)
        self.assertIsInstance(order_line["ol_amount"], float)

    def test_generate_history(self):
        """Test history record generation."""
        history = self.generator.generate_history(50, 1, 1, 1, 1)

        # Check required fields
        self.assertEqual(history["h_c_id"], 50)
        self.assertEqual(history["h_c_d_id"], 1)
        self.assertEqual(history["h_c_w_id"], 1)
        self.assertEqual(history["h_d_id"], 1)
        self.assertEqual(history["h_w_id"], 1)
        self.assertIsInstance(history["h_amount"], float)
        self.assertTrue(history["date_bucket"])


class TestConfiguration(unittest.TestCase):
    """Test configuration files."""

    def test_cassandra_config_exists(self):
        """Test that Cassandra config file exists."""
        config_path = Path("config/cassandra_config.yaml")
        self.assertTrue(config_path.exists(), "Cassandra config file not found")

    def test_benchmark_config_exists(self):
        """Test that benchmark config file exists."""
        config_path = Path("config/benchmark_config.yaml")
        self.assertTrue(config_path.exists(), "Benchmark config file not found")

    def test_schema_file_exists(self):
        """Test that schema file exists."""
        schema_path = Path("schema/tpcc_schema.cql")
        self.assertTrue(schema_path.exists(), "Schema file not found")

    def test_benchmark_config_snapshot_keys(self):
        """Test that benchmark config includes snapshot isolation settings."""
        import yaml

        with open("config/benchmark_config.yaml") as f:
            config = yaml.safe_load(f)
        bench = config["benchmark"]
        self.assertIn(
            "snapshot_before_benchmark",
            bench,
            "Missing 'snapshot_before_benchmark' key in benchmark config",
        )
        self.assertIn(
            "cleanup_benchmark_keyspace",
            bench,
            "Missing 'cleanup_benchmark_keyspace' key in benchmark config",
        )
        self.assertIn("schema_file", bench, "Missing 'schema_file' key in benchmark config")
        self.assertIsInstance(bench["snapshot_before_benchmark"], bool)
        self.assertIsInstance(bench["cleanup_benchmark_keyspace"], bool)
        self.assertTrue(bench["schema_file"], "'schema_file' must not be empty")

    def test_snapshot_schema_file_exists(self):
        """Test that the schema_file referenced in benchmark config exists."""
        import yaml

        with open("config/benchmark_config.yaml") as f:
            config = yaml.safe_load(f)
        schema_file = config["benchmark"]["schema_file"]
        self.assertTrue(
            Path(schema_file).exists(),
            f"schema_file '{schema_file}' referenced in benchmark config not found",
        )


class TestSnapshotIsolation(unittest.TestCase):
    """Test snapshot isolation logic without a live Cassandra instance."""

    def _mock_cassandra_modules(self):
        """Inject lightweight mock modules so Cassandra-dependent code can import."""
        import sys
        from unittest.mock import MagicMock

        for mod in [
            "cassandra",
            "cassandra.cluster",
            "cassandra.auth",
            "cassandra.query",
            "cassandra.concurrent",
            "numpy",
        ]:
            if mod not in sys.modules:
                sys.modules[mod] = MagicMock()

    def test_schema_setup_from_session_classmethod(self):
        """SchemaSetup.from_session should create an instance with the given session/config."""
        self._mock_cassandra_modules()
        import sys

        for key in list(sys.modules.keys()):
            if key.startswith("schema"):
                del sys.modules[key]

        from unittest.mock import MagicMock

        from schema.schema_setup import SchemaSetup

        mock_session = MagicMock()
        config = {"cassandra": {"keyspace": "test_ks"}}
        instance = SchemaSetup.from_session(mock_session, config)

        self.assertIs(instance.session, mock_session)
        self.assertEqual(instance.config, config)
        self.assertIsNone(instance.cluster)

    def test_benchmark_runner_snapshot_attributes(self):
        """BenchmarkRunner.__init__ should set _snapshot_keyspace_name to None."""
        self._mock_cassandra_modules()
        import inspect
        import sys

        for key in list(sys.modules.keys()):
            if (
                key.startswith("test_harness")
                or key.startswith("benchmarks")
                or key.startswith("queries")
            ):
                del sys.modules[key]

        from test_harness.benchmark_runner import BenchmarkRunner

        # Verify that __init__ actually sets the attribute (not just that None == None)
        source = inspect.getsource(BenchmarkRunner.__init__)
        self.assertIn(
            "_snapshot_keyspace_name",
            source,
            "BenchmarkRunner.__init__ must initialise _snapshot_keyspace_name",
        )
        self.assertIn(
            "None", source, "BenchmarkRunner.__init__ must set _snapshot_keyspace_name to None"
        )


class TestProjectStructure(unittest.TestCase):
    """Test project structure and files."""

    def test_required_directories_exist(self):
        """Test that all required directories exist."""
        required_dirs = [
            "config",
            "schema",
            "queries",
            "benchmarks",
            "data_generator",
            "test_harness",
            "results",
        ]

        for dir_name in required_dirs:
            dir_path = Path(dir_name)
            self.assertTrue(dir_path.exists(), f"Required directory '{dir_name}' not found")

    def test_query_modules_exist(self):
        """Test that all query modules exist."""
        query_modules = [
            "queries/select_queries.py",
            "queries/insert_queries.py",
            "queries/update_queries.py",
            "queries/delete_queries.py",
        ]

        for module in query_modules:
            module_path = Path(module)
            self.assertTrue(module_path.exists(), f"Query module '{module}' not found")

    def test_main_entry_point_exists(self):
        """Test that main entry point exists."""
        main_path = Path("main.py")
        self.assertTrue(main_path.exists(), "main.py not found")

    def test_readme_exists(self):
        """Test that README exists."""
        readme_path = Path("README.md")
        self.assertTrue(readme_path.exists(), "README.md not found")

    def test_requirements_exists(self):
        """Test that requirements.txt exists."""
        req_path = Path("requirements.txt")
        self.assertTrue(req_path.exists(), "requirements.txt not found")


def run_validation_tests():
    """Run all validation tests."""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestQueryDefinitions))
    suite.addTests(loader.loadTestsFromTestCase(TestTPCCDataGenerator))
    suite.addTests(loader.loadTestsFromTestCase(TestConfiguration))
    suite.addTests(loader.loadTestsFromTestCase(TestProjectStructure))
    suite.addTests(loader.loadTestsFromTestCase(TestSnapshotIsolation))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Print summary
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print("=" * 80)

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_validation_tests()
    sys.exit(0 if success else 1)

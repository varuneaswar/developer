"""
Validation tests for Cassandra TPC-E Benchmark Framework.
Tests all components without requiring a running Cassandra instance.
"""

import sys
import unittest
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from benchmarks.query_definitions import (
    QueryDefinitions, QueryType, ComplexityLevel, QueryDefinition
)
from data_generator.tpce_data_generator import TPCEDataGenerator


class TestQueryDefinitions(unittest.TestCase):
    """Test query definitions and registry."""

    def setUp(self):
        self.query_defs = QueryDefinitions()

    def test_total_queries(self):
        """Test that we have exactly 80 queries defined (20 per type)."""
        queries = self.query_defs.get_all_queries()
        self.assertEqual(len(queries), 80,
                         f"Expected 80 queries (20 per type), got {len(queries)}")

    def test_query_types(self):
        """Test that all query types have exactly 20 queries."""
        for query_type in QueryType:
            queries = self.query_defs.get_queries_by_type(query_type)
            self.assertEqual(len(queries), 20,
                             f"Expected 20 queries for type {query_type.value}, got {len(queries)}")

    def test_complexity_levels(self):
        """Test that all complexity levels are represented."""
        for complexity in ComplexityLevel:
            queries = self.query_defs.get_queries_by_complexity(complexity)
            self.assertGreater(len(queries), 0,
                               f"No queries found for complexity {complexity.value}")

    def test_query_counts_by_type(self):
        """Test query count breakdown by type."""
        counts = self.query_defs.get_query_count_by_type()

        self.assertIn('select', counts)
        self.assertIn('insert', counts)
        self.assertIn('update', counts)
        self.assertIn('delete', counts)

        for query_type, count in counts.items():
            self.assertEqual(count, 20,
                             f"Expected 20 {query_type} queries, got {count}")

    def test_query_counts_by_complexity(self):
        """Test query count breakdown by complexity."""
        counts = self.query_defs.get_query_count_by_complexity()

        self.assertIn('simple', counts)
        self.assertIn('medium', counts)
        self.assertIn('complex', counts)

        self.assertGreaterEqual(counts['simple'], 4)
        self.assertGreaterEqual(counts['medium'], 4)
        self.assertGreaterEqual(counts['complex'], 2)

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

    def test_params_generator_callable(self):
        """Test that all params_generators are callable and return dicts."""
        queries = self.query_defs.get_all_queries()
        for query in queries:
            params = query.params_generator()
            self.assertIsInstance(params, dict,
                                  f"Query {query.query_id} params_generator should return a dict")


class TestTPCEDataGenerator(unittest.TestCase):
    """Test TPC-E data generator."""

    def setUp(self):
        self.generator = TPCEDataGenerator(
            num_customers=100,
            num_brokers=10,
            num_securities=500,
            num_companies=100,
            num_trades=1000
        )

    def test_scale_info(self):
        """Test scale information calculation."""
        scale_info = self.generator.get_scale_info()

        self.assertEqual(scale_info['customers'], 100)
        self.assertEqual(scale_info['brokers'], 10)
        self.assertEqual(scale_info['securities'], 500)
        self.assertEqual(scale_info['companies'], 100)
        self.assertEqual(scale_info['trades'], 1000)
        self.assertIn('estimated_total_records', scale_info)

    def test_generate_customer(self):
        """Test customer data generation."""
        customer = self.generator.generate_customer(1)

        self.assertEqual(customer['c_id'], 1)
        self.assertTrue(customer['c_l_name'])
        self.assertTrue(customer['c_f_name'])
        self.assertIn(customer['c_gndr'], ['M', 'F', 'U'])
        self.assertIsInstance(customer['c_tier'], int)
        self.assertTrue(customer['c_email_1'])

    def test_generate_account(self):
        """Test customer account data generation."""
        account = self.generator.generate_customer_account(1, 1, 1)

        self.assertEqual(account['ca_id'], 1)
        self.assertEqual(account['ca_b_id'], 1)
        self.assertEqual(account['ca_c_id'], 1)
        self.assertTrue(account['ca_name'])
        self.assertIsInstance(account['ca_bal'], float)

    def test_generate_broker(self):
        """Test broker data generation."""
        broker = self.generator.generate_broker(1)

        self.assertEqual(broker['b_id'], 1)
        self.assertTrue(broker['b_name'])
        self.assertIsInstance(broker['b_num_trades'], int)
        self.assertIsInstance(broker['b_comm_total'], float)

    def test_generate_security(self):
        """Test security data generation."""
        security = self.generator.generate_security('AAPL', 1, 'NYSE')

        self.assertEqual(security['s_symb'], 'AAPL')
        self.assertEqual(security['s_co_id'], 1)
        self.assertEqual(security['s_ex_id'], 'NYSE')
        self.assertIsInstance(security['s_52wk_high'], float)
        self.assertIsInstance(security['s_52wk_low'], float)
        self.assertGreaterEqual(security['s_52wk_high'], security['s_52wk_low'])

    def test_generate_company(self):
        """Test company data generation."""
        company = self.generator.generate_company(1, 'IN01')

        self.assertEqual(company['co_id'], 1)
        self.assertEqual(company['co_in_id'], 'IN01')
        self.assertTrue(company['co_name'])
        self.assertTrue(company['co_ceo'])

    def test_generate_trade(self):
        """Test trade data generation."""
        trade = self.generator.generate_trade(1000, 1, 'S00001')

        self.assertEqual(trade['t_id'], 1000)
        self.assertEqual(trade['t_ca_id'], 1)
        self.assertEqual(trade['t_s_symb'], 'S00001')
        self.assertIsInstance(trade['t_qty'], int)
        self.assertIsInstance(trade['t_bid_price'], float)
        self.assertIsInstance(trade['t_is_cash'], bool)

    def test_generate_daily_market(self):
        """Test daily market data generation."""
        from datetime import date
        dm_date = date(2024, 1, 15)
        dm = self.generator.generate_daily_market('S00001', dm_date)

        self.assertEqual(dm['dm_s_symb'], 'S00001')
        self.assertEqual(dm['dm_date'], dm_date)
        self.assertIsInstance(dm['dm_close'], float)
        self.assertIsInstance(dm['dm_vol'], int)
        self.assertGreaterEqual(dm['dm_high'], dm['dm_low'])

    def test_generate_holding(self):
        """Test holding data generation."""
        holding = self.generator.generate_holding(1000, 1, 'S00001')

        self.assertEqual(holding['h_t_id'], 1000)
        self.assertEqual(holding['h_ca_id'], 1)
        self.assertEqual(holding['h_s_symb'], 'S00001')
        self.assertIsInstance(holding['h_qty'], int)
        self.assertIsInstance(holding['h_price'], float)

    def test_generate_holding_summary(self):
        """Test holding summary data generation."""
        hs = self.generator.generate_holding_summary(1, 'S00001', 500)

        self.assertEqual(hs['hs_ca_id'], 1)
        self.assertEqual(hs['hs_s_symb'], 'S00001')
        self.assertEqual(hs['hs_qty'], 500)

    def test_generate_watch_list(self):
        """Test watch list data generation."""
        wl = self.generator.generate_watch_list(1, 1)

        self.assertEqual(wl['wl_id'], 1)
        self.assertEqual(wl['wl_c_id'], 1)

    def test_generate_address(self):
        """Test address data generation."""
        addr = self.generator.generate_address(1)

        self.assertEqual(addr['ad_id'], 1)
        self.assertTrue(addr['ad_line1'])
        self.assertTrue(addr['ad_town'])
        self.assertTrue(addr['ad_ctry'])


class TestConfiguration(unittest.TestCase):
    """Test configuration files."""

    def test_cassandra_config_exists(self):
        self.assertTrue(Path('config/cassandra_config.yaml').exists(),
                        "Cassandra config file not found")

    def test_benchmark_config_exists(self):
        self.assertTrue(Path('config/benchmark_config.yaml').exists(),
                        "Benchmark config file not found")

    def test_schema_file_exists(self):
        self.assertTrue(Path('schema/tpce_schema.cql').exists(),
                        "TPC-E schema file not found")

    def test_cassandra_config_keyspace(self):
        """Test that keyspace is tpce_benchmark."""
        import yaml
        with open('config/cassandra_config.yaml') as f:
            config = yaml.safe_load(f)
        self.assertEqual(config['cassandra']['keyspace'], 'tpce_benchmark')

    def test_benchmark_config_tpce_params(self):
        """Test that benchmark config has TPC-E scale parameters."""
        import yaml
        with open('config/benchmark_config.yaml') as f:
            config = yaml.safe_load(f)
        dg = config['data_generation']
        self.assertIn('num_customers', dg)
        self.assertIn('num_brokers', dg)
        self.assertIn('num_securities', dg)
        self.assertIn('num_companies', dg)
        self.assertIn('num_trades', dg)

    def test_benchmark_config_snapshot_keys(self):
        """Test that benchmark config includes snapshot isolation settings."""
        import yaml
        with open('config/benchmark_config.yaml') as f:
            config = yaml.safe_load(f)
        bench = config['benchmark']
        self.assertIn('snapshot_before_benchmark', bench,
                      "Missing 'snapshot_before_benchmark' key in benchmark config")
        self.assertIn('cleanup_benchmark_keyspace', bench,
                      "Missing 'cleanup_benchmark_keyspace' key in benchmark config")
        self.assertIn('schema_file', bench,
                      "Missing 'schema_file' key in benchmark config")
        self.assertIsInstance(bench['snapshot_before_benchmark'], bool)
        self.assertIsInstance(bench['cleanup_benchmark_keyspace'], bool)
        self.assertTrue(bench['schema_file'],
                        "'schema_file' must not be empty")

    def test_snapshot_schema_file_exists(self):
        """Test that the schema_file referenced in benchmark config exists."""
        import yaml
        with open('config/benchmark_config.yaml') as f:
            config = yaml.safe_load(f)
        schema_file = config['benchmark']['schema_file']
        self.assertTrue(Path(schema_file).exists(),
                        f"schema_file '{schema_file}' referenced in benchmark config not found")


class TestSnapshotIsolation(unittest.TestCase):
    """Test snapshot isolation logic without a live Cassandra instance."""

    def _mock_cassandra_modules(self):
        """Inject lightweight mock modules so Cassandra-dependent code can import."""
        import sys
        from unittest.mock import MagicMock

        for mod in ['cassandra', 'cassandra.cluster', 'cassandra.auth',
                    'cassandra.query', 'cassandra.concurrent', 'numpy']:
            if mod not in sys.modules:
                sys.modules[mod] = MagicMock()

    def test_schema_setup_from_session_classmethod(self):
        """SchemaSetup.from_session should create an instance with the given session/config."""
        self._mock_cassandra_modules()
        import sys
        for key in list(sys.modules.keys()):
            if key.startswith('schema'):
                del sys.modules[key]

        from unittest.mock import MagicMock
        from schema.schema_setup import SchemaSetup

        mock_session = MagicMock()
        config = {'cassandra': {'keyspace': 'tpce_benchmark'}}
        instance = SchemaSetup.from_session(mock_session, config)

        self.assertIs(instance.session, mock_session)
        self.assertEqual(instance.config, config)
        self.assertIsNone(instance.cluster)

    def test_benchmark_runner_snapshot_attributes(self):
        """BenchmarkRunner.__init__ should set _snapshot_keyspace_name to None."""
        self._mock_cassandra_modules()
        import sys
        import inspect
        for key in list(sys.modules.keys()):
            if key.startswith('test_harness') or key.startswith('benchmarks') \
                    or key.startswith('queries'):
                del sys.modules[key]

        from test_harness.benchmark_runner import BenchmarkRunner

        # Verify that __init__ actually sets the attribute (not just that None == None)
        source = inspect.getsource(BenchmarkRunner.__init__)
        self.assertIn('_snapshot_keyspace_name', source,
                      "BenchmarkRunner.__init__ must initialise _snapshot_keyspace_name")
        self.assertIn('None', source,
                      "BenchmarkRunner.__init__ must set _snapshot_keyspace_name to None")


class TestProjectStructure(unittest.TestCase):
    """Test project structure and files."""

    def test_required_directories_exist(self):
        required_dirs = [
            'config', 'schema', 'queries', 'benchmarks',
            'data_generator', 'test_harness', 'results'
        ]
        for dir_name in required_dirs:
            self.assertTrue(Path(dir_name).exists(),
                            f"Required directory '{dir_name}' not found")

    def test_query_modules_exist(self):
        modules = [
            'queries/select_queries.py',
            'queries/insert_queries.py',
            'queries/update_queries.py',
            'queries/delete_queries.py',
        ]
        for module in modules:
            self.assertTrue(Path(module).exists(),
                            f"Query module '{module}' not found")

    def test_main_entry_point_exists(self):
        self.assertTrue(Path('main.py').exists(), "main.py not found")

    def test_readme_exists(self):
        self.assertTrue(Path('README.md').exists(), "README.md not found")

    def test_requirements_exists(self):
        self.assertTrue(Path('requirements.txt').exists(), "requirements.txt not found")

    def test_data_generator_module_exists(self):
        self.assertTrue(Path('data_generator/tpce_data_generator.py').exists(),
                        "tpce_data_generator.py not found")

    def test_schema_file_contains_tpce_tables(self):
        """Verify schema file contains key TPC-E tables."""
        schema_text = Path('schema/tpce_schema.cql').read_text()
        required_tables = ['customer', 'customer_account', 'broker', 'security',
                           'trade', 'holding', 'daily_market', 'company']
        for table in required_tables:
            self.assertIn(table, schema_text,
                          f"Table '{table}' not found in schema file")

    def test_cql_reference_files_exist(self):
        cql_files = [
            'queries/cql_reference/select_queries.cql',
            'queries/cql_reference/insert_queries.cql',
            'queries/cql_reference/update_queries.cql',
            'queries/cql_reference/delete_queries.cql',
            'queries/cql_reference/README.md',
        ]
        for f in cql_files:
            self.assertTrue(Path(f).exists(), f"CQL reference file '{f}' not found")


def run_validation_tests():
    """Run all validation tests."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    suite.addTests(loader.loadTestsFromTestCase(TestQueryDefinitions))
    suite.addTests(loader.loadTestsFromTestCase(TestTPCEDataGenerator))
    suite.addTests(loader.loadTestsFromTestCase(TestConfiguration))
    suite.addTests(loader.loadTestsFromTestCase(TestSnapshotIsolation))
    suite.addTests(loader.loadTestsFromTestCase(TestProjectStructure))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print("=" * 80)

    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_validation_tests()
    sys.exit(0 if success else 1)

# Cassandra TPC-C Benchmark - Quick Start Guide

Get up and running with the Cassandra TPC-C benchmark framework in 5 minutes!

## Prerequisites

- Python 3.8+
- Apache Cassandra 3.x or 4.x (or DataStax Astra)
- Cassandra running and accessible

## Installation

### 1. Install Python Dependencies

```bash
cd cassandra-benchmark
pip install -r requirements.txt
```

### 2. Verify Installation

```bash
python main.py info
```

You should see:
```
================================================================================
Cassandra TPC-C Benchmark Framework
================================================================================

Query Statistics:
  Total queries defined: 26

  By Type:
    SELECT    : 11
    INSERT    : 6
    UPDATE    : 5
    DELETE    : 4

  By Complexity:
    Simple    : 12
    Medium    : 8
    Complex   : 6
...
```

## Configuration

### 3. Configure Cassandra Connection

Edit `config/cassandra_config.yaml`:

```yaml
cassandra:
  contact_points: ["127.0.0.1"]  # Change to your Cassandra node IP
  port: 9042
  keyspace: "tpcc_benchmark"
  username: "cassandra"          # Optional, comment out if not needed
  password: "cassandra"          # Optional, comment out if not needed
```

### 4. Configure Benchmark Parameters (Optional)

Edit `config/benchmark_config.yaml` to adjust:
- Duration: `duration_seconds: 3600` (1 hour default)
- Concurrency: `concurrency: 50` (50 concurrent threads)
- Load pattern: `load_pattern: "constant"` (or ramp-up, spike, wave)

## Running the Benchmark

### 5. Setup Schema

Create the TPC-C keyspace and tables:

```bash
python main.py setup-schema
```

Expected output:
```
✓ Keyspace 'tpcc_benchmark' created/verified
✓ Created/verified table: warehouse
✓ Created/verified table: district
...
✓ Schema setup completed successfully
```

### 6. Generate Test Data

For quick testing, generate sample data:

```bash
python main.py generate-data --sample-only
```

For full dataset:

```bash
python main.py generate-data
```

Expected output:
```
Data scale:
  Warehouses: 2
  Districts: 4
  Customers: 400
  Items: 1000
  Estimated total records: 3,806

Loaded 2 warehouses
Loaded 4 districts
Loaded 400 customers
Loaded 1000 items
✓ Data generation and loading completed successfully
```

### 7. Run Benchmark

First, validate configuration with a dry run:

```bash
python main.py run-benchmark --dry-run
```

Then run the actual benchmark:

```bash
python main.py run-benchmark
```

Expected output:
```
Starting benchmark execution...
Running benchmark for 3600 seconds with constant load pattern
Interval metrics - QPS: 165.23, p95: 8.45ms, p99: 12.34ms
...
================================================================================
BENCHMARK SUMMARY
================================================================================
Total Queries:        10,000
Elapsed Time:         60.45 seconds
Queries per Second:   165.42

Latency Metrics:
  Average:            5.23 ms
  p50:                4.12 ms
  p95:                8.45 ms
  p99:                12.34 ms
  p999:               18.92 ms
  Max:                45.67 ms

Error Statistics:
  Total Errors:       0
  Error Rate:         0.00%
================================================================================
```

### 8. View Results

Results are saved in the `results/` directory:

```bash
ls -lh results/
```

You'll find:
- `metrics_TIMESTAMP.json` - Complete metrics with time-series data
- `metrics_TIMESTAMP.csv` - Time-series data for analysis

## Quick Commands Reference

```bash
# Get framework information
python main.py info

# Setup schema
python main.py setup-schema

# Generate data
python main.py generate-data --sample-only    # Quick test
python main.py generate-data                  # Full dataset

# Run benchmarks
python main.py run-benchmark --dry-run        # Validate only
python main.py run-benchmark                  # Run full benchmark

# Run specific query type
python main.py run-query select --iterations 100
python main.py run-query insert --iterations 50

# Cleanup (CAUTION: Deletes all data)
python main.py cleanup --force
```

## Common Options

### Custom Configuration Files

```bash
python main.py run-benchmark \
  -c config/cassandra_config.yaml \
  -b config/benchmark_config.yaml
```

### Verbose Logging

```bash
python main.py --verbose run-benchmark
```

### Sample Data Mode

```bash
python main.py generate-data --sample-only
```

## Troubleshooting

### Connection Error

**Problem**: `Cannot connect to Cassandra`

**Solution**:
```bash
# Check if Cassandra is running
nodetool status

# Verify contact_points in config/cassandra_config.yaml
# Ensure firewall allows port 9042
```

### Schema Already Exists

**Problem**: `Schema setup fails - keyspace exists`

**Solution**:
```bash
# Cleanup existing schema (CAUTION: Deletes data)
python main.py cleanup --force

# Then setup again
python main.py setup-schema
```

### Out of Memory

**Problem**: Data generation runs out of memory

**Solution**:
```bash
# Use sample data mode
python main.py generate-data --sample-only

# Or reduce scale in config/benchmark_config.yaml:
# num_warehouses: 2
# num_customers_per_district: 100
```

## Load Pattern Examples

### Constant Load (Default)
```yaml
load_pattern: "constant"
concurrency: 50
```
Maintains steady 50 concurrent queries throughout the test.

### Ramp-Up Load
```yaml
load_pattern: "ramp-up"
concurrency: 100
```
Gradually increases from 0 to 100 concurrent queries over 30% of duration.

### Spike Load
```yaml
load_pattern: "spike"
concurrency: 200
```
Alternates between low (50) and high (200) load with periodic spikes.

### Wave Load
```yaml
load_pattern: "wave"
concurrency: 100
```
Sinusoidal variation between 25 and 100 concurrent queries.

## Next Steps

1. **Adjust Configuration**: Tune benchmark parameters in `config/benchmark_config.yaml`
2. **Run Longer Tests**: Set `duration_seconds: 86400` for 24-hour soak test
3. **Analyze Results**: Use CSV exports for detailed time-series analysis
4. **Scale Up**: Increase data scale for larger datasets
5. **Read Documentation**: Check `README.md` for detailed information

## Need Help?

- Check `README.md` for comprehensive documentation
- Review `IMPLEMENTATION_SUMMARY.md` for technical details
- Check logs in `benchmark.log`
- Run validation tests: `python test_validation.py`

---

**Happy Benchmarking! 🚀**

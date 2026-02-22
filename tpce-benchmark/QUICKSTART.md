# TPC-E Benchmark Quick Start

Get up and running in 5 minutes.

## Prerequisites

- Python 3.8+
- Apache Cassandra 3.11+ running on `127.0.0.1:9042`

## Steps

### 1. Install dependencies

```bash
cd tpce-benchmark
pip install -r requirements.txt
```

### 2. Validate (no Cassandra needed)

```bash
python test_validation.py
```

Expected output: **all tests pass** (80 queries, data generator, config, structure).

### 3. Set up the schema

```bash
python main.py setup-schema --replication-factor 1
```

Creates the `tpce_benchmark` keyspace and all 42 tables.

### 4. Load sample data

```bash
python main.py generate-data --sample-only
```

Loads: 100 customers, 10 brokers, 100 companies, 500 securities, 1000 trades.

### 5. Run a short benchmark

```bash
# Edit config/benchmark_config.yaml: set duration_seconds: 60
python main.py run-benchmark
```

Results will be written to `./results/`.

### 6. View the query catalog

```bash
python main.py info
```

---

## Common Commands

```bash
# Full-scale data load
python main.py generate-data

# Benchmark with verbose logging
python main.py -v run-benchmark

# Dry run (validates config only)
python main.py run-benchmark --dry-run

# Custom config paths
python main.py setup-schema --config /path/to/cassandra_config.yaml
```

## Sample Output

```
2024-01-15 10:00:00 - INFO - Connected to Cassandra at ['127.0.0.1']
2024-01-15 10:00:00 - INFO - Using keyspace: tpce_benchmark
2024-01-15 10:00:10 - INFO - Interval metrics - QPS: 245.32, p95: 8.41ms, p99: 22.17ms
...
================================================================================
BENCHMARK SUMMARY
================================================================================
Total Queries:     14,720
QPS:               245.3
Avg Latency:       4.12 ms
P95 Latency:       8.41 ms
P99 Latency:      22.17 ms
Success Rate:      99.98%
================================================================================
```

## TPC-E Data Model Overview

TPC-E simulates a **brokerage firm's OLTP system**:

```
customer ──┬── customer_account ──┬── trade ──┬── trade_history
           │                      │            ├── holding
           └── watch_list         │            └── settlement
                └── watch_item    │
                                  └── holding_summary
broker ────── customer_account

security ──┬── trade
           ├── daily_market
           ├── last_trade
           └── holding_summary

company ───┬── security
           ├── financial
           └── news_xref ── news_item
```

See `README.md` for the complete query catalog and advanced feature documentation.

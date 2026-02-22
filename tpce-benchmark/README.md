# Cassandra TPC-E Benchmark Framework

A comprehensive Cassandra benchmarking framework built on the **TPC-E** (brokerage firm OLTP) data model. It provides 80 parameterized queries (20 SELECT, 20 INSERT, 20 UPDATE, 20 DELETE) across simple, medium, and complex complexity levels, demonstrating best-practice Cassandra data modelling and advanced CQL features.

---

## Features

- **TPC-E data model** – 29 core tables, 5 denormalized access-pattern tables, 8 advanced-feature tables
- **80 benchmark queries** – 20 per type (SELECT / INSERT / UPDATE / DELETE), spanning three complexity levels
- **Advanced Cassandra features** – Collections (set, list, map), UDTs, counter columns, static columns, TTL, LWT (lightweight transactions), LOGGED/UNLOGGED batches, custom timestamps, secondary indexes
- **Configurable load patterns** – Constant, ramp-up, spike, wave
- **Metrics collection** – Latency percentiles (p50/p95/p99/p99.9), QPS, error rates, per-query breakdown
- **Export formats** – JSON and CSV results
- **CLI interface** – `setup-schema`, `generate-data`, `run-benchmark`, `info` commands

---

## Architecture

```
tpce-benchmark/
├── config/               # YAML configuration files
│   ├── cassandra_config.yaml
│   └── benchmark_config.yaml
├── schema/               # CQL schema and setup utilities
│   ├── tpce_schema.cql
│   └── schema_setup.py
├── data_generator/       # TPC-E synthetic data generation
│   ├── tpce_data_generator.py
│   └── data_loader.py
├── benchmarks/           # Query registry and executor
│   ├── query_definitions.py   (80 queries)
│   └── query_executor.py
├── queries/              # Prepared-statement query implementations
│   ├── select_queries.py
│   ├── insert_queries.py
│   ├── update_queries.py
│   ├── delete_queries.py
│   └── cql_reference/    # Standalone CQL files for manual testing
├── test_harness/         # Benchmark orchestration
│   ├── benchmark_runner.py
│   ├── concurrency_manager.py
│   └── metrics_collector.py
├── results/              # Output directory (JSON / CSV)
├── main.py               # CLI entry point
└── test_validation.py    # Unit tests (no Cassandra required)
```

---

## Installation

```bash
cd tpce-benchmark
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

---

## Quick Start

### 1 – Validate the framework (no Cassandra needed)

```bash
python test_validation.py
```

### 2 – Set up the schema

```bash
python main.py setup-schema --replication-factor 1
```

### 3 – Load sample data

```bash
python main.py generate-data --sample-only
```

### 4 – Run the benchmark

```bash
python main.py run-benchmark
```

### 5 – View info

```bash
python main.py info
```

---

## Configuration

### `config/cassandra_config.yaml`

| Key | Default | Description |
|-----|---------|-------------|
| `contact_points` | `["127.0.0.1"]` | Cassandra node addresses |
| `port` | `9042` | CQL port |
| `keyspace` | `tpce_benchmark` | Target keyspace |
| `username` / `password` | `cassandra` | Authentication credentials |

### `config/benchmark_config.yaml`

| Key | Default | Description |
|-----|---------|-------------|
| `duration_seconds` | `3600` | Benchmark run time |
| `concurrency` | `50` | Parallel threads |
| `load_pattern` | `constant` | `constant`, `ramp-up`, `spike`, `wave` |
| `num_customers` | `1000` | TPC-E scale: customers |
| `num_brokers` | `100` | TPC-E scale: brokers |
| `num_securities` | `5000` | TPC-E scale: securities |
| `num_companies` | `1000` | TPC-E scale: companies |
| `num_trades` | `10000` | TPC-E scale: trades |

---

## Query Catalog

### SELECT Queries (20 total)

| ID | Name | Complexity | Description |
|----|------|-----------|-------------|
| S1 | Select Customer by ID | Simple | Single partition lookup |
| S2 | Select Account by ID | Simple | Single partition lookup |
| S3 | Select Broker by ID | Simple | Single partition lookup |
| S4 | Select Security by Symbol | Simple | Single partition lookup |
| S5 | Select Last Trade by Symbol | Simple | Single partition lookup |
| S6 | Select Status Type by ID | Simple | Single partition lookup |
| M1 | Select Trades by Account | Medium | Denormalized table scan |
| M2 | Select Holdings by Account | Medium | Multi-row partition query |
| M3 | Select Watch Items by Watch List | Medium | Clustering key scan |
| M4 | Select Daily Market Range | Medium | Date-range clustering query |
| M5 | Select Companies by Industry | Medium | ALLOW FILTERING |
| M6 | Select Financial by Company | Medium | Multi-row partition query |
| M7 | Select Holding Summary by Account | Medium | Multi-row partition query |
| M8 | Select News by Company | Medium | Denormalized table scan |
| C1 | Trades by Symbol with Date Range | Complex | Clustering key range |
| C2 | Customer by Name | Complex | ALLOW FILTERING |
| C3 | Active Trades with Filter | Complex | Secondary index + filter |
| C4 | Market Feed Latest | Complex | TTL table query |
| C5 | Portfolio Value Calculation | Complex | Multi-step join simulation |
| C6 | Broker Performance | Complex | Counter table + broker lookup |

### INSERT Queries (20 total)

| ID | Name | Complexity | Description |
|----|------|-----------|-------------|
| I1–I5 | Insert core entities | Simple | Customer, account, trade, holding, watch_item |
| I6 | Insert Trade with TTL | Medium | TTL market_feed record |
| I7 | Insert Trade History Batch | Medium | LOGGED BATCH |
| I8–I11 | Insert with Collections/UDTs | Medium | set, list, map, UDT |
| I12 | Insert Batch Trades | Medium | LOGGED BATCH |
| I13 | Insert with Timestamp | Medium | USING TIMESTAMP |
| I14 | Account Activity Counter | Medium | Counter increment |
| I15 | Insert Trade LWT | Complex | IF NOT EXISTS |
| I16 | Customer Denorm Multi-Table | Complex | customer + customer_extended |
| I17 | Portfolio Snapshot Static | Complex | Static columns |
| I18 | Trade All Collections | Complex | set + list + map |
| I19 | Watch List LWT | Complex | IF NOT EXISTS |
| I20 | Multiple Tables Batch | Complex | LOGGED BATCH across tables |

### UPDATE Queries (20 total)

| ID | Name | Complexity | Description |
|----|------|-----------|-------------|
| U1–U4 | Update core fields | Simple | balance, commission, qty, last_trade |
| U5 | Balance with LWT | Medium | IF condition |
| U6 | Holdings Batch | Medium | UNLOGGED BATCH |
| U7–U8 | Collection updates | Medium | list append, map update |
| U9 | Trade Status LWT | Medium | IF condition |
| U10–U14 | Various medium updates | Medium | multi-field, TTL, counter, timestamp, static |
| U15 | Trade LWT Complex | Complex | Multiple IF conditions |
| U16 | Account + Holding Batch | Complex | LOGGED BATCH |
| U17 | Collection with TTL | Complex | TTL + set add |
| U18 | LWT Multiple Conditions | Complex | Multiple IF |
| U19 | Unlogged Batch | Complex | UNLOGGED BATCH last_trade |
| U20 | Counter Columns | Complex | Multiple counter updates |

### DELETE Queries (20 total)

| ID | Name | Complexity | Description |
|----|------|-----------|-------------|
| D1–D3 | Simple deletes | Simple | watch_item, holding, column |
| D4 | Watch Item IF EXISTS | Medium | LWT delete |
| D5 | Old Market Feed Range | Medium | Clustering range |
| D6–D8 | Collection deletes | Medium | set element, map key, list |
| D9 | Delete with Timestamp | Medium | USING TIMESTAMP |
| D10 | Delete Static Column | Medium | portfolio_snapshot |
| D11 | Delete TTL Simulation | Medium | Full symbol partition |
| D12 | All Holdings (partition) | Complex | Partition delete |
| D13 | Trade + History Batch | Complex | LOGGED BATCH |
| D14 | Batch Watch Items | Complex | UNLOGGED BATCH |
| D15 | Trade History Range | Complex | Clustering key range |
| D16 | Delete with IN | Complex | IN clause on clustering key |
| D17 | Watch List LWT | Complex | IF EXISTS |
| D18 | LOGGED BATCH Delete | Complex | Logged batch |
| D19 | UNLOGGED BATCH Delete | Complex | Unlogged batch |
| D20 | Partition Delete | Complex | Full partition removal |

---

## TPC-E Cassandra Concepts Covered

| Concept | Tables / Queries |
|---------|-----------------|
| Denormalized access patterns | `trade_by_account`, `trade_by_symbol`, `holding_by_account`, `news_by_company`, `daily_market_by_symbol` |
| Counter columns | `account_activity`, `broker_metrics` |
| UDTs | `contact_info`, `address_type` used in `trade_extended`, `customer_extended` |
| Collections | `set<text>`, `list<text>`, `map<text,text>` in `trade_extended`, `customer_extended` |
| Static columns | `portfolio_snapshot` (account_name, account_bal) |
| TTL | `market_feed` (default_time_to_live = 86400) |
| Lightweight transactions | I15, I19, U5, U9, U15, U18, D4, D17 |
| LOGGED batch | I7, I12, I20, U16, D13, D18 |
| UNLOGGED batch | U6, U19, D14, D19 |
| Secondary indexes | `idx_trade_ca_id`, `idx_holding_summary_qty` |
| Custom timestamps | I13, U13, D9 |
| Clustering key ranges | C1, M4, D5, D15 |

---

## Usage Guide

### CLI Commands

```bash
# Setup schema (RF=1 for local dev, RF=3 for production)
python main.py setup-schema --replication-factor 1

# Generate full-scale data
python main.py generate-data

# Generate sample data (fast, for testing)
python main.py generate-data --sample-only

# Run full benchmark
python main.py run-benchmark

# Dry-run (validate connection without benchmarking)
python main.py run-benchmark --dry-run

# Show TPC-E info and query catalog summary
python main.py info

# Enable verbose logging
python main.py -v run-benchmark
```

### Running Individual Queries (Python)

```python
from cassandra.cluster import Cluster
from queries.select_queries import SelectQueries

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('tpce_benchmark')

sq = SelectQueries(session)
result = sq.select_customer_by_id(customer_id=1)
print(result)
```

---

## Metrics and Reports

Results are written to `./results/` after each benchmark run:

- `benchmark_results_<timestamp>.json` – Full time-series metrics
- `benchmark_results_<timestamp>.csv` – Per-query latency data

Metrics captured:
- Latency: p50, p95, p99, p99.9, max, average
- Throughput: queries per second (QPS)
- Error rate and error details
- Per-query-type and per-complexity breakdowns

---

## Best Practices

1. **Partition key selection** – Choose keys that distribute load evenly. Avoid hot partitions (e.g., do not use a single-row partition for market data; use `dm_s_symb` to spread across symbols).
2. **Denormalization** – Model for query patterns, not entity relationships. The five denormalized tables (`trade_by_account`, etc.) exist solely to support specific access patterns.
3. **TTL for time-series** – Use `default_time_to_live` on `market_feed` to avoid manual purge jobs.
4. **Counter tables** – Keep counter tables separate from regular tables (Cassandra restriction).
5. **LWT sparingly** – Lightweight transactions use Paxos consensus and have ~4× the latency of regular writes. Use only where serial consistency is required.
6. **Batch carefully** – LOGGED batches guarantee atomicity but add coordinator overhead. UNLOGGED batches are faster but lack atomicity. Prefer single-partition batches.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `NoHostAvailable` | Check `contact_points` in `cassandra_config.yaml` and ensure Cassandra is running |
| `Unauthorized` | Verify `username` / `password` in config |
| `InvalidRequest: Keyspace not found` | Run `python main.py setup-schema` first |
| `AllowFilteringRequired` | Expected for C2, C3, M5 – these use ALLOW FILTERING by design |
| High p99 latency | Reduce `concurrency` or simplify load pattern in `benchmark_config.yaml` |
| `CounterColumnException` | Counter columns (`account_activity`, `broker_metrics`) cannot be mixed with non-counter columns in the same write |

---

## License

This project is provided for educational and benchmarking purposes. TPC-E is a trademark of the Transaction Processing Performance Council (TPC).

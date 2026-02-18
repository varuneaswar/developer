# Cassandra TPC-C Benchmark Framework

A comprehensive benchmarking framework for Apache Cassandra database using the TPC-C data model. This framework enables soak testing with categorized queries, concurrent execution, and detailed performance metrics collection.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Query Catalog](#query-catalog)
- [Usage Guide](#usage-guide)
- [Metrics and Reports](#metrics-and-reports)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Features

### Core Capabilities

- **TPC-C Data Model**: Full implementation of TPC-C schema adapted for Cassandra best practices
- **80 Benchmark Queries**: **20 queries for each operation type** (SELECT, INSERT, UPDATE, DELETE) categorized by complexity (Simple, Medium, Complex)
- **Concurrent Execution**: Configurable concurrency levels (10-500+ concurrent connections)
- **Load Patterns**: Support for constant, ramp-up, spike, and wave load patterns
- **Soak Testing**: Duration-based execution for extended testing (hours to days)
- **Comprehensive Metrics**: Latency percentiles (p50, p95, p99, p999), throughput, error rates
- **Time-Series Data**: Metrics collected at regular intervals with JSON/CSV export
- **Data Generator**: TPC-C compliant test data generation with configurable scale

### Technical Features

- Python 3.8+ with type hints
- Prepared statements for optimal performance
- Connection pooling
- Async execution support
- Lightweight Transactions (LWT) support
- Batch operations following Cassandra best practices
- Collection types (Set, List, Map)
- Counter columns
- User Defined Types (UDT)
- Static columns
- TTL and Timestamps
- Real-time console output and logging

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Main CLI (main.py)                   │
└────────────────────────┬────────────────────────────────┘
                         │
          ┌──────────────┼──────────────┐
          │              │              │
┌─────────▼─────┐ ┌─────▼──────┐ ┌─────▼─────────┐
│ Schema Setup  │ │ Data       │ │ Benchmark     │
│               │ │ Generator  │ │ Runner        │
└───────────────┘ └────────────┘ └───────┬───────┘
                                          │
                    ┌─────────────────────┼─────────────────────┐
                    │                     │                     │
          ┌─────────▼────────┐  ┌────────▼────────┐  ┌────────▼────────┐
          │ Query Executor   │  │ Concurrency     │  │ Metrics         │
          │                  │  │ Manager         │  │ Collector       │
          └──────────────────┘  └─────────────────┘  └─────────────────┘
                    │
          ┌─────────┼─────────┐
          │         │         │         │
   ┌──────▼──┐ ┌───▼───┐ ┌───▼───┐ ┌──▼────┐
   │ SELECT  │ │INSERT │ │UPDATE │ │DELETE │
   │ Queries │ │Queries│ │Queries│ │Queries│
   └─────────┘ └───────┘ └───────┘ └───────┘
```

## Installation

### Prerequisites

- Python 3.8 or higher
- Apache Cassandra 3.x or 4.x (or DataStax Astra)
- 4GB+ RAM recommended for data generation
- Network access to Cassandra cluster

### Install Dependencies

```bash
cd cassandra-benchmark
pip install -r requirements.txt
```

### Verify Installation

```bash
python main.py info
```

## Quick Start

### 1. Configure Connection

Edit `config/cassandra_config.yaml`:

```yaml
cassandra:
  contact_points: ["127.0.0.1"]
  port: 9042
  keyspace: "tpcc_benchmark"
  username: "cassandra"
  password: "cassandra"
```

### 2. Setup Schema

```bash
python main.py setup-schema --replication-factor 1
```

### 3. Generate Test Data

```bash
# Generate sample data for testing
python main.py generate-data --sample-only

# Or generate full dataset
python main.py generate-data
```

### 4. Run Benchmark

```bash
# Dry run to validate configuration
python main.py run-benchmark --dry-run

# Run actual benchmark
python main.py run-benchmark
```

### 5. View Results

Results are saved in the `results/` directory:
- `metrics_TIMESTAMP.json` - Detailed metrics
- `metrics_TIMESTAMP.csv` - Time-series data

## Configuration

### Cassandra Configuration

File: `config/cassandra_config.yaml`

```yaml
cassandra:
  contact_points: ["127.0.0.1"]  # Cassandra node IPs
  port: 9042
  keyspace: "tpcc_benchmark"
  username: "cassandra"           # Optional
  password: "cassandra"           # Optional
  protocol_version: 4

connection_pool:
  core_connections_per_host: 2
  max_connections_per_host: 8
  max_requests_per_connection: 32768

timeouts:
  connect_timeout: 10
  request_timeout: 30
```

### Benchmark Configuration

File: `config/benchmark_config.yaml`

```yaml
benchmark:
  duration_seconds: 3600          # 1 hour
  concurrency: 50                 # Concurrent threads
  warmup_duration: 60             # Warmup period
  cooldown_duration: 30           # Cooldown period
  
  load_pattern: "constant"        # constant, ramp-up, spike, wave
  
  query_distribution:             # Percentage of each operation type
    select: 60
    insert: 20
    update: 15
    delete: 5
  
  complexity_distribution:        # Percentage by complexity
    simple: 50
    medium: 30
    complex: 20

data_generation:
  num_warehouses: 10
  num_districts_per_warehouse: 10
  num_customers_per_district: 3000
  num_items: 100000

metrics:
  collection_interval: 10         # seconds
  export_format: ["json", "csv"]
  output_dir: "./results"
```

## Query Catalog

The framework includes **80 queries** with **20 queries for each operation type**, covering comprehensive Cassandra concepts:

### SELECT Queries (20 queries)

**Simple Queries (S1-S6):**
- **S1**: Warehouse by ID - partition key lookup
- **S2**: Customer by ID - composite partition key
- **S3**: Item by ID - simple partition
- **S4**: District by ID - partition + clustering
- **S5**: Orders range - clustering key range
- **S6**: Warehouses IN - partition key IN clause

**Medium Queries (S7, S11, S13, M1-M4):**
- **S7**: Customer with token - token-based pagination
- **S11**: Order lines range - clustering range
- **S13**: Districts multi-warehouse - multi-partition IN
- **M1**: Customers by district - multi-row partition
- **M2**: Stock level - business logic query
- **M3**: Orders by customer - denormalized table query
- **M4**: Order lines - partition query

**Complex Queries (S8, S9, S10, S12, C1-C4):**
- **S8**: Items with filter - ALLOW FILTERING
- **S9**: Order count - COUNT aggregation
- **S10**: Customer projection - column selection
- **S12**: Orders by carrier - secondary index
- **C1**: Customers by name - denormalized query
- **C2**: New orders - multi-row query
- **C3**: History by date range - time-series
- **C4**: Customers by credit - secondary index

### INSERT Queries (20 queries)

**Simple Queries (I1-I3, I9, I20):**
- **I1**: Customer - basic insert
- **I2**: Order - denormalized insert
- **I3**: History - time-series insert
- **I9**: Counter - counter column update
- **I20**: Warehouse counter - counter increment

**Medium Queries (I4-I5, I8, I10-I15, I19):**
- **I4**: Order lines batch - batch operation
- **I5**: History with TTL - TTL insert
- **I8**: Customer collections - set/list/map
- **I10**: Customer with UDT - User Defined Type
- **I11**: Product with static - static column
- **I12**: Inventory log TTL - TTL insert
- **I13**: Orders batch logged - LOGGED batch
- **I14**: Tracking batch unlogged - UNLOGGED batch
- **I15**: Order with timestamp - custom timestamp
- **I19**: Activity JSON - JSON insert

**Complex Queries (I6-I7, I16-I18):**
- **I6**: New order LWT - Lightweight Transaction
- **I7**: Customer denormalization - multi-table
- **I16**: Item all types - all collections
- **I17**: Multiple tables - denormalization pattern
- **I18**: LWT condition - conditional insert

### UPDATE Queries (20 queries)

**Simple Queries (U1-U3, U15):**
- **U1**: Customer balance - basic update
- **U2**: Stock quantity - basic update
- **U3**: District next order - basic update
- **U15**: Metrics counter - counter update

**Medium Queries (U4, U6-U7, U9-U14, U16, U18):**
- **U4**: Order carrier conditional - conditional update
- **U6**: Stocks batch - batch update
- **U7**: Customer credit conditional - conditional
- **U9**: Customer add phone - set collection add
- **U10**: Preferences map - map update
- **U11**: Append email - list append
- **U12**: Remove phone - set remove
- **U13**: Order with TTL - TTL update
- **U14**: Customer with timestamp - timestamp
- **U16**: Multiple fields - multi-column
- **U18**: Static column - static update

**Complex Queries (U5, U8, U17, U19-U20):**
- **U5**: Stock with LWT - LWT validation
- **U8**: Order and customer batch - multi-table
- **U17**: Collection with TTL - collection + TTL
- **U19**: LWT multiple conditions - complex LWT
- **U20**: Batch unlogged - UNLOGGED batch

### DELETE Queries (20 queries)

**Simple Queries (D1-D2, D8):**
- **D1**: Order line - basic delete
- **D2**: New order - basic delete
- **D8**: Specific column - column delete

**Medium Queries (D3, D5, D9-D13, D17):**
- **D3**: New order conditional - IF EXISTS
- **D5**: Old history records - time-series delete
- **D9**: Set collection - set element remove
- **D10**: Map by key - map key remove
- **D11**: List by index - list index remove
- **D12**: With timestamp - timestamp delete
- **D13**: Static column - static delete
- **D17**: Expired records - cleanup

**Complex Queries (D4, D6-D7, D14-D16, D18-D20):**
- **D4**: All order lines - partition delete
- **D6**: Order with lines batch - multi-table
- **D7**: Multiple new orders - batch delete
- **D14**: Clustering range - range delete
- **D15**: With IN clause - IN delete
- **D16**: LWT condition - conditional delete
- **D18**: Batch logged - LOGGED batch
- **D19**: Batch unlogged - UNLOGGED batch
- **D20**: Partition delete - full partition

### Cassandra Concepts Covered

✅ Partition keys & clustering keys  
✅ Secondary indexes  
✅ Denormalized tables  
✅ Time-series patterns  
✅ Collections (Set, List, Map)  
✅ Counter columns  
✅ User Defined Types (UDT)  
✅ Static columns  
✅ TTL (Time To Live)  
✅ Timestamps  
✅ Lightweight Transactions (LWT)  
✅ Batch operations (LOGGED/UNLOGGED)  
✅ Token-based pagination  
✅ ALLOW FILTERING  
✅ IN clauses  
✅ Range queries  
✅ JSON support  
✅ Column-level operations  

## Usage Guide

### Command Reference

```bash
# Display framework information
python main.py info

# Setup schema
python main.py setup-schema [--replication-factor N]

# Generate data
python main.py generate-data [--sample-only]

# Run benchmark
python main.py run-benchmark [--dry-run]

# Run specific query type
python main.py run-query {select|insert|update|delete} [--iterations N]

# Cleanup
python main.py cleanup [--force]
```

### Load Patterns

#### Constant Load
Maintains steady concurrency throughout the test.

```yaml
load_pattern: "constant"
concurrency: 50
```

#### Ramp-Up Load
Gradually increases concurrency from 1 to configured maximum (over 30% of duration).

```yaml
load_pattern: "ramp-up"
concurrency: 100
```

#### Spike Load
Alternates between low and high load with periodic spikes.

```yaml
load_pattern: "spike"
concurrency: 200
```

#### Wave Load
Sinusoidal variation in load (25%-100% of configured concurrency).

```yaml
load_pattern: "wave"
concurrency: 100
```

### Soak Testing

For extended soak tests:

```yaml
benchmark:
  duration_seconds: 86400  # 24 hours
  concurrency: 100
  warmup_duration: 300     # 5 minutes
```

## Metrics and Reports

### Collected Metrics

#### Latency Metrics
- **p50**: Median latency
- **p95**: 95th percentile latency
- **p99**: 99th percentile latency
- **p999**: 99.9th percentile latency
- **max**: Maximum latency
- **avg**: Average latency

#### Throughput Metrics
- **QPS**: Queries per second
- **Total queries**: Total executed queries
- **Success rate**: Percentage of successful queries

#### Error Metrics
- **Error count**: Total failed queries
- **Error rate**: Percentage of failed queries
- **Error types**: Categorized errors

### Report Formats

#### JSON Report
Complete metrics with time-series data:

```json
{
  "summary": {
    "total_queries": 10000,
    "queries_per_second": 166.67,
    "avg_latency_ms": 5.23,
    "p95_latency_ms": 12.45,
    "p99_latency_ms": 18.92
  },
  "time_series": [...],
  "per_query": {...},
  "errors": [...]
}
```

#### CSV Report
Time-series data for analysis:

```csv
timestamp,elapsed_seconds,total_queries,qps,latency_p50_ms,latency_p95_ms,latency_p99_ms
2024-01-01T10:00:00,10,1000,100.0,3.2,8.5,15.2
```

## Best Practices

### Cassandra Configuration

1. **Replication Factor**: Use RF=3 for production testing
2. **Consistency Level**: QUORUM for reads and writes
3. **Connection Pooling**: Tune based on workload
4. **Timeout Values**: Adjust for network latency

### Benchmark Configuration

1. **Warmup**: Always use warmup period (60-300 seconds)
2. **Duration**: Soak tests should run for hours (4-24 hours)
3. **Concurrency**: Start low and gradually increase
4. **Query Distribution**: Match production workload patterns

### Data Generation

1. **Scale Factor**: Use appropriate warehouse count
   - Testing: 2-10 warehouses
   - Small: 10-50 warehouses
   - Medium: 50-100 warehouses
   - Large: 100+ warehouses

2. **Sample Data**: Use `--sample-only` for quick validation

### Performance Tuning

1. **Prepared Statements**: Always enabled (automatic)
2. **Batch Size**: Keep batches small (<100 rows)
3. **LWT Usage**: Minimize LWT queries (expensive)
4. **Denormalization**: Leverage denormalized tables

## Troubleshooting

### Connection Issues

**Problem**: Cannot connect to Cassandra

**Solutions**:
- Verify Cassandra is running: `nodetool status`
- Check contact_points in config
- Verify network connectivity
- Check authentication credentials

### Schema Creation Failures

**Problem**: Schema setup fails

**Solutions**:
- Ensure keyspace doesn't already exist
- Check user permissions
- Verify CQL syntax compatibility

### Data Generation Errors

**Problem**: Data loading fails or is slow

**Solutions**:
- Reduce scale factor for testing
- Check available memory
- Verify write timeout settings
- Monitor Cassandra logs

### Low Performance

**Problem**: Benchmark shows poor performance

**Solutions**:
- Check Cassandra cluster health
- Verify network latency
- Review query execution plans
- Adjust concurrency levels
- Check for hot partitions

### Memory Issues

**Problem**: Out of memory errors

**Solutions**:
- Reduce concurrent connections
- Use smaller batch sizes
- Reduce data scale
- Increase available memory

## Schema Design Notes

The TPC-C schema is adapted for Cassandra with:

- **Partition Key Strategy**: Designed to prevent hot partitions
- **Clustering Keys**: Optimized for query patterns
- **Denormalization**: customer_by_name and orders_by_customer tables
- **Time-Series Pattern**: history table with date buckets
- **Secondary Indexes**: Used sparingly (only 2 indexes)

## Contributing

This framework follows Python best practices:
- PEP 8 style guide
- Type hints throughout
- Comprehensive docstrings
- Modular, maintainable code

## License

This project is part of the developer portfolio.

## Support

For issues or questions:
1. Check this README
2. Review configuration files
3. Check Cassandra logs
4. Review benchmark.log file

---

**Version**: 1.0.0  
**Last Updated**: 2024  
**Cassandra Compatibility**: 3.x, 4.x  
**Python Version**: 3.8+

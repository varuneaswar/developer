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
- **20+ Benchmark Queries**: Categorized by operation type (SELECT, INSERT, UPDATE, DELETE) and complexity (Simple, Medium, Complex)
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

The framework includes 24+ queries across different categories:

### SELECT Queries (10 queries)

#### Simple (4 queries)
- **S1**: Select warehouse by ID
- **S2**: Select customer by ID
- **S3**: Select item by ID
- **S4**: Select district by ID

#### Medium (4 queries)
- **M1**: Select customers by district
- **M2**: Select stock level
- **M3**: Select orders by customer
- **M4**: Select order lines

#### Complex (4 queries)
- **C1**: Select customers by name
- **C2**: Select new orders
- **C3**: Select history by date range
- **C4**: Select customers by credit status

### INSERT Queries (7 queries)

- **I1**: Insert customer
- **I2**: Insert order
- **I3**: Insert history
- **I4**: Batch insert order lines
- **I5**: Insert history with TTL
- **I6**: Insert new order with LWT
- **I7**: Insert customer with denormalization

### UPDATE Queries (8 queries)

- **U1**: Update customer balance
- **U2**: Update stock quantity
- **U3**: Update district next order
- **U4**: Update order carrier (conditional)
- **U5**: Update customer credit (conditional)
- **U6**: Batch update stocks
- **U7**: Update stock with LWT
- **U8**: Batch update order and customer

### DELETE Queries (7 queries)

- **D1**: Delete order line
- **D2**: Delete new order
- **D3**: Delete new order (conditional)
- **D4**: Delete old history records
- **D5**: Delete all order lines
- **D6**: Delete order with lines (batch)
- **D7**: Batch delete new orders

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

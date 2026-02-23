# TPC-E CQL Reference Queries

This directory contains the physical CQL reference files for all 80 benchmark queries used in the TPC-E Cassandra Benchmark Framework.

## Files

| File | Contents |
|------|----------|
| `select_queries.cql` | 20 SELECT queries (6 simple, 8 medium, 6 complex) |
| `insert_queries.cql` | 20 INSERT queries (5 simple, 9 medium, 6 complex) |
| `update_queries.cql` | 20 UPDATE queries (4 simple, 10 medium, 6 complex) |
| `delete_queries.cql` | 20 DELETE queries (3 simple, 8 medium, 9 complex) |

## Query ID Convention

- **S1–S6** – Simple SELECT queries
- **M1–M8** – Medium SELECT queries
- **C1–C6** – Complex SELECT queries
- **I1–I5** – Simple INSERT queries
- **I6–I14** – Medium INSERT queries
- **I15–I20** – Complex INSERT queries
- **U1–U4** – Simple UPDATE queries
- **U5–U14** – Medium UPDATE queries
- **U15–U20** – Complex UPDATE queries
- **D1–D3** – Simple DELETE queries
- **D4–D11** – Medium DELETE queries
- **D12–D20** – Complex DELETE queries

## Usage

These CQL files are intended as reference documentation. The actual benchmark uses prepared statements from the Python query modules.

To run a query manually in `cqlsh`:

```cql
USE tpce_benchmark;
-- paste query here
```

## Parameterized Form

Queries use `?` as parameter placeholders (prepared statement style). Example values are shown in comments above each query.

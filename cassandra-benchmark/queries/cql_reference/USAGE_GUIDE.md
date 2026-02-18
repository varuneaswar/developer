# Using CQL Reference Files - Quick Guide

This guide shows how to use the physical CQL query reference files for testing, learning, and development.

## 📍 Location

All CQL reference files are in: `queries/cql_reference/`

## 🚀 Quick Start

### 1. View Available Queries

```bash
# List all CQL files
ls queries/cql_reference/*.cql

# View SELECT queries
cat queries/cql_reference/select_queries.cql

# View INSERT queries
cat queries/cql_reference/insert_queries.cql
```

### 2. Use in cqlsh

```bash
# Connect to Cassandra
cqlsh localhost 9042 -u cassandra -p cassandra

# Use the benchmark keyspace
USE tpcc_benchmark;

# Copy a query from the .cql file and run it
# Example from select_queries.cql (S1):
SELECT * FROM warehouse WHERE w_id = 1;
```

### 3. Use as Python Reference

```python
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('tpcc_benchmark')

# Use query from select_queries.cql (S1) as prepared statement
stmt = session.prepare("SELECT * FROM warehouse WHERE w_id = ?")
result = session.execute(stmt, [1])

for row in result:
    print(row)
```

## 📚 File Overview

| File | Queries | Description |
|------|---------|-------------|
| `select_queries.cql` | 20 | All SELECT operations |
| `insert_queries.cql` | 20 | All INSERT operations |
| `update_queries.cql` | 20 | All UPDATE operations |
| `delete_queries.cql` | 20 | All DELETE operations |
| `README.md` | - | Complete documentation |

## 🎯 Common Use Cases

### Testing a Single Query

1. Open the appropriate `.cql` file
2. Find the query you want to test (e.g., S1, I5, U10, D3)
3. Copy the example query (with actual values)
4. Run in cqlsh

**Example:**

```cql
-- From select_queries.cql - S2: Select Customer by ID
SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 100;
```

### Learning CQL Syntax

The files show:
- Basic queries (partition key lookups)
- Advanced features (collections, TTL, LWT)
- Best practices (using LIMIT, proper WHERE clauses)
- Performance considerations (ALLOW FILTERING notes)

**Example Learning Path:**

1. **Day 1**: Simple SELECT queries (S1-S6)
2. **Day 2**: Simple INSERT queries (I1-I3)
3. **Day 3**: Collections (I8, U9-U12, D9-D11)
4. **Day 4**: TTL and Timestamps (I5, I12, U13, U14)
5. **Day 5**: LWT and Batches (I6, I13-I14, U5, U19)

### Creating New Queries

Use the files as templates:

```cql
-- Template from select_queries.cql
SELECT * FROM your_table WHERE partition_key = ?;

-- Template from insert_queries.cql
INSERT INTO your_table (col1, col2, col3) VALUES (?, ?, ?);

-- Template from update_queries.cql
UPDATE your_table SET col1 = ? WHERE partition_key = ?;
```

### Debugging Issues

If a query fails in the benchmark:
1. Find the query ID in the error message
2. Open the corresponding `.cql` file
3. Copy the example query and test in cqlsh
4. Verify the data exists and the query works

## 📖 File Structure

Each `.cql` file follows this structure:

```
-- ============================================================================
-- [OPERATION] QUERIES FOR TPC-C BENCHMARK
-- ============================================================================
-- Total: 20 queries
-- ============================================================================

-- ============================================================================
-- SIMPLE [OPERATION] QUERIES
-- ============================================================================

-- [ID]: [Name]
-- Complexity: Simple/Medium/Complex
-- Description: [What it does]
[CQL Query with ?]

-- Example with actual values:
-- [CQL Query with real values]


-- ============================================================================
-- MEDIUM [OPERATION] QUERIES
-- ============================================================================
...

-- ============================================================================
-- COMPLEX [OPERATION] QUERIES
-- ============================================================================
...

-- ============================================================================
-- NOTES:
-- [Important information about usage, performance, best practices]
-- ============================================================================
```

## 🔍 Finding Specific Queries

### By Query ID

```bash
# Find query S5
grep -A 10 "^-- S5:" queries/cql_reference/select_queries.cql

# Find query I10
grep -A 10 "^-- I10:" queries/cql_reference/insert_queries.cql
```

### By Concept

```bash
# Find all queries using TTL
grep -i "ttl" queries/cql_reference/*.cql

# Find all queries using collections
grep -i "collection" queries/cql_reference/*.cql

# Find all LWT queries
grep -i "IF NOT EXISTS\|IF EXISTS\|IF .* =" queries/cql_reference/*.cql
```

### By Complexity

Open any `.cql` file and look for section headers:
- **SIMPLE**: Basic operations
- **MEDIUM**: Intermediate features
- **COMPLEX**: Advanced operations

## 💡 Tips and Tricks

### 1. Batch Testing Multiple Queries

```bash
# Create a test script
cat > test_queries.cql << 'EOF'
USE tpcc_benchmark;
SELECT * FROM warehouse WHERE w_id = 1;
SELECT * FROM district WHERE d_w_id = 1 AND d_id = 1;
SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 100;
EOF

# Run in cqlsh
cqlsh -f test_queries.cql
```

### 2. Extracting Just the CQL

```bash
# Get only the CQL statements (no comments)
grep -v '^--' queries/cql_reference/select_queries.cql | grep -v '^$'
```

### 3. Creating a Custom Query Subset

```bash
# Extract simple queries only
grep -A 5 "Complexity: Simple" queries/cql_reference/select_queries.cql
```

### 4. Performance Testing

```bash
# Use TRACING to see query execution
cqlsh> USE tpcc_benchmark;
cqlsh:tpcc_benchmark> TRACING ON;
cqlsh:tpcc_benchmark> SELECT * FROM warehouse WHERE w_id = 1;
```

## 🎓 Learning Exercises

### Exercise 1: Basic CRUD
Run all simple queries (S1-S6, I1-I3, U1-U3, D1-D2)

### Exercise 2: Collections
Test collection operations (I8, U9-U12, D9-D11)

### Exercise 3: Advanced Features
Experiment with TTL, timestamps, and LWT

### Exercise 4: Performance
Compare ALLOW FILTERING vs proper WHERE clauses

### Exercise 5: Batches
Test LOGGED vs UNLOGGED batches

## ⚠️ Important Notes

### Placeholders

Queries use `?` for parameterized values:
```cql
-- This is a template (for prepared statements)
SELECT * FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;

-- This is an example (for testing)
SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 100;
```

### Data Requirements

Some queries require data to exist:
1. Run `setup-schema` first
2. Run `generate-data --sample-only` to create test data
3. Then test queries

### Performance

- Simple queries: Fastest (single partition)
- Medium queries: Moderate (batches, collections)
- Complex queries: Slower (LWT, ALLOW FILTERING)

## 📞 Getting Help

- **Main README**: `../../README.md`
- **CQL Reference README**: `README.md` (in this directory)
- **Schema**: `../../schema/tpcc_schema.cql`
- **Python Code**: `../select_queries.py`, etc.

## ✅ Checklist for Using CQL Files

- [ ] Cassandra is running
- [ ] Keyspace `tpcc_benchmark` exists
- [ ] Sample data is loaded
- [ ] Connected to cqlsh
- [ ] Using correct keyspace (`USE tpcc_benchmark;`)
- [ ] Replaced `?` placeholders with actual values
- [ ] Query syntax is correct

## 🎯 Next Steps

1. **Explore**: Browse all `.cql` files
2. **Test**: Run simple queries in cqlsh
3. **Learn**: Study medium and complex queries
4. **Experiment**: Modify queries for your use case
5. **Integrate**: Use in your applications

---

**Happy Querying!** 🚀

For complete documentation, see [CQL Reference README](README.md)

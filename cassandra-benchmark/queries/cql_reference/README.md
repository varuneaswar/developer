# CQL Query Reference

This directory contains physical CQL query files for the Cassandra TPC-C Benchmark Framework. These files provide easy reference for all 80 benchmark queries organized by operation type.

## 📁 Files

| File | Queries | Description |
|------|---------|-------------|
| **select_queries.cql** | 20 | All SELECT queries (Simple, Medium, Complex) |
| **insert_queries.cql** | 20 | All INSERT queries including batches, TTL, LWT |
| **update_queries.cql** | 20 | All UPDATE queries including collections, conditionals |
| **delete_queries.cql** | 20 | All DELETE queries including batches, ranges, LWT |
| **all_queries.cql** | 80 | Complete reference with all queries |

## 🎯 Query Organization

### By Operation Type

- **SELECT**: 20 queries for data retrieval
- **INSERT**: 20 queries for data insertion
- **UPDATE**: 20 queries for data modification
- **DELETE**: 20 queries for data removal

### By Complexity Level

Each operation type includes queries at three complexity levels:

- **Simple**: Basic operations (partition key lookups, single-row operations)
- **Medium**: Intermediate operations (batches, collections, TTL, timestamps)
- **Complex**: Advanced operations (LWT, multi-table, range operations, filtering)

## 🚀 Usage

### In cqlsh

You can run these queries directly in `cqlsh`:

```bash
# Connect to Cassandra
cqlsh localhost 9042

# Use the benchmark keyspace
USE tpcc_benchmark;

# Copy and paste queries from the .cql files
# Replace ? placeholders with actual values
```

### Example: Running a SELECT Query

```cql
-- From select_queries.cql - S1: Select Warehouse by ID
SELECT * FROM warehouse WHERE w_id = 1;
```

### Example: Running an INSERT Query

```cql
-- From insert_queries.cql - I1: Insert Customer
INSERT INTO customer (
    c_w_id, c_d_id, c_id, c_first, c_middle, c_last,
    c_street_1, c_street_2, c_city, c_state, c_zip,
    c_phone, c_since, c_credit, c_credit_lim, c_discount,
    c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data
) VALUES (
    1, 1, 1001, 'John', 'A', 'Doe',
    '123 Main St', 'Apt 4B', 'Springfield', 'IL', '62701',
    '555-1234', '2024-01-01', 'GC', 50000.00, 0.1,
    -10.00, 10.00, 1, 1, 'Customer data here'
);
```

### Loading Query Files

You can load entire files in cqlsh using SOURCE:

```bash
cqlsh> USE tpcc_benchmark;
cqlsh:tpcc_benchmark> SOURCE '/path/to/queries/cql_reference/select_queries.cql';
```

**Note**: The SOURCE command will attempt to execute all statements. Since these files contain parametrized queries (with `?`), you should copy individual queries and replace placeholders with values.

## 📋 Query Format

### Parameterized Queries

Queries use `?` as placeholders for prepared statements:

```cql
SELECT * FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;
```

### Example Queries

Each query includes commented examples with actual values:

```cql
-- Example with actual values:
-- SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1001;
```

## 🔍 Query Index

### SELECT Queries (20)

**Simple (6)**:
- S1: Warehouse by ID
- S2: Customer by ID
- S3: Item by ID
- S4: District by ID
- S5: Orders by date range
- S6: Multiple warehouses (IN)

**Medium (7)**:
- S7: Customer with token pagination
- S11: Order lines range
- S13: Districts from multiple warehouses
- M1: Customers by district
- M2: Stock level
- M3: Orders by customer
- M4: Order lines

**Complex (7)**:
- S8: Items with ALLOW FILTERING
- S9: Count orders
- S10: Customer projection
- S12: Orders by carrier (index)
- C1: Customers by name
- C2: New orders
- C3: History by date range

### INSERT Queries (20)

**Simple (5)**:
- I1: Customer
- I2: Order
- I3: History
- I9: Counter (metrics)
- I20: Counter increment

**Medium (11)**:
- I4: Batch order lines
- I5: History with TTL
- I8: Customer collections
- I10: Customer with UDT
- I11: Product with static
- I12: Inventory log TTL
- I13: LOGGED batch
- I14: UNLOGGED batch
- I15: With timestamp
- I16: All collection types
- I19: JSON insert

**Complex (4)**:
- I6: New order LWT
- I7: Customer denormalization
- I17: Multiple tables
- I18: LWT with condition

### UPDATE Queries (20)

**Simple (4)**:
- U1: Customer balance
- U2: Stock quantity
- U3: District next order
- U15: Metrics counter

**Medium (12)**:
- U4: Order carrier (conditional)
- U6: Batch stocks
- U7: Customer credit (conditional)
- U9: Add phone (set)
- U10: Preferences (map)
- U11: Append email (list)
- U12: Remove phone (set)
- U13: With TTL
- U14: With timestamp
- U16: Multiple fields
- U18: Static column

**Complex (4)**:
- U5: Stock with LWT
- U8: Order and customer batch
- U17: Collection with TTL
- U19: LWT multiple conditions
- U20: UNLOGGED batch

### DELETE Queries (20)

**Simple (3)**:
- D1: Order line
- D2: New order
- D8: Specific column

**Medium (8)**:
- D3: New order (conditional)
- D5: Old history records
- D9: Set element
- D10: Map key
- D11: List element
- D12: With timestamp
- D13: Static column
- D17: Expired records

**Complex (9)**:
- D4: All order lines (partition)
- D6: Order with lines (batch)
- D7: Batch new orders
- D14: Clustering range
- D15: With IN clause
- D16: LWT condition
- D18: LOGGED batch
- D19: UNLOGGED batch
- D20: Full partition

## 💡 Key Concepts Covered

### Cassandra Features

✅ **Basic Operations**
- Partition key lookups
- Clustering key queries
- Multi-row queries

✅ **Collections**
- Set (add, remove)
- List (append, remove)
- Map (update, delete key)

✅ **Advanced Features**
- User Defined Types (UDT)
- Static columns
- Counter columns
- TTL (Time To Live)
- Timestamps

✅ **Consistency & Transactions**
- Lightweight Transactions (LWT)
- IF NOT EXISTS
- IF EXISTS
- IF conditions

✅ **Batching**
- LOGGED batches (atomic)
- UNLOGGED batches (performance)

✅ **Query Patterns**
- Token-based pagination
- Range queries
- ALLOW FILTERING
- IN clauses
- Secondary indexes

## ⚠️ Important Notes

### Prepared Statements

In production code, always use prepared statements:

```python
# Python example
stmt = session.prepare("SELECT * FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")
result = session.execute(stmt, [1, 1, 1001])
```

### Performance Considerations

1. **ALLOW FILTERING**: Use sparingly - scans entire table
2. **Secondary Indexes**: Best for low-cardinality columns
3. **LWT (IF conditions)**: Slower due to Paxos consensus
4. **LOGGED Batches**: Atomicity overhead
5. **Token Queries**: Efficient for pagination

### Best Practices

- Always specify full partition key
- Use LIMIT to control result sizes
- Batch operations on same partition
- Keep batches small (< 100 operations)
- Use TTL for temporary data
- Denormalize for query patterns

## 📚 Additional Resources

- **Main README**: `../../README.md` - Complete framework documentation
- **Schema**: `../../schema/tpcc_schema.cql` - Table definitions
- **Python Implementation**: `../` - Query implementations in Python
- **Benchmarks**: `../../benchmarks/` - Query executor and definitions

## 🔗 Related Files

- **Query Implementations**: `../select_queries.py`, `../insert_queries.py`, etc.
- **Query Definitions**: `../../benchmarks/query_definitions.py`
- **Schema Setup**: `../../schema/schema_setup.py`

## 🎓 Learning Path

1. **Start with Simple Queries**: Basic CRUD operations
2. **Explore Medium Queries**: Collections, batches, TTL
3. **Master Complex Queries**: LWT, multi-table, advanced patterns
4. **Run Benchmarks**: Test performance of different query patterns

## 📊 Statistics

- **Total Queries**: 80
- **SELECT**: 20 (25%)
- **INSERT**: 20 (25%)
- **UPDATE**: 20 (25%)
- **DELETE**: 20 (25%)

---

**Last Updated**: 2024  
**Framework Version**: 1.0  
**Compatible with**: Cassandra 3.x, 4.x, DataStax Astra

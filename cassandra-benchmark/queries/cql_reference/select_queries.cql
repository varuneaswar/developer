-- ============================================================================
-- SELECT QUERIES FOR TPC-C BENCHMARK
-- ============================================================================
-- This file contains all 20 SELECT queries used in the Cassandra TPC-C benchmark
-- Organized by complexity: Simple, Medium, Complex
-- Total: 20 queries
-- ============================================================================

-- ============================================================================
-- SIMPLE SELECT QUERIES (6 queries)
-- Single partition key lookups and basic queries
-- ============================================================================

-- S1: Select Warehouse by ID
-- Complexity: Simple - Single partition key lookup
-- Description: Get warehouse by ID
SELECT * FROM warehouse WHERE w_id = ?;

-- Example with actual value:
-- SELECT * FROM warehouse WHERE w_id = 1;


-- S2: Select Customer by ID
-- Complexity: Simple - Composite partition key lookup
-- Description: Get customer by ID
SELECT * FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;

-- Example with actual values:
-- SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 100;


-- S3: Select Item by ID
-- Complexity: Simple - Single partition key lookup
-- Description: Get item by ID
SELECT * FROM item WHERE i_id = ?;

-- Example with actual value:
-- SELECT * FROM item WHERE i_id = 1000;


-- S4: Select District by ID
-- Complexity: Simple - Composite partition key lookup
-- Description: Get district by ID
SELECT * FROM district WHERE d_w_id = ? AND d_id = ?;

-- Example with actual values:
-- SELECT * FROM district WHERE d_w_id = 1 AND d_id = 1;


-- S5: Select Orders by Date Range
-- Complexity: Simple - Clustering key range query
-- Description: Get orders within a date range
SELECT * FROM order_tracking 
WHERE ot_w_id = ? AND ot_d_id = ? 
  AND ot_entry_d >= ? AND ot_entry_d <= ?;

-- Example with actual values:
-- SELECT * FROM order_tracking 
-- WHERE ot_w_id = 1 AND ot_d_id = 1 
--   AND ot_entry_d >= '2024-01-01' AND ot_entry_d <= '2024-01-31';


-- S6: Select Multiple Warehouses using IN
-- Complexity: Simple - Partition key IN clause
-- Description: Get multiple warehouses by ID list
SELECT * FROM warehouse WHERE w_id IN ?;

-- Example with actual values:
-- SELECT * FROM warehouse WHERE w_id IN (1, 2, 3, 4, 5);


-- ============================================================================
-- MEDIUM SELECT QUERIES (7 queries)
-- Multi-row queries, pagination, denormalized tables
-- ============================================================================

-- S7: Select Customer with Token Pagination
-- Complexity: Medium - Token-based pagination
-- Description: Get customers with token for pagination
SELECT * FROM customer_activity 
WHERE ca_w_id = ? AND ca_d_id = ? 
  AND TOKEN(ca_w_id, ca_d_id) > ? 
LIMIT ?;

-- Example with actual values:
-- SELECT * FROM customer_activity 
-- WHERE ca_w_id = 1 AND ca_d_id = 1 
--   AND TOKEN(ca_w_id, ca_d_id) > -9223372036854775808 
-- LIMIT 100;


-- S11: Select Order Lines Range
-- Complexity: Medium - Clustering key range query
-- Description: Get order lines within a range
SELECT * FROM order_line 
WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?
  AND ol_number >= ? AND ol_number <= ?;

-- Example with actual values:
-- SELECT * FROM order_line 
-- WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 1000
--   AND ol_number >= 1 AND ol_number <= 10;


-- S13: Select Districts from Multiple Warehouses
-- Complexity: Medium - Multi-partition IN query
-- Description: Get districts from multiple warehouses
SELECT * FROM district WHERE d_w_id IN ? AND d_id = ?;

-- Example with actual values:
-- SELECT * FROM district WHERE d_w_id IN (1, 2, 3) AND d_id = 1;


-- M1: Select Customers by District
-- Complexity: Medium - Multi-row partition query
-- Description: Get customers in a district with limit
SELECT * FROM customer WHERE c_w_id = ? AND c_d_id = ? LIMIT ?;

-- Example with actual values:
-- SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = 1 LIMIT 100;


-- M2: Select Stock Level
-- Complexity: Medium - Business logic query
-- Description: Get stock information for an item in a warehouse
SELECT * FROM stock WHERE s_w_id = ? AND s_i_id = ?;

-- Example with actual values:
-- SELECT * FROM stock WHERE s_w_id = 1 AND s_i_id = 5000;


-- M3: Select Orders by Customer
-- Complexity: Medium - Denormalized table query
-- Description: Get orders for a customer from denormalized table
SELECT * FROM orders_by_customer 
WHERE o_c_id = ? AND o_w_id = ? AND o_d_id = ? 
LIMIT ?;

-- Example with actual values:
-- SELECT * FROM orders_by_customer 
-- WHERE o_c_id = 100 AND o_w_id = 1 AND o_d_id = 1 
-- LIMIT 50;


-- M4: Select Order Lines
-- Complexity: Medium - Partition query with all clustering keys
-- Description: Get all order lines for an order
SELECT * FROM order_line 
WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;

-- Example with actual values:
-- SELECT * FROM order_line 
-- WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 1000;


-- ============================================================================
-- COMPLEX SELECT QUERIES (7 queries)
-- Advanced queries with filtering, aggregations, secondary indexes
-- ============================================================================

-- S8: Select Items with ALLOW FILTERING
-- Complexity: Complex - Query with ALLOW FILTERING
-- Description: Find items by price range (requires ALLOW FILTERING)
SELECT * FROM item WHERE i_price >= ? AND i_price <= ? ALLOW FILTERING;

-- Example with actual values:
-- SELECT * FROM item WHERE i_price >= 10.0 AND i_price <= 100.0 ALLOW FILTERING;


-- S9: Count Orders
-- Complexity: Complex - COUNT aggregation
-- Description: Count total orders for a warehouse/district
SELECT COUNT(*) FROM orders WHERE o_w_id = ? AND o_d_id = ?;

-- Example with actual values:
-- SELECT COUNT(*) FROM orders WHERE o_w_id = 1 AND o_d_id = 1;


-- S10: Select Customer with Column Projection
-- Complexity: Complex - Column selection/projection
-- Description: Get specific customer fields only
SELECT c_id, c_first, c_last, c_balance, c_credit 
FROM customer 
WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;

-- Example with actual values:
-- SELECT c_id, c_first, c_last, c_balance, c_credit 
-- FROM customer 
-- WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 100;


-- S12: Select Orders by Carrier (Secondary Index)
-- Complexity: Complex - Secondary index query
-- Description: Find orders by carrier ID using secondary index
SELECT * FROM orders WHERE o_carrier_id = ? LIMIT ? ALLOW FILTERING;

-- Example with actual values:
-- SELECT * FROM orders WHERE o_carrier_id = 5 LIMIT 100 ALLOW FILTERING;


-- C1: Select Customers by Name
-- Complexity: Complex - Denormalized table for name lookup
-- Description: Find customers by last name
SELECT * FROM customer_by_name 
WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?;

-- Example with actual values:
-- SELECT * FROM customer_by_name 
-- WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'SMITH';


-- C2: Select New Orders
-- Complexity: Complex - Multi-row query with limit
-- Description: Get new orders for processing
SELECT * FROM new_order 
WHERE no_w_id = ? AND no_d_id = ? 
LIMIT ?;

-- Example with actual values:
-- SELECT * FROM new_order 
-- WHERE no_w_id = 1 AND no_d_id = 1 
-- LIMIT 20;


-- C3: Select History by Date Range
-- Complexity: Complex - Time-series query with range
-- Description: Get history records within date range
SELECT * FROM history 
WHERE h_w_id = ? AND h_d_id = ? AND date_bucket = ? 
  AND h_date >= ? AND h_date <= ?;

-- Example with actual values:
-- SELECT * FROM history 
-- WHERE h_w_id = 1 AND h_d_id = 1 AND date_bucket = '2024-01-15' 
--   AND h_date >= '2024-01-15 00:00:00' AND h_date <= '2024-01-15 23:59:59';


-- ============================================================================
-- NOTES:
-- 1. ? represents parameterized query placeholders (for prepared statements)
-- 2. Examples show actual values for testing in cqlsh
-- 3. ALLOW FILTERING should be used sparingly in production
-- 4. TOKEN() is used for pagination across partitions
-- 5. LIMIT clauses help control result set sizes
-- 6. Time-series queries use date_bucket for efficient partitioning
-- ============================================================================

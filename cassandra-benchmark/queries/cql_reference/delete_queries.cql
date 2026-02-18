-- ============================================================================
-- DELETE QUERIES FOR TPC-C BENCHMARK
-- ============================================================================
-- This file contains all 20 DELETE queries used in the Cassandra TPC-C benchmark
-- Organized by complexity: Simple, Medium, Complex
-- Total: 20 queries
-- ============================================================================

-- ============================================================================
-- SIMPLE DELETE QUERIES (3 queries)
-- Basic delete operations
-- ============================================================================

-- D1: Delete Order Line
-- Complexity: Simple - Basic delete
-- Description: Delete a specific order line
DELETE FROM order_line 
WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_number = ?;

-- Example with actual values:
-- DELETE FROM order_line 
-- WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 3001 AND ol_number = 5;


-- D2: Delete New Order
-- Complexity: Simple - Basic delete
-- Description: Delete a new order record
DELETE FROM new_order 
WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?;

-- Example with actual values:
-- DELETE FROM new_order 
-- WHERE no_w_id = 1 AND no_d_id = 1 AND no_o_id = 3001;


-- D8: Delete Specific Column
-- Complexity: Simple - Column-level delete
-- Description: Delete specific columns from customer
DELETE c_data, c_phone FROM customer 
WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;

-- Example with actual values:
-- DELETE c_data, c_phone FROM customer 
-- WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1001;


-- ============================================================================
-- MEDIUM DELETE QUERIES (8 queries)
-- Conditional deletes, collections, timestamps, TTL cleanup
-- ============================================================================

-- D3: Delete New Order (Conditional)
-- Complexity: Medium - Conditional delete
-- Description: Delete new order only if it exists
DELETE FROM new_order 
WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?
IF EXISTS;

-- Example with actual values:
-- DELETE FROM new_order 
-- WHERE no_w_id = 1 AND no_d_id = 1 AND no_o_id = 3001
-- IF EXISTS;


-- D5: Delete Old History Records
-- Complexity: Medium - Time-series delete
-- Description: Delete history records older than cutoff date
-- Note: Requires SELECT first, then DELETE each record
SELECT h_date, h_c_id FROM history 
WHERE h_w_id = ? AND h_d_id = ? AND date_bucket = ? AND h_date < ?;

-- Then for each returned row:
DELETE FROM history 
WHERE h_w_id = ? AND h_d_id = ? AND date_bucket = ? AND h_date = ? AND h_c_id = ?;

-- Example with actual values:
-- SELECT h_date, h_c_id FROM history 
-- WHERE h_w_id = 1 AND h_d_id = 1 AND date_bucket = '2024-01-01' AND h_date < '2024-01-01 12:00:00';
-- 
-- DELETE FROM history 
-- WHERE h_w_id = 1 AND h_d_id = 1 AND date_bucket = '2024-01-01' 
--   AND h_date = '2024-01-01 10:30:00' AND h_c_id = 1001;


-- D9: Delete Set Collection Element
-- Complexity: Medium - Remove from set collection
-- Description: Remove phone from customer's phone set
UPDATE customer_extended 
SET ce_phones = ce_phones - ?
WHERE ce_w_id = ? AND ce_d_id = ? AND ce_id = ?;

-- Example with actual values:
-- UPDATE customer_extended 
-- SET ce_phones = ce_phones - {'555-1234'}
-- WHERE ce_w_id = 1 AND ce_d_id = 1 AND ce_id = 1001;


-- D10: Delete Map by Key
-- Complexity: Medium - Remove map key
-- Description: Remove specific preference key from map
DELETE ce_preferences['newsletter'] FROM customer_extended 
WHERE ce_w_id = ? AND ce_d_id = ? AND ce_id = ?;

-- Example with actual values:
-- DELETE ce_preferences['newsletter'] FROM customer_extended 
-- WHERE ce_w_id = 1 AND ce_d_id = 1 AND ce_id = 1001;


-- D11: Delete List by Index
-- Complexity: Medium - Remove from list
-- Description: Remove email from customer's email list
-- Note: Cassandra doesn't support direct index removal, use list subtraction
UPDATE customer_extended 
SET ce_emails = ce_emails - ?
WHERE ce_w_id = ? AND ce_d_id = ? AND ce_id = ?;

-- Example with actual values:
-- UPDATE customer_extended 
-- SET ce_emails = ce_emails - ['old@example.com']
-- WHERE ce_w_id = 1 AND ce_d_id = 1 AND ce_id = 1001;


-- D12: Delete with Timestamp
-- Complexity: Medium - Delete with custom timestamp
-- Description: Delete order line with specific timestamp
DELETE FROM order_line 
WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_number = ?
USING TIMESTAMP ?;

-- Example with actual values (timestamp in microseconds):
-- DELETE FROM order_line 
-- WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 3001 AND ol_number = 3
-- USING TIMESTAMP 1705324800000000;


-- D13: Delete Static Column
-- Complexity: Medium - Static column delete
-- Description: Delete static column from product catalog
DELETE pc_category_desc FROM product_catalog 
WHERE pc_category = ?;

-- Example with actual values:
-- DELETE pc_category_desc FROM product_catalog 
-- WHERE pc_category = 'Electronics';


-- D17: Delete Expired Records
-- Complexity: Medium - Cleanup expired TTL records
-- Description: Delete expired inventory log records
-- Note: Records with TTL expire automatically, but you can also explicitly delete
DELETE FROM inventory_log 
WHERE il_w_id = ? AND il_i_id = ? AND il_timestamp < ?;

-- Example with actual values:
-- DELETE FROM inventory_log 
-- WHERE il_w_id = 1 AND il_i_id = 5000 AND il_timestamp < '2024-01-01 00:00:00';


-- ============================================================================
-- COMPLEX DELETE QUERIES (9 queries)
-- Multi-table deletes, range deletes, batches, LWT
-- ============================================================================

-- D4: Delete All Order Lines (Partition Delete)
-- Complexity: Complex - Partition-level delete
-- Description: Delete all order lines for an order
DELETE FROM order_line 
WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;

-- Example with actual values:
-- DELETE FROM order_line 
-- WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 3001;


-- D6: Delete Order with Lines (Multi-table Batch)
-- Complexity: Complex - Multi-table batch delete
-- Description: Delete order and all its lines atomically
BEGIN BATCH
    DELETE FROM orders 
    WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?;
    
    DELETE FROM orders_by_customer 
    WHERE o_c_id = ? AND o_w_id = ? AND o_d_id = ? AND o_id = ?;
    
    DELETE FROM order_line 
    WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;
APPLY BATCH;

-- Example with actual values:
-- BEGIN BATCH
--     DELETE FROM orders 
--     WHERE o_w_id = 1 AND o_d_id = 1 AND o_id = 3001;
--     
--     DELETE FROM orders_by_customer 
--     WHERE o_c_id = 1001 AND o_w_id = 1 AND o_d_id = 1 AND o_id = 3001;
--     
--     DELETE FROM order_line 
--     WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 3001;
-- APPLY BATCH;


-- D7: Batch Delete Multiple New Orders
-- Complexity: Complex - Batch delete
-- Description: Delete multiple new orders in a batch
BEGIN BATCH
    DELETE FROM new_order 
    WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?;
    -- Repeat for each order
APPLY BATCH;

-- Example with actual values:
-- BEGIN BATCH
--     DELETE FROM new_order 
--     WHERE no_w_id = 1 AND no_d_id = 1 AND no_o_id = 3001;
--     DELETE FROM new_order 
--     WHERE no_w_id = 1 AND no_d_id = 1 AND no_o_id = 3002;
--     DELETE FROM new_order 
--     WHERE no_w_id = 1 AND no_d_id = 1 AND no_o_id = 3003;
-- APPLY BATCH;


-- D14: Delete Clustering Range
-- Complexity: Complex - Range delete on clustering keys
-- Description: Delete range of order lines
-- Note: Must specify full partition key and clustering range
DELETE FROM order_line 
WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? 
  AND ol_number >= ? AND ol_number <= ?;

-- Example with actual values:
-- DELETE FROM order_line 
-- WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 3001 
--   AND ol_number >= 3 AND ol_number <= 5;


-- D15: Delete with IN Clause
-- Complexity: Complex - Multi-row delete with IN
-- Description: Delete multiple specific order lines
DELETE FROM order_line 
WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? 
  AND ol_number IN ?;

-- Example with actual values:
-- DELETE FROM order_line 
-- WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 3001 
--   AND ol_number IN (1, 3, 5);


-- D16: Delete with LWT Condition
-- Complexity: Complex - Conditional delete with LWT
-- Description: Delete new order only if order ID matches
DELETE FROM new_order 
WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?
IF no_o_id = ?;

-- Example with actual values:
-- DELETE FROM new_order 
-- WHERE no_w_id = 1 AND no_d_id = 1 AND no_o_id = 3001
-- IF no_o_id = 3001;


-- D18: Batch Delete (LOGGED)
-- Complexity: Complex - LOGGED batch for atomicity
-- Description: Delete related records with atomicity guarantee
BEGIN BATCH
    DELETE FROM new_order 
    WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?;
    
    DELETE FROM order_tracking 
    WHERE ot_w_id = ? AND ot_d_id = ? AND ot_entry_d = ? AND ot_o_id = ?;
APPLY BATCH;

-- Example with actual values:
-- BEGIN BATCH
--     DELETE FROM new_order 
--     WHERE no_w_id = 1 AND no_d_id = 1 AND no_o_id = 3001;
--     
--     DELETE FROM order_tracking 
--     WHERE ot_w_id = 1 AND ot_d_id = 1 AND ot_entry_d = '2024-01-15 10:00:00' AND ot_o_id = 3001;
-- APPLY BATCH;


-- D19: Batch Delete (UNLOGGED)
-- Complexity: Complex - UNLOGGED batch for performance
-- Description: Delete multiple records without atomicity overhead
BEGIN UNLOGGED BATCH
    DELETE FROM order_line 
    WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_number = ?;
    -- Repeat for each line
APPLY BATCH;

-- Example with actual values:
-- BEGIN UNLOGGED BATCH
--     DELETE FROM order_line 
--     WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 3001 AND ol_number = 1;
--     DELETE FROM order_line 
--     WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 3001 AND ol_number = 2;
--     DELETE FROM order_line 
--     WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 3001 AND ol_number = 3;
-- APPLY BATCH;


-- D20: Delete Full Partition
-- Complexity: Complex - Full partition delete
-- Description: Delete entire customer partition
DELETE FROM customer 
WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;

-- Also delete from denormalized table:
DELETE FROM customer_by_name 
WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? AND c_first = ? AND c_id = ?;

-- Example with actual values:
-- DELETE FROM customer 
-- WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1001;
-- 
-- DELETE FROM customer_by_name 
-- WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'Doe' AND c_first = 'John' AND c_id = 1001;


-- ============================================================================
-- NOTES:
-- 1. ? represents parameterized query placeholders (for prepared statements)
-- 2. Examples show actual values for testing in cqlsh
-- 3. IF conditions enable Lightweight Transactions (LWT) - slower but consistent
-- 4. Timestamps for USING TIMESTAMP are in microseconds
-- 5. Collection element removal:
--    - Set: Use UPDATE SET collection = collection - {element}
--    - Map: Use DELETE column['key']
--    - List: Use UPDATE SET collection = collection - [element]
-- 6. Partition deletes remove all rows for the given partition key
-- 7. Range deletes require full partition key and clustering range
-- 8. LOGGED batches guarantee atomicity across multiple tables
-- 9. UNLOGGED batches trade atomicity for better performance
-- 10. TTL records expire automatically; explicit delete is optional
-- 11. Static column deletes affect all rows in the partition
-- 12. IF EXISTS and IF conditions use Paxos consensus (expensive)
-- ============================================================================

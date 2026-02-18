-- ============================================================================
-- UPDATE QUERIES FOR TPC-C BENCHMARK
-- ============================================================================
-- This file contains all 20 UPDATE queries used in the Cassandra TPC-C benchmark
-- Organized by complexity: Simple, Medium, Complex
-- Total: 20 queries
-- ============================================================================

-- ============================================================================
-- SIMPLE UPDATE QUERIES (4 queries)
-- Basic update operations
-- ============================================================================

-- U1: Update Customer Balance
-- Complexity: Simple - Basic update
-- Description: Update customer balance and payment information
UPDATE customer 
SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ?
WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;

-- Example with actual values:
-- UPDATE customer 
-- SET c_balance = -50.00, c_ytd_payment = 150.00, c_payment_cnt = 5
-- WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1001;


-- U2: Update Stock Quantity
-- Complexity: Simple - Basic update
-- Description: Update stock quantity and statistics
UPDATE stock 
SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?
WHERE s_w_id = ? AND s_i_id = ?;

-- Example with actual values:
-- UPDATE stock 
-- SET s_quantity = 85, s_ytd = 150, s_order_cnt = 15
-- WHERE s_w_id = 1 AND s_i_id = 5000;


-- U3: Update District Next Order
-- Complexity: Simple - Basic update
-- Description: Update district next order ID
UPDATE district 
SET d_next_o_id = ?
WHERE d_w_id = ? AND d_id = ?;

-- Example with actual values:
-- UPDATE district 
-- SET d_next_o_id = 3005
-- WHERE d_w_id = 1 AND d_id = 1;


-- U15: Update Metrics Counter
-- Complexity: Simple - Counter update
-- Description: Update warehouse metrics counter
UPDATE warehouse_stats 
SET total_orders = total_orders + ?
WHERE ws_w_id = ?;

-- Example with actual values:
-- UPDATE warehouse_stats 
-- SET total_orders = total_orders + 1
-- WHERE ws_w_id = 1;


-- ============================================================================
-- MEDIUM UPDATE QUERIES (12 queries)
-- Conditional updates, collections, TTL, timestamps, batches
-- ============================================================================

-- U4: Update Order Carrier (Conditional)
-- Complexity: Medium - Conditional update
-- Description: Update order carrier only if not already assigned
UPDATE orders 
SET o_carrier_id = ?
WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?
IF o_carrier_id = null;

-- Example with actual values:
-- UPDATE orders 
-- SET o_carrier_id = 5
-- WHERE o_w_id = 1 AND o_d_id = 1 AND o_id = 3001
-- IF o_carrier_id = null;


-- U6: Batch Update Stocks
-- Complexity: Medium - Batch update
-- Description: Update multiple stock items in a batch
BEGIN BATCH
    UPDATE stock SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?
    WHERE s_w_id = ? AND s_i_id = ?;
    -- Repeat for each stock item
APPLY BATCH;

-- Example with actual values:
-- BEGIN BATCH
--     UPDATE stock SET s_quantity = 90, s_ytd = 160, s_order_cnt = 16
--     WHERE s_w_id = 1 AND s_i_id = 5000;
--     UPDATE stock SET s_quantity = 75, s_ytd = 200, s_order_cnt = 20
--     WHERE s_w_id = 1 AND s_i_id = 5001;
-- APPLY BATCH;


-- U7: Update Customer Credit (Conditional)
-- Complexity: Medium - Conditional update with data
-- Description: Update customer credit status conditionally
UPDATE customer 
SET c_credit = ?, c_data = ?
WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
IF c_credit = ?;

-- Example with actual values:
-- UPDATE customer 
-- SET c_credit = 'BC', c_data = 'Credit downgraded'
-- WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1001
-- IF c_credit = 'GC';


-- U9: Update Customer Add Phone (Set Collection)
-- Complexity: Medium - Add to set collection
-- Description: Add phone number to customer's phone set
UPDATE customer_extended 
SET ce_phones = ce_phones + ?
WHERE ce_w_id = ? AND ce_d_id = ? AND ce_id = ?;

-- Example with actual values:
-- UPDATE customer_extended 
-- SET ce_phones = ce_phones + {'555-9999'}
-- WHERE ce_w_id = 1 AND ce_d_id = 1 AND ce_id = 1001;


-- U10: Update Customer Preferences Map
-- Complexity: Medium - Update map collection
-- Description: Update customer preferences map
UPDATE customer_extended 
SET ce_preferences = ce_preferences + ?
WHERE ce_w_id = ? AND ce_d_id = ? AND ce_id = ?;

-- Example with actual values:
-- UPDATE customer_extended 
-- SET ce_preferences = ce_preferences + {'theme': 'dark', 'language': 'en'}
-- WHERE ce_w_id = 1 AND ce_d_id = 1 AND ce_id = 1001;


-- U11: Update Append Email (List Collection)
-- Complexity: Medium - Append to list collection
-- Description: Append email to customer's email list
UPDATE customer_extended 
SET ce_emails = ce_emails + ?
WHERE ce_w_id = ? AND ce_d_id = ? AND ce_id = ?;

-- Example with actual values:
-- UPDATE customer_extended 
-- SET ce_emails = ce_emails + ['newemail@example.com']
-- WHERE ce_w_id = 1 AND ce_d_id = 1 AND ce_id = 1001;


-- U12: Update Remove Phone (Set Collection)
-- Complexity: Medium - Remove from set collection
-- Description: Remove phone number from customer's phone set
UPDATE customer_extended 
SET ce_phones = ce_phones - ?
WHERE ce_w_id = ? AND ce_d_id = ? AND ce_id = ?;

-- Example with actual values:
-- UPDATE customer_extended 
-- SET ce_phones = ce_phones - {'555-5678'}
-- WHERE ce_w_id = 1 AND ce_d_id = 1 AND ce_id = 1001;


-- U13: Update Order with TTL
-- Complexity: Medium - Update with TTL
-- Description: Update order status with TTL (temporary status)
UPDATE order_tracking 
SET ot_status = ?
WHERE ot_w_id = ? AND ot_d_id = ? AND ot_entry_d = ? AND ot_o_id = ?
USING TTL ?;

-- Example with actual values (expires in 1 hour):
-- UPDATE order_tracking 
-- SET ot_status = 'PROCESSING'
-- WHERE ot_w_id = 1 AND ot_d_id = 1 AND ot_entry_d = '2024-01-15 10:00:00' AND ot_o_id = 3001
-- USING TTL 3600;


-- U14: Update Customer with Timestamp
-- Complexity: Medium - Update with custom timestamp
-- Description: Update customer with specific write timestamp
UPDATE customer 
SET c_balance = ?, c_ytd_payment = ?
WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
USING TIMESTAMP ?;

-- Example with actual values (timestamp in microseconds):
-- UPDATE customer 
-- SET c_balance = -75.00, c_ytd_payment = 175.00
-- WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1001
-- USING TIMESTAMP 1705324800000000;


-- U16: Update Multiple Fields
-- Complexity: Medium - Multi-column update
-- Description: Update multiple customer fields at once
UPDATE customer 
SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ?, 
    c_data = ?, c_credit = ?
WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;

-- Example with actual values:
-- UPDATE customer 
-- SET c_balance = -100.00, c_ytd_payment = 200.00, c_payment_cnt = 10, 
--     c_data = 'Updated customer data', c_credit = 'GC'
-- WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1001;


-- U18: Update Static Column
-- Complexity: Medium - Static column update
-- Description: Update static column in product catalog
UPDATE product_catalog 
SET pc_category_desc = ?
WHERE pc_category = ?;

-- Example with actual values:
-- UPDATE product_catalog 
-- SET pc_category_desc = 'All electronic devices, computers, and accessories'
-- WHERE pc_category = 'Electronics';


-- ============================================================================
-- COMPLEX UPDATE QUERIES (4 queries)
-- LWT with multiple conditions, multi-table batches, advanced operations
-- ============================================================================

-- U5: Update Stock with LWT
-- Complexity: Complex - LWT with validation
-- Description: Update stock only if quantity is sufficient
UPDATE stock 
SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ?
WHERE s_w_id = ? AND s_i_id = ?
IF s_quantity >= ?;

-- Example with actual values:
-- UPDATE stock 
-- SET s_quantity = 80, s_ytd = 180, s_order_cnt = 18, s_remote_cnt = 2
-- WHERE s_w_id = 1 AND s_i_id = 5000
-- IF s_quantity >= 10;


-- U8: Update Order and Customer (Multi-table Batch)
-- Complexity: Complex - Multi-table batch update
-- Description: Update order and customer in atomic batch
BEGIN BATCH
    UPDATE orders 
    SET o_carrier_id = ?
    WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?;
    
    UPDATE customer 
    SET c_balance = ?, c_delivery_cnt = ?
    WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;
APPLY BATCH;

-- Example with actual values:
-- BEGIN BATCH
--     UPDATE orders 
--     SET o_carrier_id = 5
--     WHERE o_w_id = 1 AND o_d_id = 1 AND o_id = 3001;
--     
--     UPDATE customer 
--     SET c_balance = -125.00, c_delivery_cnt = 5
--     WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1001;
-- APPLY BATCH;


-- U17: Update Collection with TTL
-- Complexity: Complex - Collection update with TTL
-- Description: Update customer preferences with expiration
UPDATE customer_extended 
SET ce_preferences = ce_preferences + ?
WHERE ce_w_id = ? AND ce_d_id = ? AND ce_id = ?
USING TTL ?;

-- Example with actual values (expires in 7 days):
-- UPDATE customer_extended 
-- SET ce_preferences = ce_preferences + {'temp_offer': 'winter_sale'}
-- WHERE ce_w_id = 1 AND ce_d_id = 1 AND ce_id = 1001
-- USING TTL 604800;


-- U19: Update with LWT Multiple Conditions
-- Complexity: Complex - LWT with multiple conditions
-- Description: Update stock with multiple conditional checks
UPDATE stock 
SET s_quantity = ?, s_ytd = ?
WHERE s_w_id = ? AND s_i_id = ?
IF s_quantity >= ? AND s_ytd < ?;

-- Example with actual values:
-- UPDATE stock 
-- SET s_quantity = 75, s_ytd = 190
-- WHERE s_w_id = 1 AND s_i_id = 5000
-- IF s_quantity >= 10 AND s_ytd < 200;


-- U20: Batch Update (UNLOGGED)
-- Complexity: Complex - UNLOGGED batch for performance
-- Description: Update multiple stocks without atomicity guarantee
BEGIN UNLOGGED BATCH
    UPDATE stock SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?
    WHERE s_w_id = ? AND s_i_id = ?;
    -- Repeat for each stock item
APPLY BATCH;

-- Example with actual values:
-- BEGIN UNLOGGED BATCH
--     UPDATE stock SET s_quantity = 88, s_ytd = 165, s_order_cnt = 17
--     WHERE s_w_id = 1 AND s_i_id = 5000;
--     UPDATE stock SET s_quantity = 72, s_ytd = 205, s_order_cnt = 21
--     WHERE s_w_id = 1 AND s_i_id = 5001;
--     UPDATE stock SET s_quantity = 95, s_ytd = 95, s_order_cnt = 10
--     WHERE s_w_id = 1 AND s_i_id = 5002;
-- APPLY BATCH;


-- ============================================================================
-- NOTES:
-- 1. ? represents parameterized query placeholders (for prepared statements)
-- 2. Examples show actual values for testing in cqlsh
-- 3. IF conditions enable Lightweight Transactions (LWT) - slower but consistent
-- 4. TTL values are in seconds
-- 5. Timestamps for USING TIMESTAMP are in microseconds
-- 6. Collection operations:
--    - Set: + adds, - removes
--    - List: + appends, - prepends (with prepend)
--    - Map: + merges/updates
-- 7. Counter updates can only increment/decrement
-- 8. Static columns are shared across all rows in a partition
-- 9. UNLOGGED batches trade atomicity for performance
-- 10. LOGGED batches guarantee atomicity but have overhead
-- ============================================================================

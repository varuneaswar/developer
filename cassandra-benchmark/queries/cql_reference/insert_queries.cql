-- ============================================================================
-- INSERT QUERIES FOR TPC-C BENCHMARK
-- ============================================================================
-- This file contains all 20 INSERT queries used in the Cassandra TPC-C benchmark
-- Organized by complexity: Simple, Medium, Complex
-- Total: 20 queries
-- ============================================================================

-- ============================================================================
-- SIMPLE INSERT QUERIES (5 queries)
-- Basic insert operations
-- ============================================================================

-- I1: Insert Customer
-- Complexity: Simple - Basic insert
-- Description: Insert new customer record
INSERT INTO customer (
    c_w_id, c_d_id, c_id, c_first, c_middle, c_last,
    c_street_1, c_street_2, c_city, c_state, c_zip,
    c_phone, c_since, c_credit, c_credit_lim, c_discount,
    c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- Example with actual values:
-- INSERT INTO customer (
--     c_w_id, c_d_id, c_id, c_first, c_middle, c_last,
--     c_street_1, c_street_2, c_city, c_state, c_zip,
--     c_phone, c_since, c_credit, c_credit_lim, c_discount,
--     c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data
-- ) VALUES (
--     1, 1, 1001, 'John', 'A', 'Doe',
--     '123 Main St', 'Apt 4B', 'Springfield', 'IL', '62701',
--     '555-1234', '2024-01-01', 'GC', 50000.00, 0.1,
--     -10.00, 10.00, 1, 1, 'Customer data here'
-- );


-- I2: Insert Order
-- Complexity: Simple - Basic insert
-- Description: Insert new order record
INSERT INTO orders (
    o_w_id, o_d_id, o_id, o_c_id, o_entry_d, 
    o_carrier_id, o_ol_cnt, o_all_local
) VALUES (?, ?, ?, ?, ?, ?, ?, ?);

-- Also insert into denormalized table:
INSERT INTO orders_by_customer (
    o_c_id, o_w_id, o_d_id, o_id, o_entry_d,
    o_carrier_id, o_ol_cnt, o_all_local
) VALUES (?, ?, ?, ?, ?, ?, ?, ?);

-- Example with actual values:
-- INSERT INTO orders (
--     o_w_id, o_d_id, o_id, o_c_id, o_entry_d, 
--     o_carrier_id, o_ol_cnt, o_all_local
-- ) VALUES (1, 1, 3001, 1001, '2024-01-15 10:30:00', null, 5, 1);


-- I3: Insert History
-- Complexity: Simple - Basic insert with time-series pattern
-- Description: Insert history record
INSERT INTO history (
    h_c_w_id, h_c_d_id, h_c_id, h_w_id, h_d_id,
    h_date, h_amount, h_data, date_bucket
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- Example with actual values:
-- INSERT INTO history (
--     h_c_w_id, h_c_d_id, h_c_id, h_w_id, h_d_id,
--     h_date, h_amount, h_data, date_bucket
-- ) VALUES (1, 1, 1001, 1, 1, '2024-01-15 10:30:00', 100.50, 'Payment received', '2024-01-15');


-- I9: Insert Counter (Warehouse Metrics)
-- Complexity: Simple - Counter column update
-- Description: Update warehouse metrics counter
UPDATE warehouse_metrics SET total_orders = total_orders + 1 WHERE wm_id = ?;

-- Example with actual value:
-- UPDATE warehouse_metrics SET total_orders = total_orders + 1 WHERE wm_id = 1;


-- I20: Insert Counter Increment
-- Complexity: Simple - Counter increment
-- Description: Increment warehouse counter
UPDATE warehouse_stats SET total_sales = total_sales + ? WHERE ws_w_id = ?;

-- Example with actual values:
-- UPDATE warehouse_stats SET total_sales = total_sales + 500 WHERE ws_w_id = 1;


-- ============================================================================
-- MEDIUM INSERT QUERIES (11 queries)
-- Batch operations, TTL, collections, UDT, static columns
-- ============================================================================

-- I4: Batch Insert Order Lines
-- Complexity: Medium - Batch operation
-- Description: Insert multiple order lines in a batch
BEGIN BATCH
    INSERT INTO order_line (
        ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id,
        ol_supply_w_id, ol_delivery_d, ol_quantity, 
        ol_amount, ol_dist_info
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    -- Repeat for each order line
APPLY BATCH;

-- Example with actual values:
-- BEGIN BATCH
--     INSERT INTO order_line (
--         ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id,
--         ol_supply_w_id, ol_delivery_d, ol_quantity, 
--         ol_amount, ol_dist_info
--     ) VALUES (1, 1, 3001, 1, 5000, 1, null, 5, 125.50, 'S  DIST 01');
--     INSERT INTO order_line (
--         ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id,
--         ol_supply_w_id, ol_delivery_d, ol_quantity, 
--         ol_amount, ol_dist_info
--     ) VALUES (1, 1, 3001, 2, 5001, 1, null, 3, 75.25, 'S  DIST 01');
-- APPLY BATCH;


-- I5: Insert History with TTL
-- Complexity: Medium - Insert with TTL
-- Description: Insert history record that expires after specified time
INSERT INTO history (
    h_c_w_id, h_c_d_id, h_c_id, h_w_id, h_d_id,
    h_date, h_amount, h_data, date_bucket
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
USING TTL ?;

-- Example with actual values (expires in 30 days):
-- INSERT INTO history (
--     h_c_w_id, h_c_d_id, h_c_id, h_w_id, h_d_id,
--     h_date, h_amount, h_data, date_bucket
-- ) VALUES (1, 1, 1001, 1, 1, '2024-01-15 10:30:00', 50.00, 'Temporary record', '2024-01-15')
-- USING TTL 2592000;


-- I8: Insert Customer with Collections
-- Complexity: Medium - Insert with collections (set, list, map)
-- Description: Insert customer with phone set, email list, preferences map
INSERT INTO customer_extended (
    ce_w_id, ce_d_id, ce_id, ce_first, ce_last,
    ce_phones, ce_emails, ce_preferences
) VALUES (?, ?, ?, ?, ?, ?, ?, ?);

-- Example with actual values:
-- INSERT INTO customer_extended (
--     ce_w_id, ce_d_id, ce_id, ce_first, ce_last,
--     ce_phones, ce_emails, ce_preferences
-- ) VALUES (
--     1, 1, 1001, 'John', 'Doe',
--     {'555-1234', '555-5678'},
--     ['john@example.com', 'jdoe@work.com'],
--     {'newsletter': 'true', 'sms_alerts': 'false'}
-- );


-- I10: Insert Customer with UDT
-- Complexity: Medium - Insert with User Defined Type
-- Description: Insert customer with address UDT
INSERT INTO customer_extended (
    ce_w_id, ce_d_id, ce_id, ce_first, ce_last, ce_address
) VALUES (?, ?, ?, ?, ?, ?);

-- Example with actual values:
-- INSERT INTO customer_extended (
--     ce_w_id, ce_d_id, ce_id, ce_first, ce_last, ce_address
-- ) VALUES (
--     1, 1, 1002, 'Jane', 'Smith',
--     {street_1: '456 Oak Ave', street_2: '', city: 'Boston', state: 'MA', zip: '02101'}
-- );


-- I11: Insert Product with Static Column
-- Complexity: Medium - Insert with static column
-- Description: Insert product with category-level static data
INSERT INTO product_catalog (
    pc_category, pc_item_id, pc_name, pc_price, pc_category_desc
) VALUES (?, ?, ?, ?, ?);

-- Example with actual values:
-- INSERT INTO product_catalog (
--     pc_category, pc_item_id, pc_name, pc_price, pc_category_desc
-- ) VALUES ('Electronics', 1001, 'Laptop', 999.99, 'Electronic devices and accessories');


-- I12: Insert Inventory Log with TTL
-- Complexity: Medium - Insert with TTL for temporary data
-- Description: Insert inventory log that auto-expires
INSERT INTO inventory_log (
    il_w_id, il_i_id, il_timestamp, il_quantity, il_type
) VALUES (?, ?, ?, ?, ?)
USING TTL ?;

-- Example with actual values (expires in 7 days):
-- INSERT INTO inventory_log (
--     il_w_id, il_i_id, il_timestamp, il_quantity, il_type
-- ) VALUES (1, 5000, '2024-01-15 14:30:00', 100, 'RESTOCK')
-- USING TTL 604800;


-- I13: LOGGED Batch Insert
-- Complexity: Medium - LOGGED batch for atomicity
-- Description: Insert multiple orders with atomicity guarantee
BEGIN BATCH
    INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    -- Multiple inserts
APPLY BATCH;

-- Example with actual values:
-- BEGIN BATCH
--     INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
--     VALUES (1, 1, 3002, 1001, '2024-01-15 11:00:00', null, 3, 1);
--     INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
--     VALUES (1, 1, 3003, 1002, '2024-01-15 11:05:00', null, 2, 1);
-- APPLY BATCH;


-- I14: UNLOGGED Batch Insert
-- Complexity: Medium - UNLOGGED batch for performance
-- Description: Insert multiple tracking records without atomicity overhead
BEGIN UNLOGGED BATCH
    INSERT INTO order_tracking (ot_w_id, ot_d_id, ot_entry_d, ot_o_id, ot_status)
    VALUES (?, ?, ?, ?, ?);
    -- Multiple inserts
APPLY BATCH;

-- Example with actual values:
-- BEGIN UNLOGGED BATCH
--     INSERT INTO order_tracking (ot_w_id, ot_d_id, ot_entry_d, ot_o_id, ot_status)
--     VALUES (1, 1, '2024-01-15 12:00:00', 3001, 'NEW');
--     INSERT INTO order_tracking (ot_w_id, ot_d_id, ot_entry_d, ot_o_id, ot_status)
--     VALUES (1, 1, '2024-01-15 12:05:00', 3002, 'NEW');
-- APPLY BATCH;


-- I15: Insert with Custom Timestamp
-- Complexity: Medium - Insert with custom write timestamp
-- Description: Insert order with specific timestamp for ordering
INSERT INTO orders (
    o_w_id, o_d_id, o_id, o_c_id, o_entry_d, 
    o_carrier_id, o_ol_cnt, o_all_local
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
USING TIMESTAMP ?;

-- Example with actual values (timestamp in microseconds):
-- INSERT INTO orders (
--     o_w_id, o_d_id, o_id, o_c_id, o_entry_d, 
--     o_carrier_id, o_ol_cnt, o_all_local
-- ) VALUES (1, 1, 3004, 1003, '2024-01-15 13:00:00', null, 4, 1)
-- USING TIMESTAMP 1705324800000000;


-- I16: Insert Item with All Collection Types
-- Complexity: Medium - Insert with set, list, and map
-- Description: Insert item with multiple collection types
INSERT INTO product_catalog (
    pc_category, pc_item_id, pc_name, pc_price,
    pc_tags, pc_reviews, pc_specs
) VALUES (?, ?, ?, ?, ?, ?, ?);

-- Example with actual values:
-- INSERT INTO product_catalog (
--     pc_category, pc_item_id, pc_name, pc_price,
--     pc_tags, pc_reviews, pc_specs
-- ) VALUES (
--     'Electronics', 1002, 'Smartphone', 599.99,
--     {'5G', 'waterproof', 'wireless-charging'},
--     ['Great phone!', 'Fast shipping', 'Love it'],
--     {'screen': '6.5 inch', 'storage': '128GB', 'ram': '8GB'}
-- );


-- I19: Insert Activity with JSON
-- Complexity: Medium - JSON insert
-- Description: Insert customer activity using JSON format
INSERT INTO customer_activity JSON '{"ca_w_id": ?, "ca_d_id": ?, "ca_timestamp": ?, "ca_c_id": ?, "ca_action": ?}';

-- Example with actual values:
-- INSERT INTO customer_activity JSON '{
--     "ca_w_id": 1, 
--     "ca_d_id": 1, 
--     "ca_timestamp": "2024-01-15T14:30:00Z", 
--     "ca_c_id": 1001, 
--     "ca_action": "LOGIN"
-- }';


-- ============================================================================
-- COMPLEX INSERT QUERIES (4 queries)
-- LWT, multi-table denormalization, complex operations
-- ============================================================================

-- I6: Insert New Order with LWT
-- Complexity: Complex - Lightweight Transaction
-- Description: Insert new order only if it doesn't exist
INSERT INTO new_order (no_w_id, no_d_id, no_o_id)
VALUES (?, ?, ?)
IF NOT EXISTS;

-- Example with actual values:
-- INSERT INTO new_order (no_w_id, no_d_id, no_o_id)
-- VALUES (1, 1, 3001)
-- IF NOT EXISTS;


-- I7: Insert Customer with Denormalization
-- Complexity: Complex - Multi-table insert
-- Description: Insert customer into main table and denormalized name lookup table
-- First insert into main customer table:
INSERT INTO customer (
    c_w_id, c_d_id, c_id, c_first, c_middle, c_last,
    c_street_1, c_street_2, c_city, c_state, c_zip,
    c_phone, c_since, c_credit, c_credit_lim, c_discount,
    c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- Then insert into denormalized customer_by_name table:
INSERT INTO customer_by_name (
    c_w_id, c_d_id, c_last, c_first, c_id,
    c_street_1, c_street_2, c_city, c_state, c_zip,
    c_phone, c_credit, c_credit_lim, c_discount, c_balance
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);


-- I17: Insert into Multiple Tables (Denormalization Pattern)
-- Complexity: Complex - Multi-table batch for denormalization
-- Description: Insert order into multiple tables for different access patterns
BEGIN BATCH
    INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    
    INSERT INTO orders_by_customer (o_c_id, o_w_id, o_d_id, o_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    
    INSERT INTO order_tracking (ot_w_id, ot_d_id, ot_entry_d, ot_o_id, ot_status)
    VALUES (?, ?, ?, ?, ?);
APPLY BATCH;


-- I18: Insert with LWT Condition
-- Complexity: Complex - Conditional insert with IF NOT EXISTS
-- Description: Insert warehouse stats only if not exists
INSERT INTO warehouse_stats (ws_w_id, ws_name, total_orders, total_sales)
VALUES (?, ?, 0, 0)
IF NOT EXISTS;

-- Example with actual values:
-- INSERT INTO warehouse_stats (ws_w_id, ws_name, total_orders, total_sales)
-- VALUES (1, 'Main Warehouse', 0, 0)
-- IF NOT EXISTS;


-- ============================================================================
-- NOTES:
-- 1. ? represents parameterized query placeholders (for prepared statements)
-- 2. Examples show actual values for testing in cqlsh
-- 3. TTL values are in seconds (e.g., 2592000 = 30 days)
-- 4. Timestamps for USING TIMESTAMP are in microseconds
-- 5. LOGGED batches guarantee atomicity but have performance overhead
-- 6. UNLOGGED batches are faster but don't guarantee atomicity
-- 7. LWT (IF NOT EXISTS) operations are slower due to Paxos consensus
-- 8. Collections: set {}, list [], map {}
-- 9. Counters can only be incremented/decremented, not set to absolute values
-- ============================================================================

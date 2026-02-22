-- TPC-E DELETE Queries Reference
-- 20 queries: D1-D3 (simple), D4-D11 (medium), D12-D20 (complex)
-- Use: cqlsh -k tpce_benchmark -f delete_queries.cql

USE tpce_benchmark;

-- ============================================================
-- SIMPLE DELETE QUERIES (D1-D3)
-- ============================================================

-- D1: Delete Watch Item
DELETE FROM watch_item WHERE wi_wl_id = 1 AND wi_s_symb = 'S00999';

-- D2: Delete Holding Record
DELETE FROM holding
WHERE h_ca_id = 1 AND h_s_symb = 'S00001'
  AND h_dts = '2024-01-01 00:00:00' AND h_t_id = 9000001;

-- D3: Delete Specific Column (set to null)
DELETE ca_name FROM customer_account WHERE ca_id = 1;

-- ============================================================
-- MEDIUM DELETE QUERIES (D4-D11)
-- ============================================================

-- D4: Delete Watch Item with IF EXISTS (LWT)
DELETE FROM watch_item WHERE wi_wl_id = 1 AND wi_s_symb = 'S00998'
IF EXISTS;

-- D5: Delete Old Market Feed Records (range delete on clustering key)
DELETE FROM market_feed
WHERE mf_s_symb = 'S00001' AND mf_dts < '2023-01-01 00:00:00';

-- D6: Delete Element from Set Collection
UPDATE trade_extended
SET t_tags = t_tags - {'reviewed'}
WHERE t_id = 1;

-- D7: Delete Key from Map Collection
DELETE c_preferences['theme'] FROM customer_extended WHERE c_id = 1;

-- D8: Delete Element from List (by value)
UPDATE customer_extended
SET c_email_history = c_email_history - ['']
WHERE c_id = 1;

-- D9: Delete Trade with Custom Timestamp
DELETE FROM trade USING TIMESTAMP 1700000000000000 WHERE t_id = 9000001;

-- D10: Delete Static Column from Portfolio Snapshot
DELETE account_name, account_bal FROM portfolio_snapshot WHERE ca_id = 9000099;

-- D11: Delete All Market Feed Records for Symbol (simulate TTL cleanup)
DELETE FROM market_feed WHERE mf_s_symb = 'S99999';

-- ============================================================
-- COMPLEX DELETE QUERIES (D12-D20)
-- ============================================================

-- D12: Delete All Holdings for an Account (partition delete)
DELETE FROM holding WHERE h_ca_id = 9000099;

-- D13: Delete Trade and History via LOGGED BATCH
BEGIN LOGGED BATCH
    DELETE FROM trade WHERE t_id = 9000001;
    DELETE FROM trade_history WHERE th_t_id = 9000001;
APPLY BATCH;

-- D14: Batch Delete Multiple Watch Items (UNLOGGED BATCH)
BEGIN UNLOGGED BATCH
    DELETE FROM watch_item WHERE wi_wl_id = 1 AND wi_s_symb = 'S09997';
    DELETE FROM watch_item WHERE wi_wl_id = 1 AND wi_s_symb = 'S09998';
    DELETE FROM watch_item WHERE wi_wl_id = 1 AND wi_s_symb = 'S09999';
APPLY BATCH;

-- D15: Delete Trade History within Clustering Key Range
DELETE FROM trade_history
WHERE th_t_id = 1
  AND th_dts >= '2023-01-01 00:00:00'
  AND th_dts <= '2023-12-31 23:59:59';

-- D16: Delete Multiple Watch Items using IN
DELETE FROM watch_item
WHERE wi_wl_id = 1 AND wi_s_symb IN ('S09991', 'S09992', 'S09993');

-- D17: Delete Watch List with LWT (IF EXISTS)
DELETE FROM watch_list WHERE wl_id = 9000001 IF EXISTS;

-- D18: LOGGED BATCH Delete across Watch Items
BEGIN LOGGED BATCH
    DELETE FROM watch_item WHERE wi_wl_id = 1 AND wi_s_symb = 'S09994';
    DELETE FROM watch_item WHERE wi_wl_id = 1 AND wi_s_symb = 'S09995';
APPLY BATCH;

-- D19: UNLOGGED BATCH Delete Market Feed Entries
BEGIN UNLOGGED BATCH
    DELETE FROM market_feed
    WHERE mf_s_symb = 'S99998' AND mf_dts < '2023-06-01 00:00:00';
    DELETE FROM market_feed
    WHERE mf_s_symb = 'S99998' AND mf_dts < '2023-06-02 00:00:00';
APPLY BATCH;

-- D20: Delete Entire trade_by_account Partition
DELETE FROM trade_by_account WHERE t_ca_id = 9000099;

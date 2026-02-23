-- TPC-E UPDATE Queries Reference
-- 20 queries: U1-U4 (simple), U5-U14 (medium), U15-U20 (complex)
-- Use: cqlsh -k tpce_benchmark -f update_queries.cql

USE tpce_benchmark;

-- ============================================================
-- SIMPLE UPDATE QUERIES (U1-U4)
-- ============================================================

-- U1: Update Account Balance
UPDATE customer_account SET ca_bal = 50000.00 WHERE ca_id = 1;

-- U2: Update Broker Commission Total
UPDATE broker SET b_comm_total = 250000.00 WHERE b_id = 1;

-- U3: Update Holding Summary Quantity
UPDATE holding_summary SET hs_qty = 500
WHERE hs_ca_id = 1 AND hs_s_symb = 'S00001';

-- U4: Update Last Trade Price and Volume
UPDATE last_trade
SET lt_dts = toTimestamp(now()), lt_price = 46.25, lt_open_price = 45.00, lt_vol = 15000
WHERE lt_s_symb = 'S00001';

-- ============================================================
-- MEDIUM UPDATE QUERIES (U5-U14)
-- ============================================================

-- U5: Update Account Balance with LWT Condition
UPDATE customer_account SET ca_bal = 60000.00
WHERE ca_id = 1
IF ca_bal = 50000.00;

-- U6: Update Holdings Batch (UNLOGGED BATCH for same partition)
BEGIN UNLOGGED BATCH
    UPDATE holding_summary SET hs_qty = 200
    WHERE hs_ca_id = 1 AND hs_s_symb = 'S00001';
    UPDATE holding_summary SET hs_qty = 100
    WHERE hs_ca_id = 1 AND hs_s_symb = 'S00002';
APPLY BATCH;

-- U7: Update Customer Email List (append to list)
UPDATE customer_extended
SET c_email_history = c_email_history + ['updated@example.com']
WHERE c_id = 1;

-- U8: Update Customer Preferences Map (add/update keys)
UPDATE customer_extended
SET c_preferences = c_preferences + {'theme': 'dark', 'lang': 'en'}
WHERE c_id = 1;

-- U9: Update Trade Status with LWT
UPDATE trade SET t_st_id = 'COMP'
WHERE t_id = 1
IF t_st_id = 'PNDG';

-- U10: Update Multiple Customer Account Fields
UPDATE customer_account
SET ca_name = 'Updated Account', ca_tax_st = 2
WHERE ca_id = 1;

-- U11: Update Market Feed with TTL
UPDATE market_feed USING TTL 7200
SET mf_price = 47.00
WHERE mf_s_symb = 'S00001' AND mf_dts = '2024-06-01 10:00:00';

-- U12: Increment Broker Metrics Counter
UPDATE broker_metrics
SET metric_value = metric_value + 1
WHERE b_id = 1 AND metric_name = 'total_trades';

-- U13: Update Trade with Custom Timestamp
UPDATE trade USING TIMESTAMP 1700000000000000
SET t_exec_name = 'NewExec'
WHERE t_id = 1;

-- U14: Update Portfolio Snapshot Static Column
UPDATE portfolio_snapshot
SET account_name = 'Premium Portfolio', account_bal = 130000.00
WHERE ca_id = 1;

-- ============================================================
-- COMPLEX UPDATE QUERIES (U15-U20)
-- ============================================================

-- U15: Complex LWT Update (multiple IF conditions)
UPDATE trade SET t_st_id = 'COMP'
WHERE t_id = 1
IF t_st_id = 'PNDG' AND t_tt_id = 'TMB';

-- U16: Batch Update Account and Holding Summary (LOGGED BATCH)
BEGIN LOGGED BATCH
    UPDATE customer_account SET ca_bal = 55000.00 WHERE ca_id = 1;
    UPDATE holding_summary SET hs_qty = 150
    WHERE hs_ca_id = 1 AND hs_s_symb = 'S00001';
APPLY BATCH;

-- U17: Update Set Collection with TTL
UPDATE trade_extended USING TTL 86400
SET t_tags = t_tags + {'reviewed'}
WHERE t_id = 1;

-- U18: LWT Update with Multiple Conditions
UPDATE customer_account SET ca_bal = 70000.00
WHERE ca_id = 1
IF ca_bal = 55000.00 AND ca_tax_st = 1;

-- U19: UNLOGGED BATCH Update Last Trade Prices
BEGIN UNLOGGED BATCH
    UPDATE last_trade
    SET lt_price = 46.00, lt_vol = 20000, lt_dts = toTimestamp(now())
    WHERE lt_s_symb = 'S00001';
    UPDATE last_trade
    SET lt_price = 121.00, lt_vol = 5000, lt_dts = toTimestamp(now())
    WHERE lt_s_symb = 'S00002';
APPLY BATCH;

-- U20: Update Multiple Counter Columns
UPDATE account_activity
SET activity_count = activity_count + 1
WHERE ca_id = 1 AND activity_type = 'trade';
UPDATE account_activity
SET activity_count = activity_count + 1
WHERE ca_id = 1 AND activity_type = 'order';

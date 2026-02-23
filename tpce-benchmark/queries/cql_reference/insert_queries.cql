-- TPC-E INSERT Queries Reference
-- 20 queries: I1-I5 (simple), I6-I14 (medium), I15-I20 (complex)
-- Use: cqlsh -k tpce_benchmark -f insert_queries.cql

USE tpce_benchmark;

-- ============================================================
-- SIMPLE INSERT QUERIES (I1-I5)
-- ============================================================

-- I1: Insert Customer
INSERT INTO customer (c_id, c_tax_id, c_st_id, c_l_name, c_f_name, c_m_name,
    c_gndr, c_tier, c_dob, c_ad_id, c_email_1, c_email_2)
VALUES (9000001, 'TAXID9001', 'ACTV', 'New', 'Customer', 'T',
    'M', 1, '1980-01-01', 1, 'new@example.com', '');

-- I2: Insert Customer Account
INSERT INTO customer_account (ca_id, ca_b_id, ca_c_id, ca_name, ca_tax_st, ca_bal)
VALUES (9000001, 1, 1, 'Test Account', 1, 10000.00);

-- I3: Insert Trade
INSERT INTO trade (t_id, t_dts, t_st_id, t_tt_id, t_is_cash,
    t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name,
    t_trade_price, t_chrg, t_comm, t_tax, t_lifo)
VALUES (9000001, toTimestamp(now()), 'ACTV', 'TMB', true,
    'S00001', 100, 45.00, 1, 'ExecA',
    45.00, 10.00, 5.00, 2.00, false);

-- I4: Insert Holding
INSERT INTO holding (h_ca_id, h_s_symb, h_dts, h_t_id, h_price, h_qty)
VALUES (1, 'S00001', toTimestamp(now()), 9000001, 45.00, 100);

-- I5: Insert Watch Item
INSERT INTO watch_item (wi_wl_id, wi_s_symb) VALUES (1, 'S00999');

-- ============================================================
-- MEDIUM INSERT QUERIES (I6-I14)
-- ============================================================

-- I6: Insert Trade Activity with TTL (24 hours)
INSERT INTO market_feed (mf_s_symb, mf_dts, mf_price, mf_vol)
VALUES ('S00001', toTimestamp(now()), 45.25, 10000)
USING TTL 86400;

-- I7: Insert Trade History Batch (LOGGED BATCH)
BEGIN LOGGED BATCH
    INSERT INTO trade_history (th_t_id, th_dts, th_st_id)
    VALUES (1, '2024-01-01 09:00:00', 'SBMT');
    INSERT INTO trade_history (th_t_id, th_dts, th_st_id)
    VALUES (1, '2024-01-01 09:01:00', 'PNDG');
    INSERT INTO trade_history (th_t_id, th_dts, th_st_id)
    VALUES (1, '2024-01-01 09:02:00', 'COMP');
APPLY BATCH;

-- I8: Insert Holding with Collections (trade_extended)
INSERT INTO trade_extended (t_id, t_tags, t_notes, t_attributes, t_created)
VALUES (9000002,
    {'equity', 'growth'},
    ['Bought at market open'],
    {'strategy': 'momentum', 'sector': 'tech'},
    toTimestamp(now()));

-- I9: Insert Market Feed with TTL (1 hour)
INSERT INTO market_feed (mf_s_symb, mf_dts, mf_price, mf_vol)
VALUES ('S00001', toTimestamp(now()), 45.25, 5000)
USING TTL 3600;

-- I10: Insert Trade Extended with All Collections
INSERT INTO trade_extended (t_id, t_tags, t_notes, t_attributes, t_created)
VALUES (9000003,
    {'options', 'hedge'},
    ['Initial entry'],
    {'hedge_ratio': '0.5'},
    toTimestamp(now()));

-- I11: Insert Customer Extended with UDT
INSERT INTO customer_extended (c_id, c_phone_numbers, c_email_history,
    c_preferences, c_tags, c_notes, c_created)
VALUES (9000002,
    {'555-1111'},
    ['old@example.com'],
    {'theme': 'light'},
    {'retail'},
    ['New customer'],
    toTimestamp(now()));

-- I12: Batch Insert Trades (LOGGED BATCH)
BEGIN LOGGED BATCH
    INSERT INTO trade (t_id, t_dts, t_st_id, t_tt_id, t_is_cash,
        t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name,
        t_trade_price, t_chrg, t_comm, t_tax, t_lifo)
    VALUES (9000010, toTimestamp(now()), 'ACTV', 'TMB', true,
        'S00001', 50, 45.00, 1, 'BatchExec', 45.00, 10.00, 5.00, 2.00, false);
    INSERT INTO trade (t_id, t_dts, t_st_id, t_tt_id, t_is_cash,
        t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name,
        t_trade_price, t_chrg, t_comm, t_tax, t_lifo)
    VALUES (9000011, toTimestamp(now()), 'ACTV', 'TMB', true,
        'S00002', 25, 120.00, 1, 'BatchExec', 120.00, 10.00, 5.00, 2.00, false);
APPLY BATCH;

-- I13: Insert with Custom Timestamp
INSERT INTO market_feed (mf_s_symb, mf_dts, mf_price, mf_vol)
VALUES ('S00001', '2024-01-15 10:30:00', 44.75, 2500)
USING TIMESTAMP 1700000000000000;

-- I14: Update Account Activity Counter (counter increment as "insert")
UPDATE account_activity
SET activity_count = activity_count + 1
WHERE ca_id = 1 AND activity_type = 'trade';

-- ============================================================
-- COMPLEX INSERT QUERIES (I15-I20)
-- ============================================================

-- I15: Insert Trade with LWT (IF NOT EXISTS)
INSERT INTO trade (t_id, t_dts, t_st_id, t_tt_id, t_is_cash,
    t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name,
    t_trade_price, t_chrg, t_comm, t_tax, t_lifo)
VALUES (9000099, toTimestamp(now()), 'ACTV', 'TMB', true,
    'S00001', 200, 45.50, 1, 'LWT_Exec',
    45.50, 10.00, 5.00, 2.00, false)
IF NOT EXISTS;

-- I16: Insert Customer + Customer Extended (multi-table denormalization)
INSERT INTO customer (c_id, c_tax_id, c_st_id, c_l_name, c_f_name, c_m_name,
    c_gndr, c_tier, c_dob, c_ad_id, c_email_1, c_email_2)
VALUES (9000003, 'TAXID9003', 'ACTV', 'Multi', 'Table', 'X',
    'F', 2, '1990-06-15', 2, 'multi@example.com', 'multi2@example.com');
INSERT INTO customer_extended (c_id, c_phone_numbers, c_email_history,
    c_preferences, c_tags, c_notes, c_created)
VALUES (9000003, {}, ['multi@example.com'], {}, {}, [], toTimestamp(now()));

-- I17: Insert Portfolio Snapshot with Static Columns
INSERT INTO portfolio_snapshot (ca_id, snap_date, snap_time,
    account_name, account_bal, s_symb, position_qty, position_val)
VALUES (1, '2024-06-01', toTimestamp(now()),
    'Main Portfolio', 125000.00, 'S00001', 100, 4500.00);

-- I18: Insert Trade Extended with All Collection Types
INSERT INTO trade_extended (t_id, t_tags, t_notes, t_attributes, t_created, t_updated)
VALUES (9000004,
    {'options', 'covered', 'short'},
    ['Day trade', 'Pre-market'],
    {'strategy': 'covered_call', 'expiry': '2024-12-20'},
    toTimestamp(now()),
    toTimestamp(now()));

-- I19: Insert Watch List with LWT (IF NOT EXISTS)
INSERT INTO watch_list (wl_id, wl_c_id) VALUES (9000001, 1) IF NOT EXISTS;

-- I20: Insert Trade into Multiple Tables via LOGGED BATCH
BEGIN LOGGED BATCH
    INSERT INTO trade (t_id, t_dts, t_st_id, t_tt_id, t_is_cash,
        t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name,
        t_trade_price, t_chrg, t_comm, t_tax, t_lifo)
    VALUES (9000005, toTimestamp(now()), 'SBMT', 'TLB', false,
        'S00002', 300, 120.00, 2, 'BatchExec',
        119.50, 12.00, 8.00, 3.00, true);
    INSERT INTO trade_by_account (t_ca_id, t_dts, t_id, t_st_id, t_tt_id,
        t_is_cash, t_s_symb, t_qty, t_bid_price, t_exec_name,
        t_trade_price, t_chrg, t_comm, t_tax, t_lifo)
    VALUES (2, toTimestamp(now()), 9000005, 'SBMT', 'TLB', false,
        'S00002', 300, 120.00, 'BatchExec',
        119.50, 12.00, 8.00, 3.00, true);
APPLY BATCH;

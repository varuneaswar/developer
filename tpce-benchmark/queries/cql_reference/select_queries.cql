-- TPC-E SELECT Queries Reference
-- 20 queries: S1-S6 (simple), M1-M8 (medium), C1-C6 (complex)
-- Use: cqlsh -k tpce_benchmark -f select_queries.cql
-- Parameter placeholders: ? (prepared statement style)
-- Example values shown in comments

USE tpce_benchmark;

-- ============================================================
-- SIMPLE SELECT QUERIES (S1-S6)
-- ============================================================

-- S1: Select Customer by ID
-- Params: customer_id=1
SELECT * FROM customer WHERE c_id = 1;

-- S2: Select Customer Account by ID
-- Params: account_id=1
SELECT * FROM customer_account WHERE ca_id = 1;

-- S3: Select Broker by ID
-- Params: broker_id=1
SELECT * FROM broker WHERE b_id = 1;

-- S4: Select Security by Symbol
-- Params: symbol='S00001'
SELECT * FROM security WHERE s_symb = 'S00001';

-- S5: Select Last Trade by Symbol
-- Params: symbol='S00001'
SELECT * FROM last_trade WHERE lt_s_symb = 'S00001';

-- S6: Select Status Type by ID
-- Params: status_id='ACTV'
SELECT * FROM status_type WHERE st_id = 'ACTV';

-- ============================================================
-- MEDIUM SELECT QUERIES (M1-M8)
-- ============================================================

-- M1: Select Trades by Account (denormalized table, latest first)
-- Params: account_id=1, limit=20
SELECT * FROM trade_by_account WHERE t_ca_id = 1 LIMIT 20;

-- M2: Select Holdings by Account
-- Params: account_id=1, limit=50
SELECT * FROM holding WHERE h_ca_id = 1 LIMIT 50;

-- M3: Select Watch Items by Watch List
-- Params: watchlist_id=1
SELECT * FROM watch_item WHERE wi_wl_id = 1;

-- M4: Select Daily Market Range
-- Params: symbol='S00001', start_date='2024-01-01', end_date='2024-03-31'
SELECT * FROM daily_market
WHERE dm_s_symb = 'S00001'
  AND dm_date >= '2024-01-01'
  AND dm_date <= '2024-03-31';

-- M5: Select Companies by Industry
-- Params: industry_id='IN01', limit=20
SELECT * FROM company WHERE co_in_id = 'IN01' LIMIT 20 ALLOW FILTERING;

-- M6: Select Financial Records by Company
-- Params: company_id=1, limit=8
SELECT * FROM financial WHERE fi_co_id = 1 LIMIT 8;

-- M7: Select Holding Summary by Account
-- Params: account_id=1
SELECT * FROM holding_summary WHERE hs_ca_id = 1;

-- M8: Select News by Company
-- Params: company_id=1, limit=10
SELECT * FROM news_by_company WHERE nx_co_id = 1 LIMIT 10;

-- ============================================================
-- COMPLEX SELECT QUERIES (C1-C6)
-- ============================================================

-- C1: Select Trades by Symbol with Date Range
-- Params: symbol='S00001', start_dts='2024-01-01 00:00:00',
--         end_dts='2024-03-31 23:59:59', limit=50
SELECT * FROM trade_by_symbol
WHERE t_s_symb = 'S00001'
  AND t_dts >= '2024-01-01 00:00:00'
  AND t_dts <= '2024-03-31 23:59:59'
LIMIT 50;

-- C2: Select Customer by Last Name (ALLOW FILTERING)
-- Params: last_name='Smith', limit=20
SELECT * FROM customer WHERE c_l_name = 'Smith' LIMIT 20 ALLOW FILTERING;

-- C3: Select Active Trades with Status Filter
-- Params: account_id=1, status_id='ACTV', limit=20
SELECT * FROM trade_by_account
WHERE t_ca_id = 1 AND t_st_id = 'ACTV'
LIMIT 20 ALLOW FILTERING;

-- C4: Select Latest Market Feed Entries (TTL table)
-- Params: symbol='S00001', limit=10
SELECT * FROM market_feed WHERE mf_s_symb = 'S00001' LIMIT 10;

-- C5: Select Portfolio Value (multi-step)
-- Step 1: Get holding summary
SELECT hs_s_symb, hs_qty FROM holding_summary WHERE hs_ca_id = 1;
-- Step 2: For each symbol, get last trade price
SELECT lt_price FROM last_trade WHERE lt_s_symb = 'S00001';

-- C6: Select Broker Performance Metrics
-- Step 1: Get broker details
SELECT * FROM broker WHERE b_id = 1;
-- Step 2: Get broker counter metrics
SELECT * FROM broker_metrics WHERE b_id = 1;

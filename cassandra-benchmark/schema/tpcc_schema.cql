-- TPC-C Schema for Cassandra
-- Adapted for Cassandra best practices with proper partitioning

-- Drop keyspace if exists (for clean setup)
-- DROP KEYSPACE IF EXISTS tpcc_benchmark;

-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS tpcc_benchmark 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE tpcc_benchmark;

-- WAREHOUSE table
-- Simple partition by warehouse_id
CREATE TABLE IF NOT EXISTS warehouse (
    w_id int PRIMARY KEY,
    w_name text,
    w_street_1 text,
    w_street_2 text,
    w_city text,
    w_state text,
    w_zip text,
    w_tax decimal,
    w_ytd decimal
);

-- DISTRICT table
-- Partition by warehouse_id, cluster by district_id
CREATE TABLE IF NOT EXISTS district (
    d_w_id int,
    d_id int,
    d_name text,
    d_street_1 text,
    d_street_2 text,
    d_city text,
    d_state text,
    d_zip text,
    d_tax decimal,
    d_ytd decimal,
    d_next_o_id int,
    PRIMARY KEY (d_w_id, d_id)
);

-- CUSTOMER table
-- Partition by (warehouse_id, district_id), cluster by customer_id
CREATE TABLE IF NOT EXISTS customer (
    c_w_id int,
    c_d_id int,
    c_id int,
    c_first text,
    c_middle text,
    c_last text,
    c_street_1 text,
    c_street_2 text,
    c_city text,
    c_state text,
    c_zip text,
    c_phone text,
    c_since timestamp,
    c_credit text,
    c_credit_lim decimal,
    c_discount decimal,
    c_balance decimal,
    c_ytd_payment decimal,
    c_payment_cnt int,
    c_delivery_cnt int,
    c_data text,
    PRIMARY KEY ((c_w_id, c_d_id), c_id)
);

-- CUSTOMER by last name - denormalized for query efficiency
CREATE TABLE IF NOT EXISTS customer_by_name (
    c_w_id int,
    c_d_id int,
    c_last text,
    c_first text,
    c_id int,
    c_middle text,
    c_street_1 text,
    c_street_2 text,
    c_city text,
    c_state text,
    c_zip text,
    c_phone text,
    c_since timestamp,
    c_credit text,
    c_credit_lim decimal,
    c_discount decimal,
    c_balance decimal,
    c_ytd_payment decimal,
    c_payment_cnt int,
    c_delivery_cnt int,
    c_data text,
    PRIMARY KEY ((c_w_id, c_d_id, c_last), c_first, c_id)
) WITH CLUSTERING ORDER BY (c_first ASC, c_id ASC);

-- ITEM table
-- Simple partition by item_id
CREATE TABLE IF NOT EXISTS item (
    i_id int PRIMARY KEY,
    i_im_id int,
    i_name text,
    i_price decimal,
    i_data text
);

-- STOCK table
-- Partition by (warehouse_id, item_id)
CREATE TABLE IF NOT EXISTS stock (
    s_w_id int,
    s_i_id int,
    s_quantity int,
    s_dist_01 text,
    s_dist_02 text,
    s_dist_03 text,
    s_dist_04 text,
    s_dist_05 text,
    s_dist_06 text,
    s_dist_07 text,
    s_dist_08 text,
    s_dist_09 text,
    s_dist_10 text,
    s_ytd int,
    s_order_cnt int,
    s_remote_cnt int,
    s_data text,
    PRIMARY KEY ((s_w_id, s_i_id))
);

-- ORDERS table
-- Partition by (warehouse_id, district_id), cluster by order_id descending for recent orders first
CREATE TABLE IF NOT EXISTS orders (
    o_w_id int,
    o_d_id int,
    o_id int,
    o_c_id int,
    o_entry_d timestamp,
    o_carrier_id int,
    o_ol_cnt int,
    o_all_local int,
    PRIMARY KEY ((o_w_id, o_d_id), o_id)
) WITH CLUSTERING ORDER BY (o_id DESC);

-- ORDERS by customer - denormalized for efficient customer order queries
CREATE TABLE IF NOT EXISTS orders_by_customer (
    o_c_id int,
    o_w_id int,
    o_d_id int,
    o_id int,
    o_entry_d timestamp,
    o_carrier_id int,
    o_ol_cnt int,
    o_all_local int,
    PRIMARY KEY ((o_c_id, o_w_id, o_d_id), o_entry_d, o_id)
) WITH CLUSTERING ORDER BY (o_entry_d DESC, o_id DESC);

-- NEW_ORDER table
-- Partition by (warehouse_id, district_id), cluster by order_id
CREATE TABLE IF NOT EXISTS new_order (
    no_w_id int,
    no_d_id int,
    no_o_id int,
    PRIMARY KEY ((no_w_id, no_d_id), no_o_id)
) WITH CLUSTERING ORDER BY (no_o_id ASC);

-- ORDER_LINE table
-- Partition by (warehouse_id, district_id, order_id), cluster by line_number
CREATE TABLE IF NOT EXISTS order_line (
    ol_w_id int,
    ol_d_id int,
    ol_o_id int,
    ol_number int,
    ol_i_id int,
    ol_supply_w_id int,
    ol_delivery_d timestamp,
    ol_quantity int,
    ol_amount decimal,
    ol_dist_info text,
    PRIMARY KEY ((ol_w_id, ol_d_id, ol_o_id), ol_number)
) WITH CLUSTERING ORDER BY (ol_number ASC);

-- HISTORY table
-- Time-series partition pattern: partition by (warehouse_id, district_id, date_bucket)
-- This allows efficient time-range queries and prevents partition hotspots
CREATE TABLE IF NOT EXISTS history (
    h_c_w_id int,
    h_c_d_id int,
    h_c_id int,
    h_w_id int,
    h_d_id int,
    h_date timestamp,
    h_amount decimal,
    h_data text,
    date_bucket text,  -- Format: YYYY-MM-DD for daily buckets
    PRIMARY KEY ((h_w_id, h_d_id, date_bucket), h_date, h_c_id)
) WITH CLUSTERING ORDER BY (h_date DESC, h_c_id ASC);

-- Create secondary indexes (use sparingly in Cassandra)
-- Only created for specific query patterns that justify the overhead

-- Index for customer credit status queries
CREATE INDEX IF NOT EXISTS idx_customer_credit ON customer (c_credit);

-- Index for order carrier tracking
CREATE INDEX IF NOT EXISTS idx_order_carrier ON orders (o_carrier_id);

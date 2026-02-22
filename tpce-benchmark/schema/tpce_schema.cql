-- TPC-E Schema for Cassandra
-- Adapted for Cassandra best practices with proper partitioning

-- Drop keyspace if exists (for clean setup)
-- DROP KEYSPACE IF EXISTS tpce_benchmark;

-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS tpce_benchmark
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE tpce_benchmark;

-- ========== USER DEFINED TYPES ==========

CREATE TYPE IF NOT EXISTS contact_info (
    ctry text,
    area text,
    local text,
    ext  text
);

CREATE TYPE IF NOT EXISTS address_type (
    line1   text,
    line2   text,
    town    text,
    div     text,
    zc_code text,
    ctry    text
);

-- ========== REFERENCE / LOOKUP TABLES ==========

CREATE TABLE IF NOT EXISTS status_type (
    st_id   text PRIMARY KEY,
    st_name text
);

CREATE TABLE IF NOT EXISTS trade_type (
    tt_id      text PRIMARY KEY,
    tt_name    text,
    tt_is_sell boolean,
    tt_is_mrkt boolean
);

CREATE TABLE IF NOT EXISTS industry (
    in_id   text PRIMARY KEY,
    in_name text,
    in_sc_id text
);

CREATE TABLE IF NOT EXISTS sector (
    sc_id   text PRIMARY KEY,
    sc_name text
);

CREATE TABLE IF NOT EXISTS zip_code (
    zc_code text PRIMARY KEY,
    zc_town text,
    zc_div  text
);

CREATE TABLE IF NOT EXISTS taxrate (
    tx_id   text PRIMARY KEY,
    tx_name text,
    tx_rate decimal
);

-- ========== CORE ENTITY TABLES ==========

-- ADDRESS table
CREATE TABLE IF NOT EXISTS address (
    ad_id     bigint PRIMARY KEY,
    ad_line1  text,
    ad_line2  text,
    ad_town   text,
    ad_div    text,
    ad_zc_code text,
    ad_ctry   text
);

-- EXCHANGE table
CREATE TABLE IF NOT EXISTS exchange (
    ex_id      text PRIMARY KEY,
    ex_name    text,
    ex_num_symb int,
    ex_open    int,
    ex_close   int,
    ex_desc    text,
    ex_ad_id   bigint
);

-- COMPANY table
CREATE TABLE IF NOT EXISTS company (
    co_id        bigint PRIMARY KEY,
    co_st_id     text,
    co_ad_id     bigint,
    co_name      text,
    co_in_id     text,
    co_sp_rate   text,
    co_ceo       text,
    co_desc      text,
    co_open_date date,
    co_co_id     bigint
);

-- BROKER table
CREATE TABLE IF NOT EXISTS broker (
    b_id          bigint PRIMARY KEY,
    b_st_id       text,
    b_name        text,
    b_num_trades  int,
    b_comm_total  decimal
);

-- CUSTOMER table
-- Partition by c_id (each customer is its own partition)
CREATE TABLE IF NOT EXISTS customer (
    c_id      bigint PRIMARY KEY,
    c_tax_id  text,
    c_st_id   text,
    c_l_name  text,
    c_f_name  text,
    c_m_name  text,
    c_gndr    text,
    c_tier    int,
    c_dob     date,
    c_ad_id   bigint,
    c_ctry_1  text,
    c_area_1  text,
    c_local_1 text,
    c_ext_1   text,
    c_ctry_2  text,
    c_area_2  text,
    c_local_2 text,
    c_ext_2   text,
    c_ctry_3  text,
    c_area_3  text,
    c_local_3 text,
    c_ext_3   text,
    c_email_1 text,
    c_email_2 text
);

-- CUSTOMER_ACCOUNT table
-- Partition by ca_id; secondary lookup by ca_c_id via denormalized table
CREATE TABLE IF NOT EXISTS customer_account (
    ca_id     bigint PRIMARY KEY,
    ca_b_id   bigint,
    ca_c_id   bigint,
    ca_name   text,
    ca_tax_st int,
    ca_bal    decimal
);

-- SECURITY table
CREATE TABLE IF NOT EXISTS security (
    s_symb          text PRIMARY KEY,
    s_issue         text,
    s_st_id         text,
    s_name          text,
    s_ex_id         text,
    s_co_id         bigint,
    s_num_out       bigint,
    s_start_date    date,
    s_exch_date     date,
    s_pe            decimal,
    s_52wk_high     decimal,
    s_52wk_high_date date,
    s_52wk_low      decimal,
    s_52wk_low_date date,
    s_dividend      decimal,
    s_yield         decimal
);

-- LAST_TRADE table
CREATE TABLE IF NOT EXISTS last_trade (
    lt_s_symb    text PRIMARY KEY,
    lt_dts       timestamp,
    lt_price     decimal,
    lt_open_price decimal,
    lt_vol        bigint
);

-- TRADE table
-- Partition by t_id; also denormalized views for account/symbol access
CREATE TABLE IF NOT EXISTS trade (
    t_id         bigint PRIMARY KEY,
    t_dts        timestamp,
    t_st_id      text,
    t_tt_id      text,
    t_is_cash    boolean,
    t_s_symb     text,
    t_qty        int,
    t_bid_price  decimal,
    t_ca_id      bigint,
    t_exec_name  text,
    t_trade_price decimal,
    t_chrg        decimal,
    t_comm        decimal,
    t_tax         decimal,
    t_lifo        boolean
);

-- TRADE_HISTORY table
-- Partition by th_t_id, cluster by th_dts DESC
CREATE TABLE IF NOT EXISTS trade_history (
    th_t_id  bigint,
    th_dts   timestamp,
    th_st_id text,
    PRIMARY KEY (th_t_id, th_dts)
) WITH CLUSTERING ORDER BY (th_dts DESC);

-- SETTLEMENT table
CREATE TABLE IF NOT EXISTS settlement (
    se_t_id         bigint PRIMARY KEY,
    se_cash_type    text,
    se_cash_due_date date,
    se_amt          decimal
);

-- HOLDING table
-- Partition by h_ca_id, cluster by h_s_symb and h_dts
CREATE TABLE IF NOT EXISTS holding (
    h_t_id   bigint,
    h_ca_id  bigint,
    h_s_symb text,
    h_dts    timestamp,
    h_price  decimal,
    h_qty    int,
    PRIMARY KEY (h_ca_id, h_s_symb, h_dts, h_t_id)
) WITH CLUSTERING ORDER BY (h_s_symb ASC, h_dts DESC, h_t_id ASC);

-- HOLDING_SUMMARY table
CREATE TABLE IF NOT EXISTS holding_summary (
    hs_ca_id  bigint,
    hs_s_symb text,
    hs_qty    int,
    PRIMARY KEY (hs_ca_id, hs_s_symb)
) WITH CLUSTERING ORDER BY (hs_s_symb ASC);

-- HOLDING_HISTORY table
CREATE TABLE IF NOT EXISTS holding_history (
    hh_h_t_id    bigint,
    hh_t_id      bigint,
    hh_before_qty int,
    hh_after_qty  int,
    PRIMARY KEY (hh_h_t_id, hh_t_id)
);

-- WATCH_LIST table
CREATE TABLE IF NOT EXISTS watch_list (
    wl_id   bigint,
    wl_c_id bigint,
    PRIMARY KEY (wl_id)
);

-- WATCH_ITEM table
-- Partition by wl_id, cluster by wi_s_symb
CREATE TABLE IF NOT EXISTS watch_item (
    wi_wl_id  bigint,
    wi_s_symb text,
    PRIMARY KEY (wi_wl_id, wi_s_symb)
) WITH CLUSTERING ORDER BY (wi_s_symb ASC);

-- DAILY_MARKET table
-- Partition by dm_s_symb, cluster by dm_date DESC
CREATE TABLE IF NOT EXISTS daily_market (
    dm_s_symb text,
    dm_date   date,
    dm_close  decimal,
    dm_high   decimal,
    dm_low    decimal,
    dm_vol    bigint,
    PRIMARY KEY (dm_s_symb, dm_date)
) WITH CLUSTERING ORDER BY (dm_date DESC);

-- FINANCIAL table
-- Partition by fi_co_id, cluster by fi_year, fi_qtr
CREATE TABLE IF NOT EXISTS financial (
    fi_co_id         bigint,
    fi_year          int,
    fi_qtr           int,
    fi_qtr_start_date date,
    fi_revenue       decimal,
    fi_net_earn      decimal,
    fi_basic_eps     decimal,
    fi_dilut_eps     decimal,
    fi_margin        decimal,
    fi_inventory     decimal,
    fi_assets        decimal,
    fi_liability     decimal,
    fi_out_basic     bigint,
    fi_out_dilut     bigint,
    PRIMARY KEY (fi_co_id, fi_year, fi_qtr)
) WITH CLUSTERING ORDER BY (fi_year DESC, fi_qtr DESC);

-- NEWS_ITEM table
CREATE TABLE IF NOT EXISTS news_item (
    ni_id       bigint PRIMARY KEY,
    ni_headline text,
    ni_summary  text,
    ni_item     text,
    ni_dts      timestamp,
    ni_source   text,
    ni_author   text
);

-- NEWS_XREF table
-- Partition by nx_co_id, cluster by nx_ni_id
CREATE TABLE IF NOT EXISTS news_xref (
    nx_co_id bigint,
    nx_ni_id bigint,
    PRIMARY KEY (nx_co_id, nx_ni_id)
) WITH CLUSTERING ORDER BY (nx_ni_id DESC);

-- CHARGE table
CREATE TABLE IF NOT EXISTS charge (
    ch_tt_id  text,
    ch_c_tier int,
    ch_chrg   decimal,
    PRIMARY KEY (ch_tt_id, ch_c_tier)
);

-- COMMISSION_RATE table
CREATE TABLE IF NOT EXISTS commission_rate (
    cr_c_tier   int,
    cr_tt_id    text,
    cr_ex_id    text,
    cr_from_qty int,
    cr_to_qty   int,
    cr_rate     decimal,
    PRIMARY KEY ((cr_c_tier, cr_tt_id, cr_ex_id), cr_from_qty)
) WITH CLUSTERING ORDER BY (cr_from_qty ASC);

-- CUSTOMER_TAXRATE table
CREATE TABLE IF NOT EXISTS customer_taxrate (
    cx_c_id  bigint,
    cx_tx_id text,
    PRIMARY KEY (cx_c_id, cx_tx_id)
);

-- ========== DENORMALIZED ACCESS PATTERN TABLES ==========

-- TRADE_BY_ACCOUNT: partition by ca_id, cluster by t_dts DESC
CREATE TABLE IF NOT EXISTS trade_by_account (
    t_ca_id      bigint,
    t_dts        timestamp,
    t_id         bigint,
    t_st_id      text,
    t_tt_id      text,
    t_is_cash    boolean,
    t_s_symb     text,
    t_qty        int,
    t_bid_price  decimal,
    t_exec_name  text,
    t_trade_price decimal,
    t_chrg        decimal,
    t_comm        decimal,
    t_tax         decimal,
    t_lifo        boolean,
    PRIMARY KEY (t_ca_id, t_dts, t_id)
) WITH CLUSTERING ORDER BY (t_dts DESC, t_id ASC);

-- TRADE_BY_SYMBOL: partition by t_s_symb, cluster by t_dts DESC
CREATE TABLE IF NOT EXISTS trade_by_symbol (
    t_s_symb     text,
    t_dts        timestamp,
    t_id         bigint,
    t_st_id      text,
    t_tt_id      text,
    t_is_cash    boolean,
    t_qty        int,
    t_bid_price  decimal,
    t_ca_id      bigint,
    t_exec_name  text,
    t_trade_price decimal,
    t_chrg        decimal,
    t_comm        decimal,
    t_tax         decimal,
    t_lifo        boolean,
    PRIMARY KEY (t_s_symb, t_dts, t_id)
) WITH CLUSTERING ORDER BY (t_dts DESC, t_id ASC);

-- HOLDING_BY_ACCOUNT: partition by h_ca_id, cluster by h_s_symb, h_dts
CREATE TABLE IF NOT EXISTS holding_by_account (
    h_ca_id  bigint,
    h_s_symb text,
    h_dts    timestamp,
    h_t_id   bigint,
    h_price  decimal,
    h_qty    int,
    PRIMARY KEY (h_ca_id, h_s_symb, h_dts)
) WITH CLUSTERING ORDER BY (h_s_symb ASC, h_dts DESC);

-- NEWS_BY_COMPANY: partition by nx_co_id, cluster by ni_dts DESC
CREATE TABLE IF NOT EXISTS news_by_company (
    nx_co_id    bigint,
    ni_dts      timestamp,
    ni_id       bigint,
    ni_headline text,
    ni_summary  text,
    ni_source   text,
    ni_author   text,
    PRIMARY KEY (nx_co_id, ni_dts, ni_id)
) WITH CLUSTERING ORDER BY (ni_dts DESC, ni_id ASC);

-- DAILY_MARKET_BY_SYMBOL: partition by dm_s_symb, cluster by dm_date DESC
-- (same as daily_market but included for pattern clarity)
CREATE TABLE IF NOT EXISTS daily_market_by_symbol (
    dm_s_symb text,
    dm_date   date,
    dm_close  decimal,
    dm_high   decimal,
    dm_low    decimal,
    dm_vol    bigint,
    PRIMARY KEY (dm_s_symb, dm_date)
) WITH CLUSTERING ORDER BY (dm_date DESC);

-- ========== ADVANCED CASSANDRA FEATURES TABLES ==========

-- ACCOUNT_ACTIVITY: counter columns per account
CREATE TABLE IF NOT EXISTS account_activity (
    ca_id         bigint,
    activity_type text,
    activity_count counter,
    PRIMARY KEY (ca_id, activity_type)
);

-- BROKER_METRICS: counter columns per broker
CREATE TABLE IF NOT EXISTS broker_metrics (
    b_id         bigint,
    metric_name  text,
    metric_value counter,
    PRIMARY KEY (b_id, metric_name)
);

-- MARKET_FEED: TTL time-series for real-time market data
CREATE TABLE IF NOT EXISTS market_feed (
    mf_s_symb  text,
    mf_dts     timestamp,
    mf_price   decimal,
    mf_vol     bigint,
    PRIMARY KEY (mf_s_symb, mf_dts)
) WITH CLUSTERING ORDER BY (mf_dts DESC)
  AND default_time_to_live = 86400;

-- TRADE_EXTENDED: collections and UDT
CREATE TABLE IF NOT EXISTS trade_extended (
    t_id          bigint PRIMARY KEY,
    t_tags        set<text>,
    t_notes       list<text>,
    t_attributes  map<text, text>,
    t_contact     frozen<contact_info>,
    t_created     timestamp,
    t_updated     timestamp
);

-- CUSTOMER_EXTENDED: collections and UDT
CREATE TABLE IF NOT EXISTS customer_extended (
    c_id          bigint PRIMARY KEY,
    c_address     frozen<address_type>,
    c_phone_numbers set<text>,
    c_email_history list<text>,
    c_preferences  map<text, text>,
    c_tags         set<text>,
    c_notes        list<text>,
    c_created      timestamp,
    c_updated      timestamp
);

-- PORTFOLIO_SNAPSHOT: static columns per account
CREATE TABLE IF NOT EXISTS portfolio_snapshot (
    ca_id         bigint,
    snap_date     date,
    snap_time     timestamp,
    account_name  text STATIC,
    account_bal   decimal STATIC,
    s_symb        text,
    position_qty  int,
    position_val  decimal,
    PRIMARY KEY (ca_id, snap_date, snap_time, s_symb)
) WITH CLUSTERING ORDER BY (snap_date DESC, snap_time DESC, s_symb ASC);

-- ========== SECONDARY INDEXES ==========

-- Index for querying trades by account (alternative to denormalized table)
CREATE INDEX IF NOT EXISTS idx_trade_ca_id ON trade (t_ca_id);

-- Index for filtering holding_summary by quantity
CREATE INDEX IF NOT EXISTS idx_holding_summary_qty ON holding_summary (hs_qty);

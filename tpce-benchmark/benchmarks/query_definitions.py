"""
Query definitions for TPC-E benchmark.
Central registry of all 80 queries (20 per type: SELECT, INSERT, UPDATE, DELETE).
"""

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Callable, Dict, List


class QueryType(Enum):
    """Query operation types."""

    SELECT = "select"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


class ComplexityLevel(Enum):
    """Query complexity levels."""

    SIMPLE = "simple"
    MEDIUM = "medium"
    COMPLEX = "complex"


@dataclass
class QueryDefinition:
    """Definition of a benchmark query."""

    query_id: str
    name: str
    query_type: QueryType
    complexity: ComplexityLevel
    description: str
    method_name: str
    params_generator: Callable


class QueryDefinitions:
    """Central registry of all TPC-E benchmark queries."""

    def __init__(self):
        """Initialize query definitions."""
        self.queries: Dict[str, QueryDefinition] = {}
        self._register_queries()

    def _register_queries(self) -> None:
        """Register all 80 query definitions (20 per type)."""

        # ==================================================================
        # SELECT QUERIES  (S1-S6 simple, M1-M8 medium, C1-C6 complex = 20)
        # ==================================================================

        # --- Simple SELECT (6) ---
        self.queries["S1"] = QueryDefinition(
            query_id="S1",
            name="Select Customer by ID",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description="Get customer by ID - single partition lookup",
            method_name="select_customer_by_id",
            params_generator=lambda: {"customer_id": 1},
        )

        self.queries["S2"] = QueryDefinition(
            query_id="S2",
            name="Select Customer Account by ID",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description="Get customer account by ID - single partition lookup",
            method_name="select_account_by_id",
            params_generator=lambda: {"account_id": 1},
        )

        self.queries["S3"] = QueryDefinition(
            query_id="S3",
            name="Select Broker by ID",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description="Get broker by ID - single partition lookup",
            method_name="select_broker_by_id",
            params_generator=lambda: {"broker_id": 1},
        )

        self.queries["S4"] = QueryDefinition(
            query_id="S4",
            name="Select Security by Symbol",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description="Get security by symbol - single partition lookup",
            method_name="select_security_by_symbol",
            params_generator=lambda: {"symbol": "S00001"},
        )

        self.queries["S5"] = QueryDefinition(
            query_id="S5",
            name="Select Last Trade by Symbol",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description="Get last trade price by symbol - single partition lookup",
            method_name="select_last_trade_by_symbol",
            params_generator=lambda: {"symbol": "S00001"},
        )

        self.queries["S6"] = QueryDefinition(
            query_id="S6",
            name="Select Status Type by ID",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description="Get status type by ID - single partition lookup",
            method_name="select_status_type_by_id",
            params_generator=lambda: {"status_id": "ACTV"},
        )

        # --- Medium SELECT (8) ---
        self.queries["M1"] = QueryDefinition(
            query_id="M1",
            name="Select Trades by Account",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description="Get recent trades for account - denormalized table scan",
            method_name="select_trades_by_account",
            params_generator=lambda: {"account_id": 1, "limit": 20},
        )

        self.queries["M2"] = QueryDefinition(
            query_id="M2",
            name="Select Holdings by Account",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description="Get all holdings for account - multi-row partition query",
            method_name="select_holdings_by_account",
            params_generator=lambda: {"account_id": 1, "limit": 50},
        )

        self.queries["M3"] = QueryDefinition(
            query_id="M3",
            name="Select Watch Items by Watch List",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description="Get all watch items in a watch list",
            method_name="select_watch_items_by_watchlist",
            params_generator=lambda: {"watchlist_id": 1},
        )

        self.queries["M4"] = QueryDefinition(
            query_id="M4",
            name="Select Daily Market Range",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description="Get daily market data for symbol over date range",
            method_name="select_daily_market_range",
            params_generator=lambda: {
                "symbol": "S00001",
                "start_date": date(2024, 1, 1),
                "end_date": date(2024, 3, 31),
            },
        )

        self.queries["M5"] = QueryDefinition(
            query_id="M5",
            name="Select Companies by Industry",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description="Get companies in a given industry",
            method_name="select_companies_by_industry",
            params_generator=lambda: {"industry_id": "IN01", "limit": 20},
        )

        self.queries["M6"] = QueryDefinition(
            query_id="M6",
            name="Select Financial by Company",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description="Get financial records for a company",
            method_name="select_financial_by_company",
            params_generator=lambda: {"company_id": 1, "limit": 8},
        )

        self.queries["M7"] = QueryDefinition(
            query_id="M7",
            name="Select Holding Summary by Account",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description="Get holding summary for account - multi-row partition query",
            method_name="select_holding_summary_by_account",
            params_generator=lambda: {"account_id": 1},
        )

        self.queries["M8"] = QueryDefinition(
            query_id="M8",
            name="Select News by Company",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description="Get recent news articles for company - denormalized table",
            method_name="select_news_by_company",
            params_generator=lambda: {"company_id": 1, "limit": 10},
        )

        # --- Complex SELECT (6) ---
        self.queries["C1"] = QueryDefinition(
            query_id="C1",
            name="Select Trades by Symbol with Date Range",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.COMPLEX,
            description="Get trades for symbol within date range - clustering key range",
            method_name="select_trades_by_symbol_date_range",
            params_generator=lambda: {
                "symbol": "S00001",
                "start_dts": datetime(2024, 1, 1),
                "end_dts": datetime(2024, 3, 31),
                "limit": 50,
            },
        )

        self.queries["C2"] = QueryDefinition(
            query_id="C2",
            name="Select Customer by Name",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.COMPLEX,
            description="Lookup customers by last name across partitions",
            method_name="select_customer_by_name",
            params_generator=lambda: {"last_name": "Smith", "limit": 20},
        )

        self.queries["C3"] = QueryDefinition(
            query_id="C3",
            name="Select Active Trades with Filter",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.COMPLEX,
            description="Filter active trades by status using secondary index",
            method_name="select_active_trades_with_filter",
            params_generator=lambda: {"account_id": 1, "status_id": "ACTV", "limit": 20},
        )

        self.queries["C4"] = QueryDefinition(
            query_id="C4",
            name="Select Market Feed Latest",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.COMPLEX,
            description="Get latest market feed entries - TTL table query",
            method_name="select_market_feed_latest",
            params_generator=lambda: {"symbol": "S00001", "limit": 10},
        )

        self.queries["C5"] = QueryDefinition(
            query_id="C5",
            name="Select Portfolio Value Calculation",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.COMPLEX,
            description="Calculate portfolio value from holdings and last trade price",
            method_name="select_portfolio_value",
            params_generator=lambda: {"account_id": 1},
        )

        self.queries["C6"] = QueryDefinition(
            query_id="C6",
            name="Select Broker Performance",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.COMPLEX,
            description="Get broker performance metrics from counter table",
            method_name="select_broker_performance",
            params_generator=lambda: {"broker_id": 1},
        )

        # ==================================================================
        # INSERT QUERIES  (I1-I5 simple, I6-I14 medium, I15-I20 complex = 20)
        # ==================================================================

        # --- Simple INSERT (5) ---
        self.queries["I1"] = QueryDefinition(
            query_id="I1",
            name="Insert Customer",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.SIMPLE,
            description="Insert new customer record",
            method_name="insert_customer",
            params_generator=lambda: {
                "customer_id": 9_000_001,
                "tax_id": "TAXID9001",
                "status_id": "ACTV",
                "last_name": "New",
                "first_name": "Customer",
                "middle": "T",
                "gender": "M",
                "tier": 1,
                "dob": date(1980, 1, 1),
                "address_id": 1,
                "email1": "new@example.com",
                "email2": "",
            },
        )

        self.queries["I2"] = QueryDefinition(
            query_id="I2",
            name="Insert Customer Account",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.SIMPLE,
            description="Insert new customer account",
            method_name="insert_customer_account",
            params_generator=lambda: {
                "account_id": 9_000_001,
                "broker_id": 1,
                "customer_id": 1,
                "name": "Test Account",
                "tax_status": 1,
                "balance": 10000.0,
            },
        )

        self.queries["I3"] = QueryDefinition(
            query_id="I3",
            name="Insert Trade",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.SIMPLE,
            description="Insert new trade record",
            method_name="insert_trade",
            params_generator=lambda: {
                "trade_id": 9_000_001,
                "dts": datetime.now(),
                "status_id": "ACTV",
                "trade_type_id": "TMB",
                "is_cash": True,
                "symbol": "S00001",
                "qty": 100,
                "bid_price": 45.0,
                "account_id": 1,
                "exec_name": "ExecA",
                "trade_price": 45.0,
                "charge": 10.0,
                "commission": 5.0,
                "tax": 2.0,
                "lifo": False,
            },
        )

        self.queries["I4"] = QueryDefinition(
            query_id="I4",
            name="Insert Holding",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.SIMPLE,
            description="Insert new holding record",
            method_name="insert_holding",
            params_generator=lambda: {
                "account_id": 1,
                "symbol": "S00001",
                "dts": datetime.now(),
                "trade_id": 9_000_001,
                "price": 45.0,
                "qty": 100,
            },
        )

        self.queries["I5"] = QueryDefinition(
            query_id="I5",
            name="Insert Watch Item",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.SIMPLE,
            description="Insert new watch item",
            method_name="insert_watch_item",
            params_generator=lambda: {"watchlist_id": 1, "symbol": "S00999"},
        )

        # --- Medium INSERT (9) ---
        self.queries["I6"] = QueryDefinition(
            query_id="I6",
            name="Insert Trade with TTL",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="Insert trade activity record with TTL",
            method_name="insert_trade_with_ttl",
            params_generator=lambda: {
                "symbol": "S00001",
                "dts": datetime.now(),
                "price": 45.0,
                "vol": 10000,
                "ttl_seconds": 86400,
            },
        )

        self.queries["I7"] = QueryDefinition(
            query_id="I7",
            name="Insert Trade History Batch",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="Batch insert multiple trade history records",
            method_name="insert_trade_history_batch",
            params_generator=lambda: {
                "trade_id": 1,
                "history_entries": [
                    {"status_id": "SBMT", "dts": datetime.now()},
                    {"status_id": "PNDG", "dts": datetime.now()},
                    {"status_id": "COMP", "dts": datetime.now()},
                ],
            },
        )

        self.queries["I8"] = QueryDefinition(
            query_id="I8",
            name="Insert Holding with Collections",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="Insert trade_extended record with collections",
            method_name="insert_holding_with_collections",
            params_generator=lambda: {
                "trade_id": 9_000_002,
                "tags": {"equity", "growth"},
                "notes": ["Bought at market open"],
                "attributes": {"strategy": "momentum", "sector": "tech"},
            },
        )

        self.queries["I9"] = QueryDefinition(
            query_id="I9",
            name="Insert Market Feed TTL",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="Insert market feed entry with TTL",
            method_name="insert_market_feed_ttl",
            params_generator=lambda: {
                "symbol": "S00001",
                "dts": datetime.now(),
                "price": 45.25,
                "vol": 5000,
                "ttl_seconds": 3600,
            },
        )

        self.queries["I10"] = QueryDefinition(
            query_id="I10",
            name="Insert Trade Extended with Collections",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="Insert trade with all collection types",
            method_name="insert_trade_extended_with_collections",
            params_generator=lambda: {
                "trade_id": 9_000_003,
                "tags": {"options", "hedge"},
                "notes": ["Initial entry"],
                "attributes": {"hedge_ratio": "0.5"},
                "created": datetime.now(),
            },
        )

        self.queries["I11"] = QueryDefinition(
            query_id="I11",
            name="Insert Customer Extended with UDT",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="Insert customer_extended record with UDT address",
            method_name="insert_customer_extended_with_udt",
            params_generator=lambda: {
                "customer_id": 9_000_002,
                "phone_numbers": {"555-1111"},
                "email_history": ["old@example.com"],
                "preferences": {"theme": "light"},
                "tags": {"retail"},
                "notes": ["New customer"],
                "created": datetime.now(),
            },
        )

        self.queries["I12"] = QueryDefinition(
            query_id="I12",
            name="Insert Batch Trades",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="Batch insert multiple trade records",
            method_name="insert_batch_trades",
            params_generator=lambda: {
                "trades": [
                    {
                        "trade_id": 9_000_010,
                        "account_id": 1,
                        "symbol": "S00001",
                        "qty": 50,
                        "price": 45.0,
                        "dts": datetime.now(),
                    },
                    {
                        "trade_id": 9_000_011,
                        "account_id": 1,
                        "symbol": "S00002",
                        "qty": 25,
                        "price": 120.0,
                        "dts": datetime.now(),
                    },
                ]
            },
        )

        self.queries["I13"] = QueryDefinition(
            query_id="I13",
            name="Insert with Timestamp",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="Insert with custom USING TIMESTAMP",
            method_name="insert_with_timestamp",
            params_generator=lambda: {
                "symbol": "S00001",
                "dts": datetime.now(),
                "price": 44.75,
                "vol": 2500,
                "timestamp_micros": 1_700_000_000_000_000,
            },
        )

        self.queries["I14"] = QueryDefinition(
            query_id="I14",
            name="Insert Account Activity JSON",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="Insert account activity with JSON",
            method_name="insert_account_activity_json",
            params_generator=lambda: {
                "account_id": 1,
                "activity_type": "trade",
                "count_increment": 1,
            },
        )

        # --- Complex INSERT (6) ---
        self.queries["I15"] = QueryDefinition(
            query_id="I15",
            name="Insert Trade LWT",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.COMPLEX,
            description="Insert trade with IF NOT EXISTS (LWT)",
            method_name="insert_trade_lwt",
            params_generator=lambda: {
                "trade_id": 9_000_099,
                "dts": datetime.now(),
                "status_id": "ACTV",
                "trade_type_id": "TMB",
                "is_cash": True,
                "symbol": "S00001",
                "qty": 200,
                "bid_price": 45.5,
                "account_id": 1,
                "exec_name": "LWT_Exec",
                "trade_price": 45.5,
                "charge": 10.0,
                "commission": 5.0,
                "tax": 2.0,
                "lifo": False,
            },
        )

        self.queries["I16"] = QueryDefinition(
            query_id="I16",
            name="Insert Customer Denormalization Multi-Table",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.COMPLEX,
            description="Insert customer into multiple denormalized tables",
            method_name="insert_customer_denorm_multi_table",
            params_generator=lambda: {
                "customer_id": 9_000_003,
                "tax_id": "TAXID9003",
                "status_id": "ACTV",
                "last_name": "Multi",
                "first_name": "Table",
                "middle": "X",
                "gender": "F",
                "tier": 2,
                "dob": date(1990, 6, 15),
                "address_id": 2,
                "email1": "multi@example.com",
                "email2": "multi2@example.com",
            },
        )

        self.queries["I17"] = QueryDefinition(
            query_id="I17",
            name="Insert Portfolio Snapshot Static",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.COMPLEX,
            description="Insert portfolio snapshot with static columns",
            method_name="insert_portfolio_snapshot_static",
            params_generator=lambda: {
                "account_id": 1,
                "snap_date": date.today(),
                "snap_time": datetime.now(),
                "account_name": "Main Portfolio",
                "account_bal": 125000.0,
                "symbol": "S00001",
                "position_qty": 100,
                "position_val": 4500.0,
            },
        )

        self.queries["I18"] = QueryDefinition(
            query_id="I18",
            name="Insert Trade All Collections",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.COMPLEX,
            description="Insert trade with full set/list/map/UDT collections",
            method_name="insert_trade_all_collections",
            params_generator=lambda: {
                "trade_id": 9_000_004,
                "tags": {"options", "covered", "short"},
                "notes": ["Day trade", "Pre-market"],
                "attributes": {"strategy": "covered_call", "expiry": "2024-12-20"},
                "created": datetime.now(),
                "updated": datetime.now(),
            },
        )

        self.queries["I19"] = QueryDefinition(
            query_id="I19",
            name="Insert with LWT Condition",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.COMPLEX,
            description="Insert with IF NOT EXISTS LWT condition",
            method_name="insert_with_lwt_condition",
            params_generator=lambda: {"watchlist_id": 9_000_001, "customer_id": 1},
        )

        self.queries["I20"] = QueryDefinition(
            query_id="I20",
            name="Insert Multiple Tables Batch",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.COMPLEX,
            description="Insert trade into multiple tables via LOGGED BATCH",
            method_name="insert_multiple_tables_batch",
            params_generator=lambda: {
                "trade_id": 9_000_005,
                "dts": datetime.now(),
                "status_id": "SBMT",
                "trade_type_id": "TLB",
                "is_cash": False,
                "symbol": "S00002",
                "qty": 300,
                "bid_price": 120.0,
                "account_id": 2,
                "exec_name": "BatchExec",
                "trade_price": 119.5,
                "charge": 12.0,
                "commission": 8.0,
                "tax": 3.0,
                "lifo": True,
            },
        )

        # ==================================================================
        # UPDATE QUERIES  (U1-U4 simple, U5-U14 medium, U15-U20 complex = 20)
        # ==================================================================

        # --- Simple UPDATE (4) ---
        self.queries["U1"] = QueryDefinition(
            query_id="U1",
            name="Update Account Balance",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.SIMPLE,
            description="Update customer account balance",
            method_name="update_account_balance",
            params_generator=lambda: {"account_id": 1, "new_balance": 50000.0},
        )

        self.queries["U2"] = QueryDefinition(
            query_id="U2",
            name="Update Broker Commission Total",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.SIMPLE,
            description="Update broker commission total",
            method_name="update_broker_commission",
            params_generator=lambda: {"broker_id": 1, "new_comm_total": 250000.0},
        )

        self.queries["U3"] = QueryDefinition(
            query_id="U3",
            name="Update Holding Summary Quantity",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.SIMPLE,
            description="Update holding summary quantity",
            method_name="update_holding_summary_qty",
            params_generator=lambda: {"account_id": 1, "symbol": "S00001", "new_qty": 500},
        )

        self.queries["U4"] = QueryDefinition(
            query_id="U4",
            name="Update Last Trade Price",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.SIMPLE,
            description="Update last trade price and volume",
            method_name="update_last_trade",
            params_generator=lambda: {
                "symbol": "S00001",
                "dts": datetime.now(),
                "price": 46.25,
                "open_price": 45.0,
                "vol": 15000,
            },
        )

        # --- Medium UPDATE (10) ---
        self.queries["U5"] = QueryDefinition(
            query_id="U5",
            name="Update Account Balance with Condition",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Update account balance with IF condition (LWT)",
            method_name="update_account_balance_conditional",
            params_generator=lambda: {
                "account_id": 1,
                "new_balance": 60000.0,
                "expected_balance": 50000.0,
            },
        )

        self.queries["U6"] = QueryDefinition(
            query_id="U6",
            name="Update Holdings Batch",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Batch update multiple holding summary records",
            method_name="update_holdings_batch",
            params_generator=lambda: {
                "updates": [
                    {"account_id": 1, "symbol": "S00001", "qty": 200},
                    {"account_id": 1, "symbol": "S00002", "qty": 100},
                ]
            },
        )

        self.queries["U7"] = QueryDefinition(
            query_id="U7",
            name="Update Customer Email Collections",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Add email to customer_extended email_history list",
            method_name="update_customer_email_collections",
            params_generator=lambda: {"customer_id": 1, "new_email": "updated@example.com"},
        )

        self.queries["U8"] = QueryDefinition(
            query_id="U8",
            name="Update Customer Preferences Map",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Update preferences map in customer_extended",
            method_name="update_customer_preferences_map",
            params_generator=lambda: {
                "customer_id": 1,
                "prefs_update": {"theme": "dark", "lang": "en"},
            },
        )

        self.queries["U9"] = QueryDefinition(
            query_id="U9",
            name="Update Trade Status LWT",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Update trade status with LWT IF condition",
            method_name="update_trade_status_lwt",
            params_generator=lambda: {
                "trade_id": 1,
                "new_status": "COMP",
                "expected_status": "PNDG",
            },
        )

        self.queries["U10"] = QueryDefinition(
            query_id="U10",
            name="Update Multiple Customer Account Fields",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Update multiple fields on customer account",
            method_name="update_multiple_account_fields",
            params_generator=lambda: {"account_id": 1, "name": "Updated Account", "tax_status": 2},
        )

        self.queries["U11"] = QueryDefinition(
            query_id="U11",
            name="Update Market Feed with TTL",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Update market feed entry with USING TTL",
            method_name="update_market_feed_ttl",
            params_generator=lambda: {
                "symbol": "S00001",
                "dts": datetime.now(),
                "price": 47.0,
                "ttl_seconds": 7200,
            },
        )

        self.queries["U12"] = QueryDefinition(
            query_id="U12",
            name="Update Broker Stats Counter",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Increment broker metrics counter",
            method_name="update_broker_stats_counter",
            params_generator=lambda: {
                "broker_id": 1,
                "metric_name": "total_trades",
                "increment": 1,
            },
        )

        self.queries["U13"] = QueryDefinition(
            query_id="U13",
            name="Update Trade with Custom Timestamp",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Update trade field with USING TIMESTAMP",
            method_name="update_trade_with_timestamp",
            params_generator=lambda: {
                "trade_id": 1,
                "exec_name": "NewExec",
                "timestamp_micros": 1_700_000_000_000_000,
            },
        )

        self.queries["U14"] = QueryDefinition(
            query_id="U14",
            name="Update Portfolio Snapshot Static Column",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Update static column in portfolio_snapshot",
            method_name="update_portfolio_snapshot_static",
            params_generator=lambda: {
                "account_id": 1,
                "account_name": "Premium Portfolio",
                "account_bal": 130000.0,
            },
        )

        # --- Complex UPDATE (6) ---
        self.queries["U15"] = QueryDefinition(
            query_id="U15",
            name="Update Trade LWT Complex",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.COMPLEX,
            description="Complex LWT update with multiple IF conditions on trade",
            method_name="update_trade_lwt_complex",
            params_generator=lambda: {
                "trade_id": 1,
                "new_status": "COMP",
                "expected_status": "PNDG",
                "expected_type": "TMB",
            },
        )

        self.queries["U16"] = QueryDefinition(
            query_id="U16",
            name="Update Account and Holding Batch",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.COMPLEX,
            description="LOGGED BATCH update account balance and holding summary",
            method_name="update_account_and_holding_batch",
            params_generator=lambda: {
                "account_id": 1,
                "new_balance": 55000.0,
                "symbol": "S00001",
                "new_qty": 150,
            },
        )

        self.queries["U17"] = QueryDefinition(
            query_id="U17",
            name="Update Collection with TTL",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.COMPLEX,
            description="Update set collection with TTL on trade_extended",
            method_name="update_collection_with_ttl",
            params_generator=lambda: {"trade_id": 1, "tag": "reviewed", "ttl_seconds": 86400},
        )

        self.queries["U18"] = QueryDefinition(
            query_id="U18",
            name="Update LWT Multiple Conditions",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.COMPLEX,
            description="LWT update with multiple IF conditions on account",
            method_name="update_lwt_multiple_conditions",
            params_generator=lambda: {
                "account_id": 1,
                "new_balance": 70000.0,
                "expected_balance": 55000.0,
                "expected_tax_st": 1,
            },
        )

        self.queries["U19"] = QueryDefinition(
            query_id="U19",
            name="Update Unlogged Batch",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.COMPLEX,
            description="UNLOGGED BATCH update of last trade records",
            method_name="update_unlogged_batch",
            params_generator=lambda: {
                "updates": [
                    {"symbol": "S00001", "price": 46.0, "vol": 20000, "dts": datetime.now()},
                    {"symbol": "S00002", "price": 121.0, "vol": 5000, "dts": datetime.now()},
                ]
            },
        )

        self.queries["U20"] = QueryDefinition(
            query_id="U20",
            name="Update Counter Columns",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.COMPLEX,
            description="Update multiple counter columns in account_activity",
            method_name="update_counter_columns",
            params_generator=lambda: {
                "account_id": 1,
                "activity_updates": [
                    {"activity_type": "trade", "increment": 1},
                    {"activity_type": "order", "increment": 1},
                ],
            },
        )

        # ==================================================================
        # DELETE QUERIES  (D1-D3 simple, D4-D11 medium, D12-D20 complex = 20)
        # ==================================================================

        # --- Simple DELETE (3) ---
        self.queries["D1"] = QueryDefinition(
            query_id="D1",
            name="Delete Watch Item",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.SIMPLE,
            description="Delete a single watch item",
            method_name="delete_watch_item",
            params_generator=lambda: {"watchlist_id": 1, "symbol": "S00999"},
        )

        self.queries["D2"] = QueryDefinition(
            query_id="D2",
            name="Delete Holding",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.SIMPLE,
            description="Delete a holding record",
            method_name="delete_holding",
            params_generator=lambda: {
                "account_id": 1,
                "symbol": "S00001",
                "dts": datetime(2024, 1, 1),
                "trade_id": 9_000_001,
            },
        )

        self.queries["D3"] = QueryDefinition(
            query_id="D3",
            name="Delete Specific Column",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.SIMPLE,
            description="Delete a specific column value (set to null)",
            method_name="delete_specific_column",
            params_generator=lambda: {"account_id": 1, "column": "ca_name"},
        )

        # --- Medium DELETE (8) ---
        self.queries["D4"] = QueryDefinition(
            query_id="D4",
            name="Delete Watch Item Conditional",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description="Delete watch item with IF EXISTS (LWT)",
            method_name="delete_watch_item_conditional",
            params_generator=lambda: {"watchlist_id": 1, "symbol": "S00998"},
        )

        self.queries["D5"] = QueryDefinition(
            query_id="D5",
            name="Delete Old Market Feed Records",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description="Delete expired market feed entries for symbol",
            method_name="delete_old_market_feed",
            params_generator=lambda: {"symbol": "S00001", "cutoff_dts": datetime(2023, 1, 1)},
        )

        self.queries["D6"] = QueryDefinition(
            query_id="D6",
            name="Delete Set Element",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description="Remove an element from a set collection",
            method_name="delete_set_element",
            params_generator=lambda: {"trade_id": 1, "tag": "reviewed"},
        )

        self.queries["D7"] = QueryDefinition(
            query_id="D7",
            name="Delete Map Key",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description="Remove a key from a map collection",
            method_name="delete_map_key",
            params_generator=lambda: {"customer_id": 1, "pref_key": "theme"},
        )

        self.queries["D8"] = QueryDefinition(
            query_id="D8",
            name="Delete List Index",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description="Remove an element from a list by index",
            method_name="delete_list_index",
            params_generator=lambda: {"customer_id": 1, "index": 0},
        )

        self.queries["D9"] = QueryDefinition(
            query_id="D9",
            name="Delete with Timestamp",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description="Delete trade record with USING TIMESTAMP",
            method_name="delete_with_timestamp",
            params_generator=lambda: {
                "trade_id": 9_000_001,
                "timestamp_micros": 1_700_000_000_000_000,
            },
        )

        self.queries["D10"] = QueryDefinition(
            query_id="D10",
            name="Delete Static Column",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description="Delete static column from portfolio_snapshot",
            method_name="delete_static_column",
            params_generator=lambda: {"account_id": 9_000_099},
        )

        self.queries["D11"] = QueryDefinition(
            query_id="D11",
            name="Delete Expired Records TTL",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description="Delete all market feed records for a symbol (simulates TTL cleanup)",
            method_name="delete_expired_records_ttl",
            params_generator=lambda: {"symbol": "S99999"},
        )

        # --- Complex DELETE (9) ---
        self.queries["D12"] = QueryDefinition(
            query_id="D12",
            name="Delete All Holdings for Account",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="Delete all holdings for an account (partition delete)",
            method_name="delete_all_holdings_for_account",
            params_generator=lambda: {"account_id": 9_000_099},
        )

        self.queries["D13"] = QueryDefinition(
            query_id="D13",
            name="Delete Trade with History Batch",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="LOGGED BATCH delete trade and its history",
            method_name="delete_trade_with_history_batch",
            params_generator=lambda: {"trade_id": 9_000_001},
        )

        self.queries["D14"] = QueryDefinition(
            query_id="D14",
            name="Delete Batch Watch Items",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="Batch delete multiple watch items",
            method_name="delete_batch_watch_items",
            params_generator=lambda: {"watchlist_id": 1, "symbols": ["S09997", "S09998", "S09999"]},
        )

        self.queries["D15"] = QueryDefinition(
            query_id="D15",
            name="Delete Clustering Range",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="Delete trade history within a time range",
            method_name="delete_clustering_range",
            params_generator=lambda: {
                "trade_id": 1,
                "start_dts": datetime(2023, 1, 1),
                "end_dts": datetime(2023, 12, 31),
            },
        )

        self.queries["D16"] = QueryDefinition(
            query_id="D16",
            name="Delete with IN Clause",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="Delete multiple watch items with IN clause",
            method_name="delete_with_in_clause",
            params_generator=lambda: {"watchlist_id": 1, "symbols": ["S09991", "S09992", "S09993"]},
        )

        self.queries["D17"] = QueryDefinition(
            query_id="D17",
            name="Delete LWT Condition",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="Delete watch list with IF EXISTS (LWT)",
            method_name="delete_lwt_condition",
            params_generator=lambda: {"watchlist_id": 9_000_001},
        )

        self.queries["D18"] = QueryDefinition(
            query_id="D18",
            name="Delete LOGGED Batch",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="LOGGED BATCH delete across multiple tables",
            method_name="delete_batch_logged",
            params_generator=lambda: {
                "deletes": [
                    {"table": "watch_item", "wl_id": 1, "symbol": "S09994"},
                    {"table": "watch_item", "wl_id": 1, "symbol": "S09995"},
                ]
            },
        )

        self.queries["D19"] = QueryDefinition(
            query_id="D19",
            name="Delete UNLOGGED Batch",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="UNLOGGED BATCH delete of market feed entries",
            method_name="delete_batch_unlogged",
            params_generator=lambda: {
                "symbol": "S99998",
                "dts_list": [datetime(2023, 6, 1), datetime(2023, 6, 2)],
            },
        )

        self.queries["D20"] = QueryDefinition(
            query_id="D20",
            name="Delete Partition",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="Delete entire partition from trade_by_account",
            method_name="delete_partition",
            params_generator=lambda: {"account_id": 9_000_099},
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_query(self, query_id: str) -> QueryDefinition:
        """Get query definition by ID."""
        return self.queries.get(query_id)

    def get_queries_by_type(self, query_type: QueryType) -> List[QueryDefinition]:
        """Get all queries of a specific type."""
        return [q for q in self.queries.values() if q.query_type == query_type]

    def get_queries_by_complexity(self, complexity: ComplexityLevel) -> List[QueryDefinition]:
        """Get all queries of a specific complexity."""
        return [q for q in self.queries.values() if q.complexity == complexity]

    def get_all_queries(self) -> List[QueryDefinition]:
        """Get all query definitions."""
        return list(self.queries.values())

    def get_query_count_by_type(self) -> Dict[str, int]:
        """Get count of queries by type."""
        counts = {qt.value: 0 for qt in QueryType}
        for query in self.queries.values():
            counts[query.query_type.value] += 1
        return counts

    def get_query_count_by_complexity(self) -> Dict[str, int]:
        """Get count of queries by complexity."""
        counts = {cl.value: 0 for cl in ComplexityLevel}
        for query in self.queries.values():
            counts[query.complexity.value] += 1
        return counts

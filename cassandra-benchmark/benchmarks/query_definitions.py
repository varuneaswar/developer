"""
Query definitions for TPC-C benchmark.
Central registry of all queries with metadata.
"""

from dataclasses import dataclass
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
    """Central registry of all TPC-C benchmark queries."""

    def __init__(self):
        """Initialize query definitions."""
        self.queries: Dict[str, QueryDefinition] = {}
        self._register_queries()

    def _register_queries(self) -> None:
        """Register all query definitions."""

        # ========== SELECT QUERIES ==========

        # Simple SELECT queries
        self.queries["S1"] = QueryDefinition(
            query_id="S1",
            name="Select Warehouse by ID",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description="Get warehouse by ID - single partition lookup",
            method_name="select_warehouse_by_id",
            params_generator=lambda: {"warehouse_id": 1},
        )

        self.queries["S2"] = QueryDefinition(
            query_id="S2",
            name="Select Customer by ID",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description="Get customer by ID - single partition lookup",
            method_name="select_customer_by_id",
            params_generator=lambda: {"warehouse_id": 1, "district_id": 1, "customer_id": 100},
        )

        self.queries["S3"] = QueryDefinition(
            query_id="S3",
            name="Select Item by ID",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description="Get item by ID - single partition lookup",
            method_name="select_item_by_id",
            params_generator=lambda: {"item_id": 1000},
        )

        self.queries["S4"] = QueryDefinition(
            query_id="S4",
            name="Select District by ID",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description="Get district by ID - single partition lookup",
            method_name="select_district_by_id",
            params_generator=lambda: {"warehouse_id": 1, "district_id": 1},
        )

        # Medium SELECT queries
        self.queries["M1"] = QueryDefinition(
            query_id="M1",
            name="Select Customers by District",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description="Get customers in a district - multi-row partition query",
            method_name="select_customers_by_district",
            params_generator=lambda: {"warehouse_id": 1, "district_id": 1, "limit": 100},
        )

        self.queries["M2"] = QueryDefinition(
            query_id="M2",
            name="Select Stock Level",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description="Get stock level for item - business logic query",
            method_name="select_stock_level",
            params_generator=lambda: {"warehouse_id": 1, "item_id": 1000},
        )

        self.queries["M3"] = QueryDefinition(
            query_id="M3",
            name="Select Orders by Customer",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description="Get recent orders for customer - ordered multi-row query",
            method_name="select_orders_by_customer",
            params_generator=lambda: {
                "customer_id": 100,
                "warehouse_id": 1,
                "district_id": 1,
                "limit": 20,
            },
        )

        self.queries["M4"] = QueryDefinition(
            query_id="M4",
            name="Select Order Lines",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description="Get all order lines for order - multi-row partition query",
            method_name="select_order_lines",
            params_generator=lambda: {"warehouse_id": 1, "district_id": 1, "order_id": 1000},
        )

        # Complex SELECT queries
        self.queries["C1"] = QueryDefinition(
            query_id="C1",
            name="Select Customers by Name",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.COMPLEX,
            description="Get customers by last name - denormalized table query",
            method_name="select_customers_by_name",
            params_generator=lambda: {"warehouse_id": 1, "district_id": 1, "last_name": "SMITH"},
        )

        self.queries["C2"] = QueryDefinition(
            query_id="C2",
            name="Select New Orders",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.COMPLEX,
            description="Get new orders for district - multi-row query",
            method_name="select_new_orders",
            params_generator=lambda: {"warehouse_id": 1, "district_id": 1, "limit": 20},
        )

        self.queries["C3"] = QueryDefinition(
            query_id="C3",
            name="Select History by Date Range",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.COMPLEX,
            description="Get history in date range - time-series range query",
            method_name="select_history_by_date_range",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "date_bucket": "2024-01-01",
                "start_date": "2024-01-01 00:00:00",
                "end_date": "2024-01-01 23:59:59",
            },
        )

        # ========== INSERT QUERIES ==========

        # Note: Insert/Update/Delete queries will use mock data generators
        # The actual implementations would need proper data generation

        self.queries["I1"] = QueryDefinition(
            query_id="I1",
            name="Insert Customer",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.SIMPLE,
            description="Insert new customer - basic insert",
            method_name="insert_customer",
            params_generator=lambda: {"data": {}},  # Placeholder
        )

        self.queries["I2"] = QueryDefinition(
            query_id="I2",
            name="Insert Order",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.SIMPLE,
            description="Insert new order - basic insert with denormalization",
            method_name="insert_order",
            params_generator=lambda: {"data": {}},  # Placeholder
        )

        self.queries["I3"] = QueryDefinition(
            query_id="I3",
            name="Insert History",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.SIMPLE,
            description="Insert history record - time-series insert",
            method_name="insert_history",
            params_generator=lambda: {"data": {}},  # Placeholder
        )

        self.queries["I4"] = QueryDefinition(
            query_id="I4",
            name="Batch Insert Order Lines",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="Batch insert order lines",
            method_name="insert_order_lines_batch",
            params_generator=lambda: {"order_lines": []},  # Placeholder
        )

        self.queries["I5"] = QueryDefinition(
            query_id="I5",
            name="Insert History with TTL",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="Insert history with TTL",
            method_name="insert_history_with_ttl",
            params_generator=lambda: {"data": {}, "ttl_seconds": 86400},  # Placeholder
        )

        self.queries["I6"] = QueryDefinition(
            query_id="I6",
            name="Insert New Order (LWT)",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.COMPLEX,
            description="Insert new order with LWT",
            method_name="insert_new_order_lwt",
            params_generator=lambda: {"warehouse_id": 1, "district_id": 1, "order_id": 9999},
        )

        self.queries["I7"] = QueryDefinition(
            query_id="I7",
            name="Insert Customer with Denormalization",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.COMPLEX,
            description="Insert with denormalization",
            method_name="insert_customer_with_denormalization",
            params_generator=lambda: {"data": {}},
        )

        # ========== UPDATE QUERIES ==========

        self.queries["U1"] = QueryDefinition(
            query_id="U1",
            name="Update Customer Balance",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.SIMPLE,
            description="Update customer balance - basic update",
            method_name="update_customer_balance",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "customer_id": 100,
                "balance": 1000.0,
                "ytd_payment": 500.0,
                "payment_cnt": 5,
            },
        )

        self.queries["U2"] = QueryDefinition(
            query_id="U2",
            name="Update Stock Quantity",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.SIMPLE,
            description="Update stock quantity - basic update",
            method_name="update_stock_quantity",
            params_generator=lambda: {
                "warehouse_id": 1,
                "item_id": 1000,
                "quantity": 50,
                "ytd": 1000,
                "order_cnt": 100,
            },
        )

        self.queries["U3"] = QueryDefinition(
            query_id="U3",
            name="Update District Next Order",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.SIMPLE,
            description="Update district next order ID",
            method_name="update_district_next_order",
            params_generator=lambda: {"warehouse_id": 1, "district_id": 1, "next_order_id": 3001},
        )

        self.queries["U4"] = QueryDefinition(
            query_id="U4",
            name="Update Order Carrier (Conditional)",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Update order carrier conditionally",
            method_name="update_order_carrier_conditional",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "order_id": 1000,
                "carrier_id": 5,
            },
        )

        self.queries["U5"] = QueryDefinition(
            query_id="U5",
            name="Update Stock with LWT",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.COMPLEX,
            description="Update stock with LWT validation",
            method_name="update_stock_with_lwt",
            params_generator=lambda: {
                "warehouse_id": 1,
                "item_id": 1000,
                "new_quantity": 40,
                "ytd": 1100,
                "order_cnt": 101,
                "remote_cnt": 10,
                "min_quantity": 10,
            },
        )

        self.queries["U6"] = QueryDefinition(
            query_id="U6",
            name="Update Stocks Batch",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Batch update stocks",
            method_name="update_stocks_batch",
            params_generator=lambda: {"updates": []},
        )

        self.queries["U7"] = QueryDefinition(
            query_id="U7",
            name="Update Customer Credit Conditional",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Conditional credit update",
            method_name="update_customer_credit_conditional",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "customer_id": 100,
                "new_credit": "BC",
                "credit_data": "Updated",
                "expected_credit": "GC",
            },
        )

        self.queries["U8"] = QueryDefinition(
            query_id="U8",
            name="Update Order and Customer Batch",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.COMPLEX,
            description="Multi-table batch update",
            method_name="update_order_and_customer_batch",
            params_generator=lambda: {"order_update": {}, "customer_update": {}},
        )

        # ========== DELETE QUERIES ==========

        self.queries["D1"] = QueryDefinition(
            query_id="D1",
            name="Delete Order Line",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.SIMPLE,
            description="Delete specific order line",
            method_name="delete_order_line",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "order_id": 1000,
                "line_number": 1,
            },
        )

        self.queries["D2"] = QueryDefinition(
            query_id="D2",
            name="Delete New Order",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.SIMPLE,
            description="Delete new order record",
            method_name="delete_new_order",
            params_generator=lambda: {"warehouse_id": 1, "district_id": 1, "order_id": 9999},
        )

        self.queries["D3"] = QueryDefinition(
            query_id="D3",
            name="Delete New Order (Conditional)",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description="Delete new order with IF EXISTS",
            method_name="delete_new_order_conditional",
            params_generator=lambda: {"warehouse_id": 1, "district_id": 1, "order_id": 9999},
        )

        self.queries["D4"] = QueryDefinition(
            query_id="D4",
            name="Delete All Order Lines",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="Delete all order lines for an order",
            method_name="delete_all_order_lines",
            params_generator=lambda: {"warehouse_id": 1, "district_id": 1, "order_id": 1000},
        )

        self.queries["D5"] = QueryDefinition(
            query_id="D5",
            name="Delete Old History Records",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description="Delete old history records",
            method_name="delete_old_history_records",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "date_bucket": "2024-01-01",
                "cutoff_date": "2024-01-15",
            },
        )

        self.queries["D6"] = QueryDefinition(
            query_id="D6",
            name="Delete Order with Lines Batch",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="Multi-table batch delete",
            method_name="delete_order_with_lines_batch",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "order_id": 1000,
                "customer_id": 100,
            },
        )

        self.queries["D7"] = QueryDefinition(
            query_id="D7",
            name="Delete Multiple New Orders Batch",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="Batch delete operation",
            method_name="delete_multiple_new_orders_batch",
            params_generator=lambda: {"orders": []},
        )

        # ========== ADDITIONAL SELECT QUERIES (S5-S13) ==========

        self.queries["S5"] = QueryDefinition(
            query_id="S5",
            name="Select Orders Range",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description="Range query on clustering keys",
            method_name="select_orders_range",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "start_order_id": 1000,
                "end_order_id": 2000,
            },
        )

        self.queries["S6"] = QueryDefinition(
            query_id="S6",
            name="Select Warehouses IN",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description="IN clause on partition key",
            method_name="select_warehouses_in",
            params_generator=lambda: {"warehouse_ids": [1, 2, 3]},
        )

        self.queries["S7"] = QueryDefinition(
            query_id="S7",
            name="Select Customer with Token",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description="Token-based pagination",
            method_name="select_customer_with_token",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "token_value": None,
                "limit": 100,
            },
        )

        self.queries["S8"] = QueryDefinition(
            query_id="S8",
            name="Select Items with Filter",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.COMPLEX,
            description="ALLOW FILTERING query",
            method_name="select_items_with_filter",
            params_generator=lambda: {"min_price": 10.0, "max_price": 100.0},
        )

        self.queries["S9"] = QueryDefinition(
            query_id="S9",
            name="Select Order Count",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description="COUNT aggregation",
            method_name="select_order_count",
            params_generator=lambda: {"warehouse_id": 1, "district_id": 1},
        )

        self.queries["S10"] = QueryDefinition(
            query_id="S10",
            name="Select Customer Projection",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description="Column projection query",
            method_name="select_customer_projection",
            params_generator=lambda: {"warehouse_id": 1, "district_id": 1, "customer_id": 100},
        )

        self.queries["S11"] = QueryDefinition(
            query_id="S11",
            name="Select Order Lines Range",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description="Clustering key range query",
            method_name="select_order_lines_range",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "order_id": 1000,
                "start_line": 1,
                "end_line": 5,
            },
        )

        self.queries["S12"] = QueryDefinition(
            query_id="S12",
            name="Select Orders by Carrier Index",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.COMPLEX,
            description="Secondary index query",
            method_name="select_orders_by_carrier_index",
            params_generator=lambda: {"carrier_id": 5, "limit": 100},
        )

        self.queries["S13"] = QueryDefinition(
            query_id="S13",
            name="Select Districts Multi Warehouse",
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description="Multi-partition IN query",
            method_name="select_districts_multi_warehouse",
            params_generator=lambda: {"warehouse_ids": [1, 2, 3], "district_id": 1},
        )

        # ========== ADDITIONAL INSERT QUERIES (I8-I20) ==========

        self.queries["I8"] = QueryDefinition(
            query_id="I8",
            name="Insert Customer with Collections",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="Insert with set, list, map collections",
            method_name="insert_customer_with_collections",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "customer_id": 1000,
                "name": "Test Customer",
                "phones": {"555-1234", "555-5678"},
                "emails": ["test@example.com"],
                "prefs": {"lang": "en"},
            },
        )

        self.queries["I9"] = QueryDefinition(
            query_id="I9",
            name="Insert Warehouse Metric Counter",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.SIMPLE,
            description="Counter column update",
            method_name="insert_warehouse_metric_counter",
            params_generator=lambda: {
                "warehouse_id": 1,
                "metric_name": "orders_count",
                "increment": 1,
            },
        )

        self.queries["I10"] = QueryDefinition(
            query_id="I10",
            name="Insert Customer with UDT",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="Insert with User Defined Type",
            method_name="insert_customer_with_udt",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "customer_id": 1001,
                "name": "UDT Customer",
                "address_data": {
                    "street_1": "123 Main St",
                    "street_2": "Apt 4",
                    "city": "New York",
                    "state": "NY",
                    "zip": "10001",
                },
            },
        )

        self.queries["I11"] = QueryDefinition(
            query_id="I11",
            name="Insert Product with Static",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="Insert with static column",
            method_name="insert_product_with_static",
            params_generator=lambda: {
                "category_id": 1,
                "category_name": "Electronics",
                "product_id": 100,
                "product_name": "Laptop",
                "tags": {"tech", "computer"},
            },
        )

        self.queries["I12"] = QueryDefinition(
            query_id="I12",
            name="Insert Inventory Log with TTL",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="Insert with TTL",
            method_name="insert_inventory_log_with_ttl",
            params_generator=lambda: {
                "warehouse_id": 1,
                "item_id": 1000,
                "log_type": "restock",
                "message": "Inventory restocked",
                "ttl_seconds": 86400,
            },
        )

        self.queries["I13"] = QueryDefinition(
            query_id="I13",
            name="Insert Orders Batch Logged",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="LOGGED batch insert",
            method_name="insert_orders_batch_logged",
            params_generator=lambda: {"orders": []},
        )

        self.queries["I14"] = QueryDefinition(
            query_id="I14",
            name="Insert Order Tracking Batch Unlogged",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="UNLOGGED batch insert",
            method_name="insert_order_tracking_batch_unlogged",
            params_generator=lambda: {"tracking_records": []},
        )

        self.queries["I15"] = QueryDefinition(
            query_id="I15",
            name="Insert Order with Timestamp",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="Insert with custom timestamp",
            method_name="insert_order_with_timestamp",
            params_generator=lambda: {"data": {}, "timestamp_micros": 1234567890},
        )

        self.queries["I16"] = QueryDefinition(
            query_id="I16",
            name="Insert Item All Types",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.COMPLEX,
            description="Insert with all collection types",
            method_name="insert_item_all_types",
            params_generator=lambda: {
                "item_id": 2000,
                "name": "Multi-type Item",
                "price": 99.99,
                "tags": {"new", "popular"},
                "specs": {"color": "blue"},
                "reviews": ["Good product"],
            },
        )

        self.queries["I17"] = QueryDefinition(
            query_id="I17",
            name="Insert into Multiple Tables",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.COMPLEX,
            description="Multi-table denormalization",
            method_name="insert_into_multiple_tables",
            params_generator=lambda: {"order_data": {}},
        )

        self.queries["I18"] = QueryDefinition(
            query_id="I18",
            name="Insert with LWT Condition",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.COMPLEX,
            description="LWT with IF condition",
            method_name="insert_with_lwt_condition",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "order_id": 9998,
                "customer_id": 100,
                "expected_value": None,
            },
        )

        self.queries["I19"] = QueryDefinition(
            query_id="I19",
            name="Insert Customer Activity JSON",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description="JSON insert",
            method_name="insert_customer_activity_json",
            params_generator=lambda: {"customer_id": 100, "activity_json": "{}"},
        )

        self.queries["I20"] = QueryDefinition(
            query_id="I20",
            name="Increment Warehouse Counter",
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.SIMPLE,
            description="Counter increment",
            method_name="increment_warehouse_counter",
            params_generator=lambda: {
                "warehouse_id": 1,
                "stat_date": "2024-01-01",
                "orders_increment": 1,
                "revenue_increment": 100,
            },
        )

        # ========== ADDITIONAL UPDATE QUERIES (U9-U20) ==========

        self.queries["U9"] = QueryDefinition(
            query_id="U9",
            name="Update Customer Add Phone",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Set collection add",
            method_name="update_customer_add_phone",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "customer_id": 100,
                "new_phone": "555-9999",
            },
        )

        self.queries["U10"] = QueryDefinition(
            query_id="U10",
            name="Update Customer Preferences Map",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Map collection update",
            method_name="update_customer_preferences_map",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "customer_id": 100,
                "prefs_update": {"theme": "dark"},
            },
        )

        self.queries["U11"] = QueryDefinition(
            query_id="U11",
            name="Update Customer Append Email",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="List collection append",
            method_name="update_customer_append_email",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "customer_id": 100,
                "new_email": "new@example.com",
            },
        )

        self.queries["U12"] = QueryDefinition(
            query_id="U12",
            name="Update Customer Remove Phone",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Set collection remove",
            method_name="update_customer_remove_phone",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "customer_id": 100,
                "phone_to_remove": "555-1234",
            },
        )

        self.queries["U13"] = QueryDefinition(
            query_id="U13",
            name="Update Order with TTL",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Update with TTL",
            method_name="update_order_with_ttl",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "order_id": 1000,
                "carrier_id": 5,
                "ttl_seconds": 3600,
            },
        )

        self.queries["U14"] = QueryDefinition(
            query_id="U14",
            name="Update Customer with Timestamp",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Update with custom timestamp",
            method_name="update_customer_with_timestamp",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "customer_id": 100,
                "balance": 1000.0,
                "timestamp_micros": 1234567890,
            },
        )

        self.queries["U15"] = QueryDefinition(
            query_id="U15",
            name="Update Warehouse Metrics Counter",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.SIMPLE,
            description="Counter update",
            method_name="update_warehouse_metrics_counter",
            params_generator=lambda: {
                "warehouse_id": 1,
                "metric_name": "total_sales",
                "increment": 1,
            },
        )

        self.queries["U16"] = QueryDefinition(
            query_id="U16",
            name="Update Multiple Customer Fields",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Multi-column update",
            method_name="update_multiple_customer_fields",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "customer_id": 100,
                "updates": {"c_balance": 500.0, "c_payment_cnt": 10},
            },
        )

        self.queries["U17"] = QueryDefinition(
            query_id="U17",
            name="Update Customer with Collection and TTL",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.COMPLEX,
            description="Collection + TTL",
            method_name="update_customer_with_collection_and_ttl",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "customer_id": 100,
                "tag": "vip",
                "ttl_seconds": 86400,
            },
        )

        self.queries["U18"] = QueryDefinition(
            query_id="U18",
            name="Update Static Column",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description="Static column update",
            method_name="update_static_column",
            params_generator=lambda: {"category_id": 1, "new_description": "Updated category"},
        )

        self.queries["U19"] = QueryDefinition(
            query_id="U19",
            name="Update with LWT Multiple Conditions",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.COMPLEX,
            description="LWT with multiple IF conditions",
            method_name="update_with_lwt_multiple_conditions",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "customer_id": 100,
                "new_balance": 2000.0,
                "expected_balance": 1000.0,
                "expected_credit": "GC",
            },
        )

        self.queries["U20"] = QueryDefinition(
            query_id="U20",
            name="Update Batch Unlogged",
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.COMPLEX,
            description="UNLOGGED batch update",
            method_name="update_batch_unlogged",
            params_generator=lambda: {"updates": []},
        )

        # ========== ADDITIONAL DELETE QUERIES (D8-D20) ==========

        self.queries["D8"] = QueryDefinition(
            query_id="D8",
            name="Delete Specific Column",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.SIMPLE,
            description="Column-level delete",
            method_name="delete_specific_column",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "customer_id": 100,
                "column_name": "c_phone",
            },
        )

        self.queries["D9"] = QueryDefinition(
            query_id="D9",
            name="Delete from Set Collection",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description="Set element removal",
            method_name="delete_from_set_collection",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "customer_id": 100,
                "phone_to_remove": "555-1234",
            },
        )

        self.queries["D10"] = QueryDefinition(
            query_id="D10",
            name="Delete from Map by Key",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description="Map key removal",
            method_name="delete_from_map_by_key",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "customer_id": 100,
                "pref_key": "theme",
            },
        )

        self.queries["D11"] = QueryDefinition(
            query_id="D11",
            name="Delete from List by Index",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description="List index removal",
            method_name="delete_from_list_by_index",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "customer_id": 100,
                "index": 0,
            },
        )

        self.queries["D12"] = QueryDefinition(
            query_id="D12",
            name="Delete with Timestamp",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description="Delete with custom timestamp",
            method_name="delete_with_timestamp",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "order_id": 1000,
                "timestamp_micros": 1234567890,
            },
        )

        self.queries["D13"] = QueryDefinition(
            query_id="D13",
            name="Delete Static Column",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description="Static column deletion",
            method_name="delete_static_column",
            params_generator=lambda: {"category_id": 1},
        )

        self.queries["D14"] = QueryDefinition(
            query_id="D14",
            name="Delete Clustering Range",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="Range delete on clustering keys",
            method_name="delete_clustering_range",
            params_generator=lambda: {
                "warehouse_id": 1,
                "item_id": 1000,
                "start_timestamp": "2024-01-01",
                "end_timestamp": "2024-01-31",
            },
        )

        self.queries["D15"] = QueryDefinition(
            query_id="D15",
            name="Delete with IN Clause",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="DELETE with IN on clustering key",
            method_name="delete_with_in_clause",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "order_ids": [1000, 1001, 1002],
            },
        )

        self.queries["D16"] = QueryDefinition(
            query_id="D16",
            name="Delete with LWT Condition",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="LWT conditional delete",
            method_name="delete_with_lwt_condition",
            params_generator=lambda: {
                "warehouse_id": 1,
                "district_id": 1,
                "order_id": 1000,
                "expected_carrier": 5,
            },
        )

        self.queries["D17"] = QueryDefinition(
            query_id="D17",
            name="Delete Expired Records TTL",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description="Time-based deletion",
            method_name="delete_expired_records_ttl",
            params_generator=lambda: {"warehouse_id": 1, "item_id": 1000},
        )

        self.queries["D18"] = QueryDefinition(
            query_id="D18",
            name="Delete Batch Logged",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="LOGGED batch delete",
            method_name="delete_batch_logged",
            params_generator=lambda: {"deletes": []},
        )

        self.queries["D19"] = QueryDefinition(
            query_id="D19",
            name="Delete Batch Unlogged",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="UNLOGGED batch delete",
            method_name="delete_batch_unlogged",
            params_generator=lambda: {"tracking_deletes": []},
        )

        self.queries["D20"] = QueryDefinition(
            query_id="D20",
            name="Delete Partition",
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description="Full partition delete",
            method_name="delete_partition",
            params_generator=lambda: {"warehouse_id": 1, "district_id": 1},
        )

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

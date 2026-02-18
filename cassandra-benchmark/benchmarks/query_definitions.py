"""
Query definitions for TPC-C benchmark.
Central registry of all queries with metadata.
"""

from enum import Enum
from typing import Dict, Any, List, Callable
from dataclasses import dataclass


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
        self.queries['S1'] = QueryDefinition(
            query_id='S1',
            name='Select Warehouse by ID',
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description='Get warehouse by ID - single partition lookup',
            method_name='select_warehouse_by_id',
            params_generator=lambda: {'warehouse_id': 1}
        )
        
        self.queries['S2'] = QueryDefinition(
            query_id='S2',
            name='Select Customer by ID',
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description='Get customer by ID - single partition lookup',
            method_name='select_customer_by_id',
            params_generator=lambda: {
                'warehouse_id': 1, 'district_id': 1, 'customer_id': 100
            }
        )
        
        self.queries['S3'] = QueryDefinition(
            query_id='S3',
            name='Select Item by ID',
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description='Get item by ID - single partition lookup',
            method_name='select_item_by_id',
            params_generator=lambda: {'item_id': 1000}
        )
        
        self.queries['S4'] = QueryDefinition(
            query_id='S4',
            name='Select District by ID',
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.SIMPLE,
            description='Get district by ID - single partition lookup',
            method_name='select_district_by_id',
            params_generator=lambda: {'warehouse_id': 1, 'district_id': 1}
        )
        
        # Medium SELECT queries
        self.queries['M1'] = QueryDefinition(
            query_id='M1',
            name='Select Customers by District',
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description='Get customers in a district - multi-row partition query',
            method_name='select_customers_by_district',
            params_generator=lambda: {
                'warehouse_id': 1, 'district_id': 1, 'limit': 100
            }
        )
        
        self.queries['M2'] = QueryDefinition(
            query_id='M2',
            name='Select Stock Level',
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description='Get stock level for item - business logic query',
            method_name='select_stock_level',
            params_generator=lambda: {'warehouse_id': 1, 'item_id': 1000}
        )
        
        self.queries['M3'] = QueryDefinition(
            query_id='M3',
            name='Select Orders by Customer',
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description='Get recent orders for customer - ordered multi-row query',
            method_name='select_orders_by_customer',
            params_generator=lambda: {
                'customer_id': 100, 'warehouse_id': 1, 'district_id': 1, 'limit': 20
            }
        )
        
        self.queries['M4'] = QueryDefinition(
            query_id='M4',
            name='Select Order Lines',
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.MEDIUM,
            description='Get all order lines for order - multi-row partition query',
            method_name='select_order_lines',
            params_generator=lambda: {
                'warehouse_id': 1, 'district_id': 1, 'order_id': 1000
            }
        )
        
        # Complex SELECT queries
        self.queries['C1'] = QueryDefinition(
            query_id='C1',
            name='Select Customers by Name',
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.COMPLEX,
            description='Get customers by last name - denormalized table query',
            method_name='select_customers_by_name',
            params_generator=lambda: {
                'warehouse_id': 1, 'district_id': 1, 'last_name': 'SMITH'
            }
        )
        
        self.queries['C2'] = QueryDefinition(
            query_id='C2',
            name='Select New Orders',
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.COMPLEX,
            description='Get new orders for district - multi-row query',
            method_name='select_new_orders',
            params_generator=lambda: {
                'warehouse_id': 1, 'district_id': 1, 'limit': 20
            }
        )
        
        self.queries['C3'] = QueryDefinition(
            query_id='C3',
            name='Select History by Date Range',
            query_type=QueryType.SELECT,
            complexity=ComplexityLevel.COMPLEX,
            description='Get history in date range - time-series range query',
            method_name='select_history_by_date_range',
            params_generator=lambda: {
                'warehouse_id': 1, 'district_id': 1, 'date_bucket': '2024-01-01',
                'start_date': '2024-01-01 00:00:00', 'end_date': '2024-01-01 23:59:59'
            }
        )
        
        # ========== INSERT QUERIES ==========
        
        # Note: Insert/Update/Delete queries will use mock data generators
        # The actual implementations would need proper data generation
        
        self.queries['I1'] = QueryDefinition(
            query_id='I1',
            name='Insert Customer',
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.SIMPLE,
            description='Insert new customer - basic insert',
            method_name='insert_customer',
            params_generator=lambda: {'data': {}}  # Placeholder
        )
        
        self.queries['I2'] = QueryDefinition(
            query_id='I2',
            name='Insert Order',
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.SIMPLE,
            description='Insert new order - basic insert with denormalization',
            method_name='insert_order',
            params_generator=lambda: {'data': {}}  # Placeholder
        )
        
        self.queries['I3'] = QueryDefinition(
            query_id='I3',
            name='Insert History',
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.SIMPLE,
            description='Insert history record - time-series insert',
            method_name='insert_history',
            params_generator=lambda: {'data': {}}  # Placeholder
        )
        
        self.queries['I4'] = QueryDefinition(
            query_id='I4',
            name='Batch Insert Order Lines',
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description='Batch insert order lines',
            method_name='insert_order_lines_batch',
            params_generator=lambda: {'order_lines': []}  # Placeholder
        )
        
        self.queries['I5'] = QueryDefinition(
            query_id='I5',
            name='Insert History with TTL',
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.MEDIUM,
            description='Insert history with TTL',
            method_name='insert_history_with_ttl',
            params_generator=lambda: {'data': {}, 'ttl_seconds': 86400}  # Placeholder
        )
        
        self.queries['I6'] = QueryDefinition(
            query_id='I6',
            name='Insert New Order (LWT)',
            query_type=QueryType.INSERT,
            complexity=ComplexityLevel.COMPLEX,
            description='Insert new order with LWT',
            method_name='insert_new_order_lwt',
            params_generator=lambda: {
                'warehouse_id': 1, 'district_id': 1, 'order_id': 9999
            }
        )
        
        # ========== UPDATE QUERIES ==========
        
        self.queries['U1'] = QueryDefinition(
            query_id='U1',
            name='Update Customer Balance',
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.SIMPLE,
            description='Update customer balance - basic update',
            method_name='update_customer_balance',
            params_generator=lambda: {
                'warehouse_id': 1, 'district_id': 1, 'customer_id': 100,
                'balance': 1000.0, 'ytd_payment': 500.0, 'payment_cnt': 5
            }
        )
        
        self.queries['U2'] = QueryDefinition(
            query_id='U2',
            name='Update Stock Quantity',
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.SIMPLE,
            description='Update stock quantity - basic update',
            method_name='update_stock_quantity',
            params_generator=lambda: {
                'warehouse_id': 1, 'item_id': 1000,
                'quantity': 50, 'ytd': 1000, 'order_cnt': 100
            }
        )
        
        self.queries['U3'] = QueryDefinition(
            query_id='U3',
            name='Update District Next Order',
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.SIMPLE,
            description='Update district next order ID',
            method_name='update_district_next_order',
            params_generator=lambda: {
                'warehouse_id': 1, 'district_id': 1, 'next_order_id': 3001
            }
        )
        
        self.queries['U4'] = QueryDefinition(
            query_id='U4',
            name='Update Order Carrier (Conditional)',
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.MEDIUM,
            description='Update order carrier conditionally',
            method_name='update_order_carrier_conditional',
            params_generator=lambda: {
                'warehouse_id': 1, 'district_id': 1, 'order_id': 1000, 'carrier_id': 5
            }
        )
        
        self.queries['U5'] = QueryDefinition(
            query_id='U5',
            name='Update Stock with LWT',
            query_type=QueryType.UPDATE,
            complexity=ComplexityLevel.COMPLEX,
            description='Update stock with LWT validation',
            method_name='update_stock_with_lwt',
            params_generator=lambda: {
                'warehouse_id': 1, 'item_id': 1000, 'new_quantity': 40,
                'ytd': 1100, 'order_cnt': 101, 'remote_cnt': 10, 'min_quantity': 10
            }
        )
        
        # ========== DELETE QUERIES ==========
        
        self.queries['D1'] = QueryDefinition(
            query_id='D1',
            name='Delete Order Line',
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.SIMPLE,
            description='Delete specific order line',
            method_name='delete_order_line',
            params_generator=lambda: {
                'warehouse_id': 1, 'district_id': 1, 'order_id': 1000, 'line_number': 1
            }
        )
        
        self.queries['D2'] = QueryDefinition(
            query_id='D2',
            name='Delete New Order',
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.SIMPLE,
            description='Delete new order record',
            method_name='delete_new_order',
            params_generator=lambda: {
                'warehouse_id': 1, 'district_id': 1, 'order_id': 9999
            }
        )
        
        self.queries['D3'] = QueryDefinition(
            query_id='D3',
            name='Delete New Order (Conditional)',
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.MEDIUM,
            description='Delete new order with IF EXISTS',
            method_name='delete_new_order_conditional',
            params_generator=lambda: {
                'warehouse_id': 1, 'district_id': 1, 'order_id': 9999
            }
        )
        
        self.queries['D4'] = QueryDefinition(
            query_id='D4',
            name='Delete All Order Lines',
            query_type=QueryType.DELETE,
            complexity=ComplexityLevel.COMPLEX,
            description='Delete all order lines for an order',
            method_name='delete_all_order_lines',
            params_generator=lambda: {
                'warehouse_id': 1, 'district_id': 1, 'order_id': 1000
            }
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

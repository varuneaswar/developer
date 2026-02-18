"""
SELECT queries for TPC-C benchmark.
Categorized by complexity: Simple, Medium, Complex
"""

from typing import Dict, List, Any
from cassandra.cluster import Session


class SelectQueries:
    """SELECT query definitions for TPC-C benchmark."""
    
    def __init__(self, session: Session):
        """
        Initialize SELECT queries with Cassandra session.
        
        Args:
            session: Active Cassandra session
        """
        self.session = session
        self._prepare_statements()
    
    def _prepare_statements(self) -> None:
        """Prepare all SELECT statements for better performance."""
        # Simple queries
        self.get_warehouse_stmt = self.session.prepare(
            "SELECT * FROM warehouse WHERE w_id = ?"
        )
        
        self.get_customer_stmt = self.session.prepare(
            "SELECT * FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
        )
        
        self.get_item_stmt = self.session.prepare(
            "SELECT * FROM item WHERE i_id = ?"
        )
        
        self.get_district_stmt = self.session.prepare(
            "SELECT * FROM district WHERE d_w_id = ? AND d_id = ?"
        )
        
        # Medium queries
        self.get_customers_by_district_stmt = self.session.prepare(
            "SELECT * FROM customer WHERE c_w_id = ? AND c_d_id = ? LIMIT ?"
        )
        
        self.get_stock_stmt = self.session.prepare(
            "SELECT * FROM stock WHERE s_w_id = ? AND s_i_id = ?"
        )
        
        self.get_orders_by_customer_stmt = self.session.prepare(
            "SELECT * FROM orders_by_customer WHERE o_c_id = ? AND o_w_id = ? AND o_d_id = ? LIMIT ?"
        )
        
        self.get_order_lines_stmt = self.session.prepare(
            "SELECT * FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?"
        )
        
        # Complex queries
        self.get_customers_by_name_stmt = self.session.prepare(
            "SELECT * FROM customer_by_name WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?"
        )
        
        self.get_new_orders_stmt = self.session.prepare(
            "SELECT * FROM new_order WHERE no_w_id = ? AND no_d_id = ? LIMIT ?"
        )
        
        self.get_history_by_date_stmt = self.session.prepare(
            "SELECT * FROM history WHERE h_w_id = ? AND h_d_id = ? AND date_bucket = ? AND h_date >= ? AND h_date <= ?"
        )
    
    # ========== SIMPLE SELECT QUERIES ==========
    
    def select_warehouse_by_id(self, warehouse_id: int) -> List[Dict[str, Any]]:
        """
        S1: Get warehouse by ID.
        Complexity: Simple - Single partition key lookup
        
        Args:
            warehouse_id: Warehouse identifier
            
        Returns:
            List of warehouse records
        """
        result = self.session.execute(self.get_warehouse_stmt, [warehouse_id])
        return [dict(row._asdict()) for row in result]
    
    def select_customer_by_id(self, warehouse_id: int, district_id: int, 
                              customer_id: int) -> List[Dict[str, Any]]:
        """
        S2: Get customer by ID.
        Complexity: Simple - Single partition key lookup
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            
        Returns:
            List of customer records
        """
        result = self.session.execute(
            self.get_customer_stmt, 
            [warehouse_id, district_id, customer_id]
        )
        return [dict(row._asdict()) for row in result]
    
    def select_item_by_id(self, item_id: int) -> List[Dict[str, Any]]:
        """
        S3: Get item by ID.
        Complexity: Simple - Single partition key lookup
        
        Args:
            item_id: Item identifier
            
        Returns:
            List of item records
        """
        result = self.session.execute(self.get_item_stmt, [item_id])
        return [dict(row._asdict()) for row in result]
    
    def select_district_by_id(self, warehouse_id: int, district_id: int) -> List[Dict[str, Any]]:
        """
        S4: Get district by ID.
        Complexity: Simple - Single partition key lookup
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            
        Returns:
            List of district records
        """
        result = self.session.execute(
            self.get_district_stmt, 
            [warehouse_id, district_id]
        )
        return [dict(row._asdict()) for row in result]
    
    # ========== MEDIUM SELECT QUERIES ==========
    
    def select_customers_by_district(self, warehouse_id: int, district_id: int, 
                                     limit: int = 100) -> List[Dict[str, Any]]:
        """
        M1: Get customers in a district.
        Complexity: Medium - Multi-row query within partition
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            limit: Maximum number of records to return
            
        Returns:
            List of customer records
        """
        result = self.session.execute(
            self.get_customers_by_district_stmt,
            [warehouse_id, district_id, limit]
        )
        return [dict(row._asdict()) for row in result]
    
    def select_stock_level(self, warehouse_id: int, item_id: int) -> List[Dict[str, Any]]:
        """
        M2: Get stock level for an item in a warehouse.
        Complexity: Medium - Single partition lookup with business logic
        
        Args:
            warehouse_id: Warehouse identifier
            item_id: Item identifier
            
        Returns:
            List of stock records
        """
        result = self.session.execute(
            self.get_stock_stmt,
            [warehouse_id, item_id]
        )
        return [dict(row._asdict()) for row in result]
    
    def select_orders_by_customer(self, customer_id: int, warehouse_id: int, 
                                  district_id: int, limit: int = 20) -> List[Dict[str, Any]]:
        """
        M3: Get recent orders for a customer.
        Complexity: Medium - Multi-row query within partition with ordering
        
        Args:
            customer_id: Customer identifier
            warehouse_id: Warehouse identifier
            district_id: District identifier
            limit: Maximum number of records to return
            
        Returns:
            List of order records
        """
        result = self.session.execute(
            self.get_orders_by_customer_stmt,
            [customer_id, warehouse_id, district_id, limit]
        )
        return [dict(row._asdict()) for row in result]
    
    def select_order_lines(self, warehouse_id: int, district_id: int, 
                          order_id: int) -> List[Dict[str, Any]]:
        """
        M4: Get all order lines for an order.
        Complexity: Medium - Multi-row query within partition
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            order_id: Order identifier
            
        Returns:
            List of order line records
        """
        result = self.session.execute(
            self.get_order_lines_stmt,
            [warehouse_id, district_id, order_id]
        )
        return [dict(row._asdict()) for row in result]
    
    # ========== COMPLEX SELECT QUERIES ==========
    
    def select_customers_by_name(self, warehouse_id: int, district_id: int, 
                                 last_name: str) -> List[Dict[str, Any]]:
        """
        C1: Get customers by last name.
        Complexity: Complex - Requires denormalized table, filtered query
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            last_name: Customer last name
            
        Returns:
            List of customer records
        """
        result = self.session.execute(
            self.get_customers_by_name_stmt,
            [warehouse_id, district_id, last_name]
        )
        return [dict(row._asdict()) for row in result]
    
    def select_new_orders(self, warehouse_id: int, district_id: int, 
                         limit: int = 20) -> List[Dict[str, Any]]:
        """
        C2: Get new orders for a district.
        Complexity: Complex - Multi-row query for new order processing
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            limit: Maximum number of records to return
            
        Returns:
            List of new order records
        """
        result = self.session.execute(
            self.get_new_orders_stmt,
            [warehouse_id, district_id, limit]
        )
        return [dict(row._asdict()) for row in result]
    
    def select_history_by_date_range(self, warehouse_id: int, district_id: int,
                                     date_bucket: str, start_date: str, 
                                     end_date: str) -> List[Dict[str, Any]]:
        """
        C3: Get history records within a date range.
        Complexity: Complex - Time-series query with range scan
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            date_bucket: Date bucket (YYYY-MM-DD format)
            start_date: Start timestamp
            end_date: End timestamp
            
        Returns:
            List of history records
        """
        result = self.session.execute(
            self.get_history_by_date_stmt,
            [warehouse_id, district_id, date_bucket, start_date, end_date]
        )
        return [dict(row._asdict()) for row in result]
    
    def select_customers_by_credit(self, credit_status: str, 
                                   fetch_size: int = 100) -> List[Dict[str, Any]]:
        """
        C4: Get customers by credit status.
        Complexity: Complex - Secondary index query (use sparingly)
        
        Args:
            credit_status: Credit status (e.g., 'BC' or 'GC')
            fetch_size: Number of records to fetch
            
        Returns:
            List of customer records
        """
        query = "SELECT * FROM customer WHERE c_credit = ? LIMIT ?"
        result = self.session.execute(query, [credit_status, fetch_size])
        return [dict(row._asdict()) for row in result]
    
    # ========== ADDITIONAL SELECT QUERIES (S5-S13) ==========
    
    def select_orders_range(self, warehouse_id: int, district_id: int,
                           start_order_id: int, end_order_id: int) -> List[Dict[str, Any]]:
        """
        S5: Get orders within a range of order IDs.
        Complexity: Simple - Range query on clustering keys
        Cassandra Concept: Clustering key range queries
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            start_order_id: Start order ID
            end_order_id: End order ID
            
        Returns:
            List of order records
        """
        query = """
            SELECT * FROM orders 
            WHERE o_w_id = ? AND o_d_id = ? AND o_id >= ? AND o_id <= ?
        """
        result = self.session.execute(query, [warehouse_id, district_id, start_order_id, end_order_id])
        return [dict(row._asdict()) for row in result]
    
    def select_warehouses_in(self, warehouse_ids: List[int]) -> List[Dict[str, Any]]:
        """
        S6: Get multiple warehouses using IN clause.
        Complexity: Simple - IN clause on partition key
        Cassandra Concept: IN queries on partition keys
        
        Args:
            warehouse_ids: List of warehouse identifiers
            
        Returns:
            List of warehouse records
        """
        query = f"SELECT * FROM warehouse WHERE w_id IN ({','.join('?' * len(warehouse_ids))})"
        result = self.session.execute(query, warehouse_ids)
        return [dict(row._asdict()) for row in result]
    
    def select_customer_with_token(self, warehouse_id: int, district_id: int,
                                   token_value: int = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        S7: Get customers using token-based pagination.
        Complexity: Medium - Token function for pagination
        Cassandra Concept: Token-based pagination for large partitions
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            token_value: Token value for pagination (None for first page)
            limit: Number of records to fetch
            
        Returns:
            List of customer records
        """
        if token_value is None:
            query = "SELECT * FROM customer WHERE c_w_id = ? AND c_d_id = ? LIMIT ?"
            result = self.session.execute(query, [warehouse_id, district_id, limit])
        else:
            query = """
                SELECT * FROM customer 
                WHERE token(c_w_id, c_d_id) > ? 
                LIMIT ? ALLOW FILTERING
            """
            result = self.session.execute(query, [token_value, limit])
        return [dict(row._asdict()) for row in result]
    
    def select_items_with_filter(self, min_price: float, max_price: float) -> List[Dict[str, Any]]:
        """
        S8: Get items within price range using ALLOW FILTERING.
        Complexity: Complex - Full table scan with filtering
        Cassandra Concept: ALLOW FILTERING (use with caution)
        
        Args:
            min_price: Minimum price
            max_price: Maximum price
            
        Returns:
            List of item records
        """
        query = """
            SELECT * FROM item 
            WHERE i_price >= ? AND i_price <= ? 
            ALLOW FILTERING
            LIMIT 1000
        """
        result = self.session.execute(query, [min_price, max_price])
        return [dict(row._asdict()) for row in result]
    
    def select_order_count(self, warehouse_id: int, district_id: int) -> int:
        """
        S9: Count orders for a warehouse and district.
        Complexity: Simple - COUNT aggregation
        Cassandra Concept: COUNT queries
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            
        Returns:
            Count of orders
        """
        query = "SELECT COUNT(*) FROM orders WHERE o_w_id = ? AND o_d_id = ?"
        result = self.session.execute(query, [warehouse_id, district_id])
        row = result.one()
        return row[0] if row else 0
    
    def select_customer_projection(self, warehouse_id: int, district_id: int,
                                   customer_id: int) -> Dict[str, Any]:
        """
        S10: Get specific customer fields (projection).
        Complexity: Simple - Column projection
        Cassandra Concept: Selecting specific columns
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            
        Returns:
            Customer record with selected fields only
        """
        query = """
            SELECT c_id, c_first, c_last, c_balance, c_credit 
            FROM customer 
            WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
        """
        result = self.session.execute(query, [warehouse_id, district_id, customer_id])
        row = result.one()
        return dict(row._asdict()) if row else {}
    
    def select_order_lines_range(self, warehouse_id: int, district_id: int,
                                 order_id: int, start_line: int, end_line: int) -> List[Dict[str, Any]]:
        """
        S11: Get order lines within a range.
        Complexity: Medium - Clustering key range query
        Cassandra Concept: Range queries on clustering columns
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            order_id: Order identifier
            start_line: Start line number
            end_line: End line number
            
        Returns:
            List of order line records
        """
        query = """
            SELECT * FROM order_line 
            WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? 
            AND ol_number >= ? AND ol_number <= ?
        """
        result = self.session.execute(query, [warehouse_id, district_id, order_id, start_line, end_line])
        return [dict(row._asdict()) for row in result]
    
    def select_orders_by_carrier_index(self, carrier_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        """
        S12: Get orders by carrier using secondary index.
        Complexity: Complex - Secondary index with filtering
        Cassandra Concept: Secondary index queries
        
        Args:
            carrier_id: Carrier identifier
            limit: Maximum number of records
            
        Returns:
            List of order records
        """
        query = "SELECT * FROM orders WHERE o_carrier_id = ? LIMIT ?"
        result = self.session.execute(query, [carrier_id, limit])
        return [dict(row._asdict()) for row in result]
    
    def select_districts_multi_warehouse(self, warehouse_ids: List[int],
                                        district_id: int) -> List[Dict[str, Any]]:
        """
        S13: Get districts across multiple warehouses using IN.
        Complexity: Medium - Multi-partition query with IN clause
        Cassandra Concept: IN clause on partition key component
        
        Args:
            warehouse_ids: List of warehouse identifiers
            district_id: District identifier
            
        Returns:
            List of district records
        """
        query = f"""
            SELECT * FROM district 
            WHERE d_w_id IN ({','.join('?' * len(warehouse_ids))}) 
            AND d_id = ?
        """
        params = warehouse_ids + [district_id]
        result = self.session.execute(query, params)
        return [dict(row._asdict()) for row in result]

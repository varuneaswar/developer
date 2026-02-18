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

"""
DELETE queries for TPC-C benchmark.
Categorized by complexity: Simple, Medium, Complex
"""

from typing import Dict, Any, List
from cassandra.cluster import Session
from cassandra.query import BatchStatement, ConsistencyLevel


class DeleteQueries:
    """DELETE query definitions for TPC-C benchmark."""
    
    def __init__(self, session: Session):
        """
        Initialize DELETE queries with Cassandra session.
        
        Args:
            session: Active Cassandra session
        """
        self.session = session
        self._prepare_statements()
    
    def _prepare_statements(self) -> None:
        """Prepare all DELETE statements for better performance."""
        # Simple deletes
        self.delete_order_line_stmt = self.session.prepare(
            """
            DELETE FROM order_line 
            WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_number = ?
            """
        )
        
        self.delete_new_order_stmt = self.session.prepare(
            """
            DELETE FROM new_order 
            WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?
            """
        )
        
        # Medium deletes - conditional
        self.delete_new_order_conditional_stmt = self.session.prepare(
            """
            DELETE FROM new_order 
            WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?
            IF EXISTS
            """
        )
        
        # Complex deletes - batch
        self.delete_all_order_lines_stmt = self.session.prepare(
            """
            DELETE FROM order_line 
            WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?
            """
        )
    
    # ========== SIMPLE DELETE QUERIES ==========
    
    def delete_order_line(self, warehouse_id: int, district_id: int, 
                         order_id: int, line_number: int) -> bool:
        """
        D1: Delete a specific order line.
        Complexity: Simple - Basic delete
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            order_id: Order identifier
            line_number: Order line number
            
        Returns:
            True if successful
        """
        try:
            self.session.execute(self.delete_order_line_stmt, [
                warehouse_id, district_id, order_id, line_number
            ])
            return True
        except Exception as e:
            print(f"Error deleting order line: {e}")
            return False
    
    def delete_new_order(self, warehouse_id: int, district_id: int, 
                        order_id: int) -> bool:
        """
        D2: Delete a new order record.
        Complexity: Simple - Basic delete
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            order_id: Order identifier
            
        Returns:
            True if successful
        """
        try:
            self.session.execute(self.delete_new_order_stmt, [
                warehouse_id, district_id, order_id
            ])
            return True
        except Exception as e:
            print(f"Error deleting new order: {e}")
            return False
    
    # ========== MEDIUM DELETE QUERIES ==========
    
    def delete_new_order_conditional(self, warehouse_id: int, district_id: int, 
                                    order_id: int) -> Dict[str, Any]:
        """
        D3: Delete new order with conditional (IF EXISTS).
        Complexity: Medium - Conditional delete
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            order_id: Order identifier
            
        Returns:
            Dict with 'applied' boolean
        """
        try:
            result = self.session.execute(self.delete_new_order_conditional_stmt, [
                warehouse_id, district_id, order_id
            ])
            row = result.one()
            return {
                'applied': row[0],
                'message': 'Delete successful' if row[0] else 'Record does not exist'
            }
        except Exception as e:
            return {
                'applied': False,
                'message': f'Error: {e}'
            }
    
    def delete_old_history_records(self, warehouse_id: int, district_id: int, 
                                   date_bucket: str, cutoff_date: str) -> bool:
        """
        D4: Delete old history records (before cutoff date).
        Complexity: Medium - Time-series delete
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            date_bucket: Date bucket (YYYY-MM-DD)
            cutoff_date: Cutoff timestamp
            
        Returns:
            True if successful
        """
        try:
            # Note: In Cassandra, we can't directly delete by range in clustering column
            # First, we need to select the records, then delete them
            select_query = """
                SELECT h_date, h_c_id FROM history 
                WHERE h_w_id = ? AND h_d_id = ? AND date_bucket = ? AND h_date < ?
            """
            result = self.session.execute(select_query, [
                warehouse_id, district_id, date_bucket, cutoff_date
            ])
            
            # Delete each record
            delete_stmt = self.session.prepare(
                """
                DELETE FROM history 
                WHERE h_w_id = ? AND h_d_id = ? AND date_bucket = ? AND h_date = ? AND h_c_id = ?
                """
            )
            
            for row in result:
                self.session.execute(delete_stmt, [
                    warehouse_id, district_id, date_bucket, row.h_date, row.h_c_id
                ])
            
            return True
        except Exception as e:
            print(f"Error deleting old history records: {e}")
            return False
    
    # ========== COMPLEX DELETE QUERIES ==========
    
    def delete_all_order_lines(self, warehouse_id: int, district_id: int, 
                               order_id: int) -> bool:
        """
        D5: Delete all order lines for an order (partition delete).
        Complexity: Complex - Partition-level delete
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            order_id: Order identifier
            
        Returns:
            True if successful
        """
        try:
            self.session.execute(self.delete_all_order_lines_stmt, [
                warehouse_id, district_id, order_id
            ])
            return True
        except Exception as e:
            print(f"Error deleting all order lines: {e}")
            return False
    
    def delete_order_with_lines_batch(self, warehouse_id: int, district_id: int, 
                                      order_id: int, customer_id: int) -> bool:
        """
        D6: Delete order and all its lines in a batch.
        Complexity: Complex - Multi-table batch delete
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            order_id: Order identifier
            customer_id: Customer identifier
            
        Returns:
            True if successful
        """
        try:
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            
            # Delete from orders table
            delete_order = self.session.prepare(
                """
                DELETE FROM orders 
                WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?
                """
            )
            batch.add(delete_order, [warehouse_id, district_id, order_id])
            
            # Delete from orders_by_customer table
            delete_order_by_customer = self.session.prepare(
                """
                DELETE FROM orders_by_customer 
                WHERE o_c_id = ? AND o_w_id = ? AND o_d_id = ? AND o_entry_d >= ? AND o_id = ?
                """
            )
            # Note: We need entry_d for the delete, so we'd need to fetch it first
            # For this example, we'll skip the denormalized table delete
            
            # Delete all order lines
            batch.add(self.delete_all_order_lines_stmt, [
                warehouse_id, district_id, order_id
            ])
            
            # Delete from new_order if exists
            batch.add(self.delete_new_order_stmt, [
                warehouse_id, district_id, order_id
            ])
            
            self.session.execute(batch)
            return True
        except Exception as e:
            print(f"Error batch deleting order with lines: {e}")
            return False
    
    def delete_multiple_new_orders_batch(self, orders: List[Dict[str, Any]]) -> bool:
        """
        D7: Delete multiple new orders in a batch.
        Complexity: Complex - Batch delete operation
        
        Args:
            orders: List of order dictionaries with warehouse_id, district_id, order_id
            
        Returns:
            True if successful
        """
        try:
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            
            for order in orders:
                batch.add(self.delete_new_order_stmt, [
                    order['warehouse_id'],
                    order['district_id'],
                    order['order_id']
                ])
            
            self.session.execute(batch)
            return True
        except Exception as e:
            print(f"Error batch deleting new orders: {e}")
            return False

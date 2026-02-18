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
    
    # ========== ADDITIONAL DELETE QUERIES (D8-D20) ==========
    
    def delete_specific_column(self, warehouse_id: int, district_id: int,
                              customer_id: int, column_name: str) -> bool:
        """
        D8: Delete a specific column from customer.
        Complexity: Simple - Single column delete
        Cassandra Concept: Column-level deletes
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            column_name: Name of column to delete
            
        Returns:
            True if successful
        """
        try:
            query = f"""
                DELETE {column_name} FROM customer 
                WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
            """
            self.session.execute(query, [warehouse_id, district_id, customer_id])
            return True
        except Exception as e:
            print(f"Error deleting column: {e}")
            return False
    
    def delete_from_set_collection(self, warehouse_id: int, district_id: int,
                                  customer_id: int, phone_to_remove: str) -> bool:
        """
        D9: Delete element from phone numbers set.
        Complexity: Medium - Collection element delete
        Cassandra Concept: Set collection element removal
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            phone_to_remove: Phone number to remove from set
            
        Returns:
            True if successful
        """
        try:
            query = """
                UPDATE customer_extended 
                SET c_phone_numbers = c_phone_numbers - ?
                WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
            """
            self.session.execute(query, [{phone_to_remove}, warehouse_id, district_id, customer_id])
            return True
        except Exception as e:
            print(f"Error deleting from set: {e}")
            return False
    
    def delete_from_map_by_key(self, warehouse_id: int, district_id: int,
                               customer_id: int, pref_key: str) -> bool:
        """
        D10: Delete entry from preferences map by key.
        Complexity: Medium - Map key delete
        Cassandra Concept: Map collection key removal
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            pref_key: Preference key to remove
            
        Returns:
            True if successful
        """
        try:
            query = """
                DELETE c_preferences[?] FROM customer_extended 
                WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
            """
            self.session.execute(query, [pref_key, warehouse_id, district_id, customer_id])
            return True
        except Exception as e:
            print(f"Error deleting from map: {e}")
            return False
    
    def delete_from_list_by_index(self, warehouse_id: int, district_id: int,
                                  customer_id: int, index: int) -> bool:
        """
        D11: Delete element from email list by index.
        Complexity: Medium - List index delete
        Cassandra Concept: List collection index removal
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            index: Index to remove from list
            
        Returns:
            True if successful
        """
        try:
            query = """
                DELETE c_email_history[?] FROM customer_extended 
                WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
            """
            self.session.execute(query, [index, warehouse_id, district_id, customer_id])
            return True
        except Exception as e:
            print(f"Error deleting from list: {e}")
            return False
    
    def delete_with_timestamp(self, warehouse_id: int, district_id: int,
                             order_id: int, timestamp_micros: int) -> bool:
        """
        D12: Delete order with custom timestamp.
        Complexity: Medium - Delete with timestamp
        Cassandra Concept: USING TIMESTAMP for delete
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            order_id: Order identifier
            timestamp_micros: Timestamp in microseconds
            
        Returns:
            True if successful
        """
        try:
            query = """
                DELETE FROM orders 
                WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?
                USING TIMESTAMP ?
            """
            self.session.execute(query, [warehouse_id, district_id, order_id, timestamp_micros])
            return True
        except Exception as e:
            print(f"Error deleting with timestamp: {e}")
            return False
    
    def delete_static_column(self, category_id: int) -> bool:
        """
        D13: Delete static column from product catalog.
        Complexity: Medium - Static column delete
        Cassandra Concept: Static column deletion (affects partition)
        
        Args:
            category_id: Category identifier
            
        Returns:
            True if successful
        """
        try:
            query = """
                DELETE category_description FROM product_catalog 
                WHERE category_id = ?
            """
            self.session.execute(query, [category_id])
            return True
        except Exception as e:
            print(f"Error deleting static column: {e}")
            return False
    
    def delete_clustering_range(self, warehouse_id: int, item_id: int,
                               start_timestamp: str, end_timestamp: str) -> bool:
        """
        D14: Delete inventory logs within timestamp range.
        Complexity: Complex - Range delete on clustering keys
        Cassandra Concept: Clustering key range deletion
        
        Args:
            warehouse_id: Warehouse identifier
            item_id: Item identifier
            start_timestamp: Start timestamp
            end_timestamp: End timestamp
            
        Returns:
            True if successful
        """
        try:
            query = """
                DELETE FROM inventory_log 
                WHERE i_w_id = ? AND i_id = ? 
                AND log_timestamp >= ? AND log_timestamp <= ?
            """
            self.session.execute(query, [warehouse_id, item_id, start_timestamp, end_timestamp])
            return True
        except Exception as e:
            print(f"Error deleting range: {e}")
            return False
    
    def delete_with_in_clause(self, warehouse_id: int, district_id: int,
                             order_ids: List[int]) -> bool:
        """
        D15: Delete multiple orders using IN clause.
        Complexity: Complex - IN clause delete
        Cassandra Concept: DELETE with IN on clustering key
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            order_ids: List of order IDs to delete
            
        Returns:
            True if successful
        """
        try:
            query = f"""
                DELETE FROM orders 
                WHERE o_w_id = ? AND o_d_id = ? 
                AND o_id IN ({','.join('?' * len(order_ids))})
            """
            params = [warehouse_id, district_id] + order_ids
            self.session.execute(query, params)
            return True
        except Exception as e:
            print(f"Error deleting with IN clause: {e}")
            return False
    
    def delete_with_lwt_condition(self, warehouse_id: int, district_id: int,
                                  order_id: int, expected_carrier: int) -> Dict[str, Any]:
        """
        D16: Delete order with LWT condition.
        Complexity: Complex - LWT conditional delete
        Cassandra Concept: Lightweight Transaction delete with IF
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            order_id: Order identifier
            expected_carrier: Expected carrier ID
            
        Returns:
            Dict with 'applied' boolean
        """
        try:
            query = """
                DELETE FROM orders 
                WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?
                IF o_carrier_id = ?
            """
            result = self.session.execute(query, [warehouse_id, district_id, order_id, expected_carrier])
            row = result.one()
            return {
                'applied': row[0],
                'message': 'Delete successful' if row[0] else 'Condition not met'
            }
        except Exception as e:
            return {
                'applied': False,
                'message': f'Error: {e}'
            }
    
    def delete_expired_records_ttl(self, warehouse_id: int, item_id: int) -> bool:
        """
        D17: Delete old inventory logs (TTL simulation).
        Complexity: Medium - Time-based deletion
        Cassandra Concept: Manual cleanup of expired data
        
        Args:
            warehouse_id: Warehouse identifier
            item_id: Item identifier
            
        Returns:
            True if successful
        """
        try:
            # Delete records older than certain timestamp
            query = """
                DELETE FROM inventory_log 
                WHERE i_w_id = ? AND i_id = ? 
                AND log_timestamp < ?
            """
            # Use current time minus retention period
            from datetime import datetime, timedelta
            cutoff = datetime.now() - timedelta(days=30)
            self.session.execute(query, [warehouse_id, item_id, cutoff])
            return True
        except Exception as e:
            print(f"Error deleting expired records: {e}")
            return False
    
    def delete_batch_logged(self, deletes: List[Dict[str, Any]]) -> bool:
        """
        D18: Batch delete multiple records (LOGGED).
        Complexity: Complex - LOGGED batch delete for atomicity
        Cassandra Concept: LOGGED batch operations
        
        Args:
            deletes: List of delete dictionaries
            
        Returns:
            True if successful
        """
        try:
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            
            for delete in deletes:
                delete_stmt = self.session.prepare(
                    """
                    DELETE FROM new_order 
                    WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?
                    """
                )
                batch.add(delete_stmt, [
                    delete['warehouse_id'],
                    delete['district_id'],
                    delete['order_id']
                ])
            
            self.session.execute(batch)
            return True
        except Exception as e:
            print(f"Error in logged batch delete: {e}")
            return False
    
    def delete_batch_unlogged(self, tracking_deletes: List[Dict[str, Any]]) -> bool:
        """
        D19: Batch delete order tracking (UNLOGGED).
        Complexity: Complex - UNLOGGED batch for performance
        Cassandra Concept: UNLOGGED batch operations
        
        Args:
            tracking_deletes: List of tracking delete dictionaries
            
        Returns:
            True if successful
        """
        try:
            batch_query = "BEGIN UNLOGGED BATCH\n"
            params = []
            
            for delete in tracking_deletes:
                batch_query += """
                    DELETE FROM order_tracking 
                    WHERE o_w_id = ? AND o_status = ? 
                    AND o_timestamp = ? AND o_id = ?;
                """
                params.extend([
                    delete['warehouse_id'],
                    delete['status'],
                    delete['timestamp'],
                    delete['order_id']
                ])
            
            batch_query += "APPLY BATCH;"
            
            self.session.execute(batch_query, params)
            return True
        except Exception as e:
            print(f"Error in unlogged batch delete: {e}")
            return False
    
    def delete_partition(self, warehouse_id: int, district_id: int) -> bool:
        """
        D20: Delete entire partition of orders.
        Complexity: Complex - Full partition delete
        Cassandra Concept: Partition-level deletion
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            
        Returns:
            True if successful
        """
        try:
            query = """
                DELETE FROM orders 
                WHERE o_w_id = ? AND o_d_id = ?
            """
            self.session.execute(query, [warehouse_id, district_id])
            return True
        except Exception as e:
            print(f"Error deleting partition: {e}")
            return False

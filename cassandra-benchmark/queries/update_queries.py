"""
UPDATE queries for TPC-C benchmark.
Categorized by complexity: Simple, Medium, Complex
"""

from typing import Dict, Any, List
from cassandra.cluster import Session
from cassandra.query import BatchStatement, ConsistencyLevel


class UpdateQueries:
    """UPDATE query definitions for TPC-C benchmark."""
    
    def __init__(self, session: Session):
        """
        Initialize UPDATE queries with Cassandra session.
        
        Args:
            session: Active Cassandra session
        """
        self.session = session
        self._prepare_statements()
    
    def _prepare_statements(self) -> None:
        """Prepare all UPDATE statements for better performance."""
        # Simple updates
        self.update_customer_balance_stmt = self.session.prepare(
            """
            UPDATE customer 
            SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ?
            WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
            """
        )
        
        self.update_stock_quantity_stmt = self.session.prepare(
            """
            UPDATE stock 
            SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?
            WHERE s_w_id = ? AND s_i_id = ?
            """
        )
        
        self.update_district_next_order_stmt = self.session.prepare(
            """
            UPDATE district 
            SET d_next_o_id = ?
            WHERE d_w_id = ? AND d_id = ?
            """
        )
        
        # Medium updates - conditional
        self.update_order_carrier_stmt = self.session.prepare(
            """
            UPDATE orders 
            SET o_carrier_id = ?
            WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?
            IF o_carrier_id = null
            """
        )
        
        self.update_customer_credit_stmt = self.session.prepare(
            """
            UPDATE customer 
            SET c_credit = ?, c_data = ?
            WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
            IF c_credit = ?
            """
        )
        
        # Complex updates - LWT with multiple conditions
        self.update_stock_with_lwt_stmt = self.session.prepare(
            """
            UPDATE stock 
            SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ?
            WHERE s_w_id = ? AND s_i_id = ?
            IF s_quantity >= ?
            """
        )
    
    # ========== SIMPLE UPDATE QUERIES ==========
    
    def update_customer_balance(self, warehouse_id: int, district_id: int, 
                               customer_id: int, balance: float, ytd_payment: float,
                               payment_cnt: int) -> bool:
        """
        U1: Update customer balance and payment info.
        Complexity: Simple - Basic update
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            balance: New balance
            ytd_payment: Year-to-date payment
            payment_cnt: Payment count
            
        Returns:
            True if successful
        """
        try:
            self.session.execute(self.update_customer_balance_stmt, [
                balance, ytd_payment, payment_cnt,
                warehouse_id, district_id, customer_id
            ])
            return True
        except Exception as e:
            print(f"Error updating customer balance: {e}")
            return False
    
    def update_stock_quantity(self, warehouse_id: int, item_id: int, 
                             quantity: int, ytd: int, order_cnt: int) -> bool:
        """
        U2: Update stock quantity and statistics.
        Complexity: Simple - Basic update
        
        Args:
            warehouse_id: Warehouse identifier
            item_id: Item identifier
            quantity: New quantity
            ytd: Year-to-date quantity
            order_cnt: Order count
            
        Returns:
            True if successful
        """
        try:
            self.session.execute(self.update_stock_quantity_stmt, [
                quantity, ytd, order_cnt,
                warehouse_id, item_id
            ])
            return True
        except Exception as e:
            print(f"Error updating stock quantity: {e}")
            return False
    
    def update_district_next_order(self, warehouse_id: int, district_id: int, 
                                   next_order_id: int) -> bool:
        """
        U3: Update district next order ID.
        Complexity: Simple - Basic update
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            next_order_id: Next order identifier
            
        Returns:
            True if successful
        """
        try:
            self.session.execute(self.update_district_next_order_stmt, [
                next_order_id, warehouse_id, district_id
            ])
            return True
        except Exception as e:
            print(f"Error updating district next order: {e}")
            return False
    
    # ========== MEDIUM UPDATE QUERIES ==========
    
    def update_order_carrier_conditional(self, warehouse_id: int, district_id: int, 
                                        order_id: int, carrier_id: int) -> Dict[str, Any]:
        """
        U4: Update order carrier only if not already assigned.
        Complexity: Medium - Conditional update
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            order_id: Order identifier
            carrier_id: Carrier identifier
            
        Returns:
            Dict with 'applied' boolean
        """
        try:
            result = self.session.execute(self.update_order_carrier_stmt, [
                carrier_id, warehouse_id, district_id, order_id
            ])
            row = result.one()
            return {
                'applied': row[0],
                'message': 'Update successful' if row[0] else 'Carrier already assigned'
            }
        except Exception as e:
            return {
                'applied': False,
                'message': f'Error: {e}'
            }
    
    def update_customer_credit_conditional(self, warehouse_id: int, district_id: int, 
                                          customer_id: int, new_credit: str, 
                                          credit_data: str, expected_credit: str) -> Dict[str, Any]:
        """
        U5: Update customer credit status conditionally.
        Complexity: Medium - Conditional update with data
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            new_credit: New credit status
            credit_data: Credit data text
            expected_credit: Expected current credit status
            
        Returns:
            Dict with 'applied' boolean
        """
        try:
            result = self.session.execute(self.update_customer_credit_stmt, [
                new_credit, credit_data,
                warehouse_id, district_id, customer_id,
                expected_credit
            ])
            row = result.one()
            return {
                'applied': row[0],
                'message': 'Update successful' if row[0] else 'Credit status mismatch'
            }
        except Exception as e:
            return {
                'applied': False,
                'message': f'Error: {e}'
            }
    
    def update_stocks_batch(self, updates: List[Dict[str, Any]]) -> bool:
        """
        U6: Batch update multiple stock records.
        Complexity: Medium - Batch operation
        
        Args:
            updates: List of stock update dictionaries
            
        Returns:
            True if successful
        """
        try:
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            
            for update in updates:
                batch.add(self.update_stock_quantity_stmt, [
                    update['quantity'], update['ytd'], update['order_cnt'],
                    update['warehouse_id'], update['item_id']
                ])
            
            self.session.execute(batch)
            return True
        except Exception as e:
            print(f"Error batch updating stocks: {e}")
            return False
    
    # ========== COMPLEX UPDATE QUERIES ==========
    
    def update_stock_with_lwt(self, warehouse_id: int, item_id: int, 
                             new_quantity: int, ytd: int, order_cnt: int, 
                             remote_cnt: int, min_quantity: int) -> Dict[str, Any]:
        """
        U7: Update stock with LWT to ensure quantity is sufficient.
        Complexity: Complex - LWT with business logic validation
        
        Args:
            warehouse_id: Warehouse identifier
            item_id: Item identifier
            new_quantity: New quantity
            ytd: Year-to-date quantity
            order_cnt: Order count
            remote_cnt: Remote count
            min_quantity: Minimum required quantity for update
            
        Returns:
            Dict with 'applied' boolean and current quantity
        """
        try:
            result = self.session.execute(self.update_stock_with_lwt_stmt, [
                new_quantity, ytd, order_cnt, remote_cnt,
                warehouse_id, item_id,
                min_quantity
            ])
            row = result.one()
            return {
                'applied': row[0],
                'message': 'Update successful' if row[0] else 'Insufficient stock quantity',
                'current_quantity': row.s_quantity if not row[0] else new_quantity
            }
        except Exception as e:
            return {
                'applied': False,
                'message': f'Error: {e}',
                'current_quantity': None
            }
    
    def update_order_and_customer_batch(self, order_update: Dict[str, Any], 
                                       customer_update: Dict[str, Any]) -> bool:
        """
        U8: Update order and customer in a batch (delivery transaction).
        Complexity: Complex - Multi-table batch update
        
        Args:
            order_update: Order update data
            customer_update: Customer update data
            
        Returns:
            True if successful
        """
        try:
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            
            # Update order carrier
            update_order = self.session.prepare(
                """
                UPDATE orders 
                SET o_carrier_id = ?
                WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?
                """
            )
            batch.add(update_order, [
                order_update['carrier_id'],
                order_update['warehouse_id'],
                order_update['district_id'],
                order_update['order_id']
            ])
            
            # Update customer balance
            batch.add(self.update_customer_balance_stmt, [
                customer_update['balance'],
                customer_update['ytd_payment'],
                customer_update['payment_cnt'],
                customer_update['warehouse_id'],
                customer_update['district_id'],
                customer_update['customer_id']
            ])
            
            self.session.execute(batch)
            return True
        except Exception as e:
            print(f"Error batch updating order and customer: {e}")
            return False

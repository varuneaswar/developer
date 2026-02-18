"""
INSERT queries for TPC-C benchmark.
Categorized by complexity: Simple, Medium, Complex
"""

from typing import Dict, Any, List
from cassandra.cluster import Session
from cassandra.query import BatchStatement, ConsistencyLevel
from datetime import datetime


class InsertQueries:
    """INSERT query definitions for TPC-C benchmark."""
    
    def __init__(self, session: Session):
        """
        Initialize INSERT queries with Cassandra session.
        
        Args:
            session: Active Cassandra session
        """
        self.session = session
        self._prepare_statements()
    
    def _prepare_statements(self) -> None:
        """Prepare all INSERT statements for better performance."""
        # Simple inserts
        self.insert_customer_stmt = self.session.prepare(
            """
            INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_middle, c_last,
                                 c_street_1, c_street_2, c_city, c_state, c_zip,
                                 c_phone, c_since, c_credit, c_credit_lim, c_discount,
                                 c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        
        self.insert_order_stmt = self.session.prepare(
            """
            INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_entry_d, 
                               o_carrier_id, o_ol_cnt, o_all_local)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        
        self.insert_order_by_customer_stmt = self.session.prepare(
            """
            INSERT INTO orders_by_customer (o_c_id, o_w_id, o_d_id, o_id, o_entry_d,
                                           o_carrier_id, o_ol_cnt, o_all_local)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        
        self.insert_history_stmt = self.session.prepare(
            """
            INSERT INTO history (h_c_w_id, h_c_d_id, h_c_id, h_w_id, h_d_id,
                                h_date, h_amount, h_data, date_bucket)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        
        # Medium inserts
        self.insert_order_line_stmt = self.session.prepare(
            """
            INSERT INTO order_line (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id,
                                   ol_supply_w_id, ol_delivery_d, ol_quantity, 
                                   ol_amount, ol_dist_info)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        
        self.insert_history_with_ttl_stmt = self.session.prepare(
            """
            INSERT INTO history (h_c_w_id, h_c_d_id, h_c_id, h_w_id, h_d_id,
                                h_date, h_amount, h_data, date_bucket)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            USING TTL ?
            """
        )
        
        # Complex inserts (LWT)
        self.insert_new_order_lwt_stmt = self.session.prepare(
            """
            INSERT INTO new_order (no_w_id, no_d_id, no_o_id)
            VALUES (?, ?, ?)
            IF NOT EXISTS
            """
        )
    
    # ========== SIMPLE INSERT QUERIES ==========
    
    def insert_customer(self, data: Dict[str, Any]) -> bool:
        """
        I1: Insert new customer.
        Complexity: Simple - Basic insert
        
        Args:
            data: Customer data dictionary
            
        Returns:
            True if successful
        """
        try:
            self.session.execute(self.insert_customer_stmt, [
                data['c_w_id'], data['c_d_id'], data['c_id'],
                data['c_first'], data['c_middle'], data['c_last'],
                data['c_street_1'], data['c_street_2'], data['c_city'],
                data['c_state'], data['c_zip'], data['c_phone'],
                data['c_since'], data['c_credit'], data['c_credit_lim'],
                data['c_discount'], data['c_balance'], data['c_ytd_payment'],
                data['c_payment_cnt'], data['c_delivery_cnt'], data['c_data']
            ])
            return True
        except Exception as e:
            print(f"Error inserting customer: {e}")
            return False
    
    def insert_order(self, data: Dict[str, Any]) -> bool:
        """
        I2: Insert new order.
        Complexity: Simple - Basic insert
        
        Args:
            data: Order data dictionary
            
        Returns:
            True if successful
        """
        try:
            # Insert into main orders table
            self.session.execute(self.insert_order_stmt, [
                data['o_w_id'], data['o_d_id'], data['o_id'],
                data['o_c_id'], data['o_entry_d'], data.get('o_carrier_id'),
                data['o_ol_cnt'], data['o_all_local']
            ])
            
            # Insert into denormalized orders_by_customer table
            self.session.execute(self.insert_order_by_customer_stmt, [
                data['o_c_id'], data['o_w_id'], data['o_d_id'],
                data['o_id'], data['o_entry_d'], data.get('o_carrier_id'),
                data['o_ol_cnt'], data['o_all_local']
            ])
            return True
        except Exception as e:
            print(f"Error inserting order: {e}")
            return False
    
    def insert_history(self, data: Dict[str, Any]) -> bool:
        """
        I3: Insert history record.
        Complexity: Simple - Basic insert with time-series pattern
        
        Args:
            data: History data dictionary
            
        Returns:
            True if successful
        """
        try:
            # Generate date bucket from h_date
            if isinstance(data['h_date'], datetime):
                date_bucket = data['h_date'].strftime('%Y-%m-%d')
            else:
                date_bucket = data.get('date_bucket', datetime.now().strftime('%Y-%m-%d'))
            
            self.session.execute(self.insert_history_stmt, [
                data['h_c_w_id'], data['h_c_d_id'], data['h_c_id'],
                data['h_w_id'], data['h_d_id'], data['h_date'],
                data['h_amount'], data['h_data'], date_bucket
            ])
            return True
        except Exception as e:
            print(f"Error inserting history: {e}")
            return False
    
    # ========== MEDIUM INSERT QUERIES ==========
    
    def insert_order_lines_batch(self, order_lines: List[Dict[str, Any]]) -> bool:
        """
        I4: Batch insert order lines.
        Complexity: Medium - Batch operation
        
        Args:
            order_lines: List of order line data dictionaries
            
        Returns:
            True if successful
        """
        try:
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            
            for line in order_lines:
                batch.add(self.insert_order_line_stmt, [
                    line['ol_w_id'], line['ol_d_id'], line['ol_o_id'],
                    line['ol_number'], line['ol_i_id'], line['ol_supply_w_id'],
                    line.get('ol_delivery_d'), line['ol_quantity'],
                    line['ol_amount'], line['ol_dist_info']
                ])
            
            self.session.execute(batch)
            return True
        except Exception as e:
            print(f"Error batch inserting order lines: {e}")
            return False
    
    def insert_history_with_ttl(self, data: Dict[str, Any], ttl_seconds: int) -> bool:
        """
        I5: Insert history record with TTL.
        Complexity: Medium - Insert with TTL for data expiration
        
        Args:
            data: History data dictionary
            ttl_seconds: Time to live in seconds
            
        Returns:
            True if successful
        """
        try:
            # Generate date bucket from h_date
            if isinstance(data['h_date'], datetime):
                date_bucket = data['h_date'].strftime('%Y-%m-%d')
            else:
                date_bucket = data.get('date_bucket', datetime.now().strftime('%Y-%m-%d'))
            
            self.session.execute(self.insert_history_with_ttl_stmt, [
                data['h_c_w_id'], data['h_c_d_id'], data['h_c_id'],
                data['h_w_id'], data['h_d_id'], data['h_date'],
                data['h_amount'], data['h_data'], date_bucket,
                ttl_seconds
            ])
            return True
        except Exception as e:
            print(f"Error inserting history with TTL: {e}")
            return False
    
    # ========== COMPLEX INSERT QUERIES ==========
    
    def insert_new_order_lwt(self, warehouse_id: int, district_id: int, 
                            order_id: int) -> Dict[str, Any]:
        """
        I6: Insert new order using Lightweight Transaction (LWT).
        Complexity: Complex - LWT for idempotency
        
        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            order_id: Order identifier
            
        Returns:
            Dict with 'applied' boolean and result data
        """
        try:
            result = self.session.execute(
                self.insert_new_order_lwt_stmt,
                [warehouse_id, district_id, order_id]
            )
            row = result.one()
            return {
                'applied': row[0],  # First column is [applied]
                'message': 'Insert successful' if row[0] else 'Record already exists'
            }
        except Exception as e:
            return {
                'applied': False,
                'message': f'Error: {e}'
            }
    
    def insert_customer_with_denormalization(self, data: Dict[str, Any]) -> bool:
        """
        I7: Insert customer into both tables (customer and customer_by_name).
        Complexity: Complex - Insert with denormalization
        
        Args:
            data: Customer data dictionary
            
        Returns:
            True if successful
        """
        try:
            # Insert into main customer table
            self.insert_customer(data)
            
            # Insert into customer_by_name table
            insert_by_name = self.session.prepare(
                """
                INSERT INTO customer_by_name (c_w_id, c_d_id, c_last, c_first, c_id,
                                             c_middle, c_street_1, c_street_2, c_city,
                                             c_state, c_zip, c_phone, c_since, c_credit,
                                             c_credit_lim, c_discount, c_balance, 
                                             c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            )
            
            self.session.execute(insert_by_name, [
                data['c_w_id'], data['c_d_id'], data['c_last'], data['c_first'], data['c_id'],
                data['c_middle'], data['c_street_1'], data['c_street_2'], data['c_city'],
                data['c_state'], data['c_zip'], data['c_phone'], data['c_since'],
                data['c_credit'], data['c_credit_lim'], data['c_discount'],
                data['c_balance'], data['c_ytd_payment'], data['c_payment_cnt'],
                data['c_delivery_cnt'], data['c_data']
            ])
            return True
        except Exception as e:
            print(f"Error inserting customer with denormalization: {e}")
            return False

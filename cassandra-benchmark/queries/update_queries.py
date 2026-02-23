"""
UPDATE queries for TPC-C benchmark.
Categorized by complexity: Simple, Medium, Complex
"""

from typing import Any, Dict, List

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

    def update_customer_balance(
        self,
        warehouse_id: int,
        district_id: int,
        customer_id: int,
        balance: float,
        ytd_payment: float,
        payment_cnt: int,
    ) -> bool:
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
            self.session.execute(
                self.update_customer_balance_stmt,
                [balance, ytd_payment, payment_cnt, warehouse_id, district_id, customer_id],
            )
            return True
        except Exception as e:
            print(f"Error updating customer balance: {e}")
            return False

    def update_stock_quantity(
        self, warehouse_id: int, item_id: int, quantity: int, ytd: int, order_cnt: int
    ) -> bool:
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
            self.session.execute(
                self.update_stock_quantity_stmt, [quantity, ytd, order_cnt, warehouse_id, item_id]
            )
            return True
        except Exception as e:
            print(f"Error updating stock quantity: {e}")
            return False

    def update_district_next_order(
        self, warehouse_id: int, district_id: int, next_order_id: int
    ) -> bool:
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
            self.session.execute(
                self.update_district_next_order_stmt, [next_order_id, warehouse_id, district_id]
            )
            return True
        except Exception as e:
            print(f"Error updating district next order: {e}")
            return False

    # ========== MEDIUM UPDATE QUERIES ==========

    def update_order_carrier_conditional(
        self, warehouse_id: int, district_id: int, order_id: int, carrier_id: int
    ) -> Dict[str, Any]:
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
            result = self.session.execute(
                self.update_order_carrier_stmt, [carrier_id, warehouse_id, district_id, order_id]
            )
            row = result.one()
            return {
                "applied": row[0],
                "message": "Update successful" if row[0] else "Carrier already assigned",
            }
        except Exception as e:
            return {"applied": False, "message": f"Error: {e}"}

    def update_customer_credit_conditional(
        self,
        warehouse_id: int,
        district_id: int,
        customer_id: int,
        new_credit: str,
        credit_data: str,
        expected_credit: str,
    ) -> Dict[str, Any]:
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
            result = self.session.execute(
                self.update_customer_credit_stmt,
                [new_credit, credit_data, warehouse_id, district_id, customer_id, expected_credit],
            )
            row = result.one()
            return {
                "applied": row[0],
                "message": "Update successful" if row[0] else "Credit status mismatch",
            }
        except Exception as e:
            return {"applied": False, "message": f"Error: {e}"}

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
                batch.add(
                    self.update_stock_quantity_stmt,
                    [
                        update["quantity"],
                        update["ytd"],
                        update["order_cnt"],
                        update["warehouse_id"],
                        update["item_id"],
                    ],
                )

            self.session.execute(batch)
            return True
        except Exception as e:
            print(f"Error batch updating stocks: {e}")
            return False

    # ========== COMPLEX UPDATE QUERIES ==========

    def update_stock_with_lwt(
        self,
        warehouse_id: int,
        item_id: int,
        new_quantity: int,
        ytd: int,
        order_cnt: int,
        remote_cnt: int,
        min_quantity: int,
    ) -> Dict[str, Any]:
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
            result = self.session.execute(
                self.update_stock_with_lwt_stmt,
                [new_quantity, ytd, order_cnt, remote_cnt, warehouse_id, item_id, min_quantity],
            )
            row = result.one()
            return {
                "applied": row[0],
                "message": "Update successful" if row[0] else "Insufficient stock quantity",
                "current_quantity": row.s_quantity if not row[0] else new_quantity,
            }
        except Exception as e:
            return {"applied": False, "message": f"Error: {e}", "current_quantity": None}

    def update_order_and_customer_batch(
        self, order_update: Dict[str, Any], customer_update: Dict[str, Any]
    ) -> bool:
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
            batch.add(
                update_order,
                [
                    order_update["carrier_id"],
                    order_update["warehouse_id"],
                    order_update["district_id"],
                    order_update["order_id"],
                ],
            )

            # Update customer balance
            batch.add(
                self.update_customer_balance_stmt,
                [
                    customer_update["balance"],
                    customer_update["ytd_payment"],
                    customer_update["payment_cnt"],
                    customer_update["warehouse_id"],
                    customer_update["district_id"],
                    customer_update["customer_id"],
                ],
            )

            self.session.execute(batch)
            return True
        except Exception as e:
            print(f"Error batch updating order and customer: {e}")
            return False

    # ========== ADDITIONAL UPDATE QUERIES (U9-U23) ==========

    def update_customer_add_phone(
        self, warehouse_id: int, district_id: int, customer_id: int, new_phone: str
    ) -> bool:
        """
        U9: Update customer by adding to phone numbers set.
        Complexity: Medium - Set collection append
        Cassandra Concept: Collection operations (set add)

        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            new_phone: New phone number to add

        Returns:
            True if successful
        """
        try:
            query = """
                UPDATE customer_extended
                SET c_phone_numbers = c_phone_numbers + ?
                WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
            """
            self.session.execute(query, [{new_phone}, warehouse_id, district_id, customer_id])
            return True
        except Exception as e:
            print(f"Error updating set collection: {e}")
            return False

    def update_customer_preferences_map(
        self, warehouse_id: int, district_id: int, customer_id: int, prefs_update: dict
    ) -> bool:
        """
        U10: Update customer preferences map.
        Complexity: Medium - Map collection update
        Cassandra Concept: Map collection operations

        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            prefs_update: Dictionary of preference updates

        Returns:
            True if successful
        """
        try:
            query = """
                UPDATE customer_extended
                SET c_preferences = c_preferences + ?
                WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
            """
            self.session.execute(query, [prefs_update, warehouse_id, district_id, customer_id])
            return True
        except Exception as e:
            print(f"Error updating map collection: {e}")
            return False

    def update_customer_append_email(
        self, warehouse_id: int, district_id: int, customer_id: int, new_email: str
    ) -> bool:
        """
        U11: Update customer by appending to email history list.
        Complexity: Medium - List collection append
        Cassandra Concept: List collection operations (append)

        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            new_email: New email to append

        Returns:
            True if successful
        """
        try:
            query = """
                UPDATE customer_extended
                SET c_email_history = c_email_history + ?
                WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
            """
            self.session.execute(query, [[new_email], warehouse_id, district_id, customer_id])
            return True
        except Exception as e:
            print(f"Error updating list collection: {e}")
            return False

    def update_customer_remove_phone(
        self, warehouse_id: int, district_id: int, customer_id: int, phone_to_remove: str
    ) -> bool:
        """
        U12: Update customer by removing from phone numbers set.
        Complexity: Medium - Set collection remove
        Cassandra Concept: Set collection remove operation

        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            phone_to_remove: Phone number to remove

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
            print(f"Error removing from set: {e}")
            return False

    def update_order_with_ttl(
        self, warehouse_id: int, district_id: int, order_id: int, carrier_id: int, ttl_seconds: int
    ) -> bool:
        """
        U13: Update order carrier with TTL.
        Complexity: Medium - Update with TTL
        Cassandra Concept: TTL on updates

        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            order_id: Order identifier
            carrier_id: Carrier identifier
            ttl_seconds: Time to live in seconds

        Returns:
            True if successful
        """
        try:
            query = """
                UPDATE orders
                SET o_carrier_id = ?
                WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?
                USING TTL ?
            """
            self.session.execute(
                query, [carrier_id, warehouse_id, district_id, order_id, ttl_seconds]
            )
            return True
        except Exception as e:
            print(f"Error updating with TTL: {e}")
            return False

    def update_customer_with_timestamp(
        self,
        warehouse_id: int,
        district_id: int,
        customer_id: int,
        balance: float,
        timestamp_micros: int,
    ) -> bool:
        """
        U14: Update customer balance with custom timestamp.
        Complexity: Medium - Update with timestamp
        Cassandra Concept: USING TIMESTAMP

        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            balance: New balance
            timestamp_micros: Timestamp in microseconds

        Returns:
            True if successful
        """
        try:
            query = """
                UPDATE customer
                SET c_balance = ?
                WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
                USING TIMESTAMP ?
            """
            self.session.execute(
                query, [balance, warehouse_id, district_id, customer_id, timestamp_micros]
            )
            return True
        except Exception as e:
            print(f"Error updating with timestamp: {e}")
            return False

    def update_warehouse_metrics_counter(
        self, warehouse_id: int, metric_name: str, increment: int
    ) -> bool:
        """
        U15: Update warehouse metrics counter.
        Complexity: Simple - Counter update
        Cassandra Concept: Counter column increment

        Args:
            warehouse_id: Warehouse identifier
            metric_name: Name of metric
            increment: Amount to increment (can be negative)

        Returns:
            True if successful
        """
        try:
            query = """
                UPDATE warehouse_metrics
                SET metric_value = metric_value + ?
                WHERE w_id = ? AND metric_name = ?
            """
            self.session.execute(query, [increment, warehouse_id, metric_name])
            return True
        except Exception as e:
            print(f"Error updating counter: {e}")
            return False

    def update_multiple_customer_fields(
        self, warehouse_id: int, district_id: int, customer_id: int, updates: Dict[str, Any]
    ) -> bool:
        """
        U16: Update multiple customer columns.
        Complexity: Medium - Multiple column update
        Cassandra Concept: Multi-column updates

        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            updates: Dictionary of fields to update

        Returns:
            True if successful
        """
        try:
            # Build dynamic UPDATE query
            set_clauses = ", ".join([f"{key} = ?" for key in updates.keys()])
            query = f"""
                UPDATE customer
                SET {set_clauses}
                WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
            """
            params = list(updates.values()) + [warehouse_id, district_id, customer_id]
            self.session.execute(query, params)
            return True
        except Exception as e:
            print(f"Error updating multiple fields: {e}")
            return False

    def update_customer_with_collection_and_ttl(
        self, warehouse_id: int, district_id: int, customer_id: int, tag: str, ttl_seconds: int
    ) -> bool:
        """
        U17: Update customer tags with TTL.
        Complexity: Complex - Collection update with TTL
        Cassandra Concept: Collection + TTL

        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            tag: Tag to add
            ttl_seconds: Time to live in seconds

        Returns:
            True if successful
        """
        try:
            query = """
                UPDATE customer_extended
                SET c_tags = c_tags + ?
                WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
                USING TTL ?
            """
            self.session.execute(
                query, [{tag}, warehouse_id, district_id, customer_id, ttl_seconds]
            )
            return True
        except Exception as e:
            print(f"Error updating collection with TTL: {e}")
            return False

    def update_static_column(self, category_id: int, new_description: str) -> bool:
        """
        U18: Update static column in product catalog.
        Complexity: Medium - Static column update
        Cassandra Concept: Static column updates (affects all rows in partition)

        Args:
            category_id: Category identifier
            new_description: New category description

        Returns:
            True if successful
        """
        try:
            query = """
                UPDATE product_catalog
                SET category_description = ?
                WHERE category_id = ?
            """
            self.session.execute(query, [new_description, category_id])
            return True
        except Exception as e:
            print(f"Error updating static column: {e}")
            return False

    def update_with_lwt_multiple_conditions(
        self,
        warehouse_id: int,
        district_id: int,
        customer_id: int,
        new_balance: float,
        expected_balance: float,
        expected_credit: str,
    ) -> Dict[str, Any]:
        """
        U19: Update customer with multiple LWT conditions.
        Complexity: Complex - LWT with multiple IF conditions
        Cassandra Concept: Lightweight Transactions with multiple conditions

        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            new_balance: New balance value
            expected_balance: Expected current balance
            expected_credit: Expected credit status

        Returns:
            Dict with 'applied' boolean
        """
        try:
            query = """
                UPDATE customer
                SET c_balance = ?
                WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
                IF c_balance = ? AND c_credit = ?
            """
            result = self.session.execute(
                query,
                [
                    new_balance,
                    warehouse_id,
                    district_id,
                    customer_id,
                    expected_balance,
                    expected_credit,
                ],
            )
            row = result.one()
            return {
                "applied": row[0],
                "message": "Update successful" if row[0] else "Conditions not met",
                "current_balance": row.c_balance if not row[0] else new_balance,
                "current_credit": row.c_credit if not row[0] else expected_credit,
            }
        except Exception as e:
            return {"applied": False, "message": f"Error: {e}"}

    def update_batch_unlogged(self, updates: List[Dict[str, Any]]) -> bool:
        """
        U20: Batch update multiple records (UNLOGGED).
        Complexity: Complex - UNLOGGED batch updates
        Cassandra Concept: UNLOGGED batch for performance

        Args:
            updates: List of update dictionaries

        Returns:
            True if successful
        """
        try:
            batch_query = "BEGIN UNLOGGED BATCH\n"
            params = []

            for update in updates:
                batch_query += """
                    UPDATE stock
                    SET s_quantity = ?
                    WHERE s_w_id = ? AND s_i_id = ?;
                """
                params.extend([update["quantity"], update["warehouse_id"], update["item_id"]])

            batch_query += "APPLY BATCH;"

            self.session.execute(batch_query, params)
            return True
        except Exception as e:
            print(f"Error in unlogged batch update: {e}")
            return False

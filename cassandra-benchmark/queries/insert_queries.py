"""
INSERT queries for TPC-C benchmark.
Categorized by complexity: Simple, Medium, Complex
"""

from datetime import datetime
from typing import Any, Dict, List

from cassandra.cluster import Session
from cassandra.query import BatchStatement, ConsistencyLevel


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
            self.session.execute(
                self.insert_customer_stmt,
                [
                    data["c_w_id"],
                    data["c_d_id"],
                    data["c_id"],
                    data["c_first"],
                    data["c_middle"],
                    data["c_last"],
                    data["c_street_1"],
                    data["c_street_2"],
                    data["c_city"],
                    data["c_state"],
                    data["c_zip"],
                    data["c_phone"],
                    data["c_since"],
                    data["c_credit"],
                    data["c_credit_lim"],
                    data["c_discount"],
                    data["c_balance"],
                    data["c_ytd_payment"],
                    data["c_payment_cnt"],
                    data["c_delivery_cnt"],
                    data["c_data"],
                ],
            )
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
            self.session.execute(
                self.insert_order_stmt,
                [
                    data["o_w_id"],
                    data["o_d_id"],
                    data["o_id"],
                    data["o_c_id"],
                    data["o_entry_d"],
                    data.get("o_carrier_id"),
                    data["o_ol_cnt"],
                    data["o_all_local"],
                ],
            )

            # Insert into denormalized orders_by_customer table
            self.session.execute(
                self.insert_order_by_customer_stmt,
                [
                    data["o_c_id"],
                    data["o_w_id"],
                    data["o_d_id"],
                    data["o_id"],
                    data["o_entry_d"],
                    data.get("o_carrier_id"),
                    data["o_ol_cnt"],
                    data["o_all_local"],
                ],
            )
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
            if isinstance(data["h_date"], datetime):
                date_bucket = data["h_date"].strftime("%Y-%m-%d")
            else:
                date_bucket = data.get("date_bucket", datetime.now().strftime("%Y-%m-%d"))

            self.session.execute(
                self.insert_history_stmt,
                [
                    data["h_c_w_id"],
                    data["h_c_d_id"],
                    data["h_c_id"],
                    data["h_w_id"],
                    data["h_d_id"],
                    data["h_date"],
                    data["h_amount"],
                    data["h_data"],
                    date_bucket,
                ],
            )
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
                batch.add(
                    self.insert_order_line_stmt,
                    [
                        line["ol_w_id"],
                        line["ol_d_id"],
                        line["ol_o_id"],
                        line["ol_number"],
                        line["ol_i_id"],
                        line["ol_supply_w_id"],
                        line.get("ol_delivery_d"),
                        line["ol_quantity"],
                        line["ol_amount"],
                        line["ol_dist_info"],
                    ],
                )

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
            if isinstance(data["h_date"], datetime):
                date_bucket = data["h_date"].strftime("%Y-%m-%d")
            else:
                date_bucket = data.get("date_bucket", datetime.now().strftime("%Y-%m-%d"))

            self.session.execute(
                self.insert_history_with_ttl_stmt,
                [
                    data["h_c_w_id"],
                    data["h_c_d_id"],
                    data["h_c_id"],
                    data["h_w_id"],
                    data["h_d_id"],
                    data["h_date"],
                    data["h_amount"],
                    data["h_data"],
                    date_bucket,
                    ttl_seconds,
                ],
            )
            return True
        except Exception as e:
            print(f"Error inserting history with TTL: {e}")
            return False

    # ========== COMPLEX INSERT QUERIES ==========

    def insert_new_order_lwt(
        self, warehouse_id: int, district_id: int, order_id: int
    ) -> Dict[str, Any]:
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
                self.insert_new_order_lwt_stmt, [warehouse_id, district_id, order_id]
            )
            row = result.one()
            return {
                "applied": row[0],  # First column is [applied]
                "message": "Insert successful" if row[0] else "Record already exists",
            }
        except Exception as e:
            return {"applied": False, "message": f"Error: {e}"}

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

            self.session.execute(
                insert_by_name,
                [
                    data["c_w_id"],
                    data["c_d_id"],
                    data["c_last"],
                    data["c_first"],
                    data["c_id"],
                    data["c_middle"],
                    data["c_street_1"],
                    data["c_street_2"],
                    data["c_city"],
                    data["c_state"],
                    data["c_zip"],
                    data["c_phone"],
                    data["c_since"],
                    data["c_credit"],
                    data["c_credit_lim"],
                    data["c_discount"],
                    data["c_balance"],
                    data["c_ytd_payment"],
                    data["c_payment_cnt"],
                    data["c_delivery_cnt"],
                    data["c_data"],
                ],
            )
            return True
        except Exception as e:
            print(f"Error inserting customer with denormalization: {e}")
            return False

    # ========== ADDITIONAL INSERT QUERIES (I8-I20) ==========

    def insert_customer_with_collections(
        self,
        warehouse_id: int,
        district_id: int,
        customer_id: int,
        name: str,
        phones: set,
        emails: list,
        prefs: dict,
    ) -> bool:
        """
        I8: Insert customer with collection types.
        Complexity: Medium - Collections (set, list, map)
        Cassandra Concept: Collection data types

        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            name: Customer name
            phones: Set of phone numbers
            emails: List of email addresses
            prefs: Map of preferences

        Returns:
            True if successful
        """
        try:
            query = """
                INSERT INTO customer_extended
                (c_w_id, c_d_id, c_id, c_name, c_phone_numbers, c_email_history, c_preferences, c_created)  # noqa: E501
                VALUES (?, ?, ?, ?, ?, ?, ?, toTimestamp(now()))
            """
            self.session.execute(
                query, [warehouse_id, district_id, customer_id, name, phones, emails, prefs]
            )
            return True
        except Exception as e:
            print(f"Error inserting customer with collections: {e}")
            return False

    def insert_warehouse_metric_counter(
        self, warehouse_id: int, metric_name: str, increment: int = 1
    ) -> bool:
        """
        I9: Insert/increment counter for warehouse metrics.
        Complexity: Simple - Counter column update
        Cassandra Concept: Counter columns

        Args:
            warehouse_id: Warehouse identifier
            metric_name: Name of the metric
            increment: Amount to increment

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

    def insert_customer_with_udt(
        self, warehouse_id: int, district_id: int, customer_id: int, name: str, address_data: dict
    ) -> bool:
        """
        I10: Insert customer with User Defined Type.
        Complexity: Medium - UDT (User Defined Type)
        Cassandra Concept: User Defined Types

        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            customer_id: Customer identifier
            name: Customer name
            address_data: Address as dict with street_1, street_2, city, state, zip

        Returns:
            True if successful
        """
        try:
            query = """
                INSERT INTO customer_extended
                (c_w_id, c_d_id, c_id, c_name, c_address, c_created)
                VALUES (?, ?, ?, ?, ?, toTimestamp(now()))
            """
            # Note: In actual implementation, would need to create UDT instance
            # For now, using dict representation
            self.session.execute(
                query, [warehouse_id, district_id, customer_id, name, address_data]
            )
            return True
        except Exception as e:
            print(f"Error inserting with UDT: {e}")
            return False

    def insert_product_with_static(
        self, category_id: int, category_name: str, product_id: int, product_name: str, tags: set
    ) -> bool:
        """
        I11: Insert product with static column.
        Complexity: Medium - Static columns
        Cassandra Concept: Static columns (shared across partition)

        Args:
            category_id: Category identifier
            category_name: Category name (static column)
            product_id: Product identifier
            product_name: Product name
            tags: Product tags

        Returns:
            True if successful
        """
        try:
            query = """
                INSERT INTO product_catalog
                (category_id, category_name, product_id, product_name, product_tags)
                VALUES (?, ?, ?, ?, ?)
            """
            self.session.execute(
                query, [category_id, category_name, product_id, product_name, tags]
            )
            return True
        except Exception as e:
            print(f"Error inserting with static column: {e}")
            return False

    def insert_inventory_log_with_ttl(
        self, warehouse_id: int, item_id: int, log_type: str, message: str, ttl_seconds: int
    ) -> bool:
        """
        I12: Insert inventory log with TTL.
        Complexity: Medium - TTL (Time To Live)
        Cassandra Concept: TTL for automatic data expiration

        Args:
            warehouse_id: Warehouse identifier
            item_id: Item identifier
            log_type: Type of log entry
            message: Log message
            ttl_seconds: Time to live in seconds

        Returns:
            True if successful
        """
        try:
            query = """
                INSERT INTO inventory_log
                (i_w_id, i_id, log_timestamp, log_type, log_message)
                VALUES (?, ?, toTimestamp(now()), ?, ?)
                USING TTL ?
            """
            self.session.execute(query, [warehouse_id, item_id, log_type, message, ttl_seconds])
            return True
        except Exception as e:
            print(f"Error inserting with TTL: {e}")
            return False

    def insert_orders_batch_logged(self, orders: List[Dict[str, Any]]) -> bool:
        """
        I13: Batch insert orders (LOGGED batch).
        Complexity: Medium - LOGGED batch for atomicity
        Cassandra Concept: LOGGED batch (atomic, slower)

        Args:
            orders: List of order data dictionaries

        Returns:
            True if successful
        """
        try:
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

            for order in orders:
                batch.add(
                    self.insert_order_stmt,
                    [
                        order["o_w_id"],
                        order["o_d_id"],
                        order["o_id"],
                        order["o_c_id"],
                        order["o_entry_d"],
                        order.get("o_carrier_id"),
                        order["o_ol_cnt"],
                        order["o_all_local"],
                    ],
                )

            self.session.execute(batch)
            return True
        except Exception as e:
            print(f"Error in logged batch insert: {e}")
            return False

    def insert_order_tracking_batch_unlogged(self, tracking_records: List[Dict[str, Any]]) -> bool:
        """
        I14: Batch insert order tracking (UNLOGGED batch).
        Complexity: Medium - UNLOGGED batch for performance
        Cassandra Concept: UNLOGGED batch (faster, no atomicity guarantee)

        Args:
            tracking_records: List of tracking data dictionaries

        Returns:
            True if successful
        """
        try:
            # Create UNLOGGED batch
            batch_query = "BEGIN UNLOGGED BATCH\n"
            params = []

            for record in tracking_records:
                batch_query += """
                    INSERT INTO order_tracking
                    (o_w_id, o_status, o_timestamp, o_id, o_details)
                    VALUES (?, ?, ?, ?, ?);
                """
                params.extend(
                    [
                        record["o_w_id"],
                        record["o_status"],
                        record["o_timestamp"],
                        record["o_id"],
                        record["o_details"],
                    ]
                )

            batch_query += "APPLY BATCH;"

            self.session.execute(batch_query, params)
            return True
        except Exception as e:
            print(f"Error in unlogged batch insert: {e}")
            return False

    def insert_order_with_timestamp(self, data: Dict[str, Any], timestamp_micros: int) -> bool:
        """
        I15: Insert order with custom timestamp.
        Complexity: Medium - Custom timestamp
        Cassandra Concept: USING TIMESTAMP for write-time control

        Args:
            data: Order data dictionary
            timestamp_micros: Timestamp in microseconds

        Returns:
            True if successful
        """
        try:
            query = """
                INSERT INTO orders
                (o_w_id, o_d_id, o_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                USING TIMESTAMP ?
            """
            self.session.execute(
                query,
                [
                    data["o_w_id"],
                    data["o_d_id"],
                    data["o_id"],
                    data["o_c_id"],
                    data["o_entry_d"],
                    data.get("o_carrier_id"),
                    data["o_ol_cnt"],
                    data["o_all_local"],
                    timestamp_micros,
                ],
            )
            return True
        except Exception as e:
            print(f"Error inserting with timestamp: {e}")
            return False

    def insert_item_all_types(
        self, item_id: int, name: str, price: float, tags: set, specs: dict, reviews: list
    ) -> bool:
        """
        I16: Insert item with all collection types.
        Complexity: Complex - Multiple collection types
        Cassandra Concept: Set, Map, List collections together

        Args:
            item_id: Item identifier
            name: Item name
            price: Item price
            tags: Set of tags
            specs: Map of specifications
            reviews: List of reviews

        Returns:
            True if successful
        """
        try:
            query = """
                INSERT INTO product_catalog
                (category_id, product_id, product_name, product_tags, product_specs, product_reviews)  # noqa: E501
                VALUES (?, ?, ?, ?, ?, ?)
            """
            # Using item_id % 10 as category_id for distribution
            category_id = item_id % 10
            self.session.execute(query, [category_id, item_id, name, tags, specs, reviews])
            return True
        except Exception as e:
            print(f"Error inserting with all collection types: {e}")
            return False

    def insert_into_multiple_tables(self, order_data: Dict[str, Any]) -> bool:
        """
        I17: Insert order into multiple denormalized tables.
        Complexity: Complex - Multi-table denormalization pattern
        Cassandra Concept: Denormalization for query patterns

        Args:
            order_data: Order data dictionary

        Returns:
            True if successful
        """
        try:
            # Insert into orders table
            query1 = """
                INSERT INTO orders
                (o_w_id, o_d_id, o_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            self.session.execute(
                query1,
                [
                    order_data["o_w_id"],
                    order_data["o_d_id"],
                    order_data["o_id"],
                    order_data["o_c_id"],
                    order_data["o_entry_d"],
                    order_data.get("o_carrier_id"),
                    order_data["o_ol_cnt"],
                    order_data["o_all_local"],
                ],
            )

            # Insert into orders_by_customer table
            query2 = """
                INSERT INTO orders_by_customer
                (o_c_id, o_w_id, o_d_id, o_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            self.session.execute(
                query2,
                [
                    order_data["o_c_id"],
                    order_data["o_w_id"],
                    order_data["o_d_id"],
                    order_data["o_id"],
                    order_data["o_entry_d"],
                    order_data.get("o_carrier_id"),
                    order_data["o_ol_cnt"],
                    order_data["o_all_local"],
                ],
            )

            # Insert into order_tracking table
            query3 = """
                INSERT INTO order_tracking
                (o_w_id, o_status, o_timestamp, o_id, o_details)
                VALUES (?, ?, ?, ?, ?)
            """
            self.session.execute(
                query3,
                [
                    order_data["o_w_id"],
                    "CREATED",
                    order_data["o_entry_d"],
                    order_data["o_id"],
                    f"Order for customer {order_data['o_c_id']}",
                ],
            )

            return True
        except Exception as e:
            print(f"Error inserting into multiple tables: {e}")
            return False

    def insert_with_lwt_condition(
        self,
        warehouse_id: int,
        district_id: int,
        order_id: int,
        customer_id: int,
        expected_value: int = None,
    ) -> Dict[str, Any]:
        """
        I18: Insert order with LWT condition variant.
        Complexity: Complex - LWT with IF condition
        Cassandra Concept: Lightweight Transactions with conditions

        Args:
            warehouse_id: Warehouse identifier
            district_id: District identifier
            order_id: Order identifier
            customer_id: Customer identifier
            expected_value: Expected value for condition

        Returns:
            Dict with 'applied' boolean
        """
        try:
            query = """
                INSERT INTO new_order (no_w_id, no_d_id, no_o_id)
                VALUES (?, ?, ?)
                IF NOT EXISTS
            """
            result = self.session.execute(query, [warehouse_id, district_id, order_id])
            row = result.one()
            return {
                "applied": row[0],
                "message": "Insert successful" if row[0] else "Record already exists",
            }
        except Exception as e:
            return {"applied": False, "message": f"Error: {e}"}

    def insert_customer_activity_json(self, customer_id: int, activity_json: str) -> bool:
        """
        I19: Insert customer activity from JSON.
        Complexity: Medium - JSON insert
        Cassandra Concept: JSON support

        Args:
            customer_id: Customer identifier
            activity_json: JSON string with activity data

        Returns:
            True if successful
        """
        try:
            query = f"""
                INSERT INTO customer_activity JSON '{activity_json}'
            """
            self.session.execute(query)
            return True
        except Exception as e:
            print(f"Error inserting from JSON: {e}")
            return False

    def increment_warehouse_counter(
        self,
        warehouse_id: int,
        stat_date: str,
        orders_increment: int = 1,
        revenue_increment: int = 0,
    ) -> bool:
        """
        I20: Increment warehouse statistics counters.
        Complexity: Simple - Counter increment operations
        Cassandra Concept: Counter column updates

        Args:
            warehouse_id: Warehouse identifier
            stat_date: Statistics date
            orders_increment: Amount to increment order count
            revenue_increment: Amount to increment revenue

        Returns:
            True if successful
        """
        try:
            query = """
                UPDATE warehouse_stats
                SET total_orders = total_orders + ?, total_revenue = total_revenue + ?
                WHERE w_id = ? AND stat_date = ?
            """
            self.session.execute(
                query, [orders_increment, revenue_increment, warehouse_id, stat_date]
            )
            return True
        except Exception as e:
            print(f"Error incrementing counters: {e}")
            return False

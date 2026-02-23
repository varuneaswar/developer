"""
Data loader for TPC-C benchmark.
Handles bulk loading of generated data into Cassandra.
"""

import logging

from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from data_generator.tpcc_data_generator import TPCCDataGenerator

logger = logging.getLogger(__name__)


class DataLoader:
    """Loads TPC-C data into Cassandra."""

    def __init__(self, session: Session, data_generator: TPCCDataGenerator):
        """
        Initialize data loader.

        Args:
            session: Active Cassandra session
            data_generator: TPC-C data generator instance
        """
        self.session = session
        self.generator = data_generator
        self._prepare_statements()

    def _prepare_statements(self) -> None:
        """Prepare all insert statements."""
        # Warehouse
        self.insert_warehouse = self.session.prepare(
            """
            INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city,
                                  w_state, w_zip, w_tax, w_ytd)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        # District
        self.insert_district = self.session.prepare(
            """
            INSERT INTO district (d_w_id, d_id, d_name, d_street_1, d_street_2,
                                 d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        # Customer
        self.insert_customer = self.session.prepare(
            """
            INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_middle, c_last,
                                 c_street_1, c_street_2, c_city, c_state, c_zip,
                                 c_phone, c_since, c_credit, c_credit_lim, c_discount,
                                 c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        # Customer by name
        self.insert_customer_by_name = self.session.prepare(
            """
            INSERT INTO customer_by_name (c_w_id, c_d_id, c_last, c_first, c_id,
                                         c_middle, c_street_1, c_street_2, c_city,
                                         c_state, c_zip, c_phone, c_since, c_credit,
                                         c_credit_lim, c_discount, c_balance,
                                         c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        # Item
        self.insert_item = self.session.prepare(
            """
            INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data)
            VALUES (?, ?, ?, ?, ?)
            """
        )

        # Stock
        self.insert_stock = self.session.prepare(
            """
            INSERT INTO stock (s_w_id, s_i_id, s_quantity, s_dist_01, s_dist_02,
                              s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07,
                              s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt,
                              s_remote_cnt, s_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

    def load_warehouses(self) -> int:
        """
        Load warehouse data.

        Returns:
            Number of warehouses loaded
        """
        logger.info(f"Loading {self.generator.num_warehouses} warehouses...")

        count = 0
        for w_id in range(1, self.generator.num_warehouses + 1):
            warehouse = self.generator.generate_warehouse(w_id)
            self.session.execute(
                self.insert_warehouse,
                [
                    warehouse["w_id"],
                    warehouse["w_name"],
                    warehouse["w_street_1"],
                    warehouse["w_street_2"],
                    warehouse["w_city"],
                    warehouse["w_state"],
                    warehouse["w_zip"],
                    warehouse["w_tax"],
                    warehouse["w_ytd"],
                ],
            )
            count += 1

            if count % 10 == 0:
                logger.info(f"Loaded {count} warehouses")

        logger.info(f"Loaded {count} warehouses successfully")
        return count

    def load_districts(self) -> int:
        """
        Load district data.

        Returns:
            Number of districts loaded
        """
        total = self.generator.num_warehouses * self.generator.num_districts_per_warehouse
        logger.info(f"Loading {total} districts...")

        count = 0
        for w_id in range(1, self.generator.num_warehouses + 1):
            for d_id in range(1, self.generator.num_districts_per_warehouse + 1):
                district = self.generator.generate_district(d_id, w_id)
                self.session.execute(
                    self.insert_district,
                    [
                        district["d_w_id"],
                        district["d_id"],
                        district["d_name"],
                        district["d_street_1"],
                        district["d_street_2"],
                        district["d_city"],
                        district["d_state"],
                        district["d_zip"],
                        district["d_tax"],
                        district["d_ytd"],
                        district["d_next_o_id"],
                    ],
                )
                count += 1

                if count % 100 == 0:
                    logger.info(f"Loaded {count}/{total} districts")

        logger.info(f"Loaded {count} districts successfully")
        return count

    def load_customers(self, batch_size: int = 50) -> int:
        """
        Load customer data using concurrent execution for better performance.

        Args:
            batch_size: Number of concurrent executions

        Returns:
            Number of customers loaded
        """
        total = (
            self.generator.num_warehouses
            * self.generator.num_districts_per_warehouse
            * self.generator.num_customers_per_district
        )
        logger.info(f"Loading {total} customers...")

        count = 0
        customer_params = []
        customer_by_name_params = []

        for w_id in range(1, self.generator.num_warehouses + 1):
            for d_id in range(1, self.generator.num_districts_per_warehouse + 1):
                for c_id in range(1, self.generator.num_customers_per_district + 1):
                    customer = self.generator.generate_customer(c_id, d_id, w_id)

                    # Prepare parameters for customer table
                    customer_params.append(
                        [
                            customer["c_w_id"],
                            customer["c_d_id"],
                            customer["c_id"],
                            customer["c_first"],
                            customer["c_middle"],
                            customer["c_last"],
                            customer["c_street_1"],
                            customer["c_street_2"],
                            customer["c_city"],
                            customer["c_state"],
                            customer["c_zip"],
                            customer["c_phone"],
                            customer["c_since"],
                            customer["c_credit"],
                            customer["c_credit_lim"],
                            customer["c_discount"],
                            customer["c_balance"],
                            customer["c_ytd_payment"],
                            customer["c_payment_cnt"],
                            customer["c_delivery_cnt"],
                            customer["c_data"],
                        ]
                    )

                    # Prepare parameters for customer_by_name table
                    customer_by_name_params.append(
                        [
                            customer["c_w_id"],
                            customer["c_d_id"],
                            customer["c_last"],
                            customer["c_first"],
                            customer["c_id"],
                            customer["c_middle"],
                            customer["c_street_1"],
                            customer["c_street_2"],
                            customer["c_city"],
                            customer["c_state"],
                            customer["c_zip"],
                            customer["c_phone"],
                            customer["c_since"],
                            customer["c_credit"],
                            customer["c_credit_lim"],
                            customer["c_discount"],
                            customer["c_balance"],
                            customer["c_ytd_payment"],
                            customer["c_payment_cnt"],
                            customer["c_delivery_cnt"],
                            customer["c_data"],
                        ]
                    )

                    count += 1

                    # Execute in batches
                    if len(customer_params) >= batch_size:
                        execute_concurrent_with_args(
                            self.session,
                            self.insert_customer,
                            customer_params,
                            concurrency=batch_size,
                        )
                        execute_concurrent_with_args(
                            self.session,
                            self.insert_customer_by_name,
                            customer_by_name_params,
                            concurrency=batch_size,
                        )
                        logger.info(f"Loaded {count}/{total} customers")
                        customer_params = []
                        customer_by_name_params = []

        # Execute remaining
        if customer_params:
            execute_concurrent_with_args(
                self.session,
                self.insert_customer,
                customer_params,
                concurrency=len(customer_params),
            )
            execute_concurrent_with_args(
                self.session,
                self.insert_customer_by_name,
                customer_by_name_params,
                concurrency=len(customer_by_name_params),
            )

        logger.info(f"Loaded {count} customers successfully")
        return count

    def load_items(self, batch_size: int = 100) -> int:
        """
        Load item data.

        Args:
            batch_size: Number of concurrent executions

        Returns:
            Number of items loaded
        """
        logger.info(f"Loading {self.generator.num_items} items...")

        count = 0
        item_params = []

        for i_id in range(1, self.generator.num_items + 1):
            item = self.generator.generate_item(i_id)
            item_params.append(
                [item["i_id"], item["i_im_id"], item["i_name"], item["i_price"], item["i_data"]]
            )
            count += 1

            # Execute in batches
            if len(item_params) >= batch_size:
                execute_concurrent_with_args(
                    self.session, self.insert_item, item_params, concurrency=batch_size
                )
                logger.info(f"Loaded {count}/{self.generator.num_items} items")
                item_params = []

        # Execute remaining
        if item_params:
            execute_concurrent_with_args(
                self.session, self.insert_item, item_params, concurrency=len(item_params)
            )

        logger.info(f"Loaded {count} items successfully")
        return count

    def load_all_data(self) -> dict:
        """
        Load all TPC-C data.

        Returns:
            Dictionary with counts of loaded records
        """
        logger.info("Starting full data load...")

        result = {
            "warehouses": self.load_warehouses(),
            "districts": self.load_districts(),
            "customers": self.load_customers(),
            "items": self.load_items(),
        }

        logger.info(f"Data load complete: {result}")
        return result

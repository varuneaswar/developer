"""
Data loader for TPC-E benchmark.
Handles bulk loading of generated data into Cassandra.
"""

import logging
import random

from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from data_generator.tpce_data_generator import TPCEDataGenerator

logger = logging.getLogger(__name__)


class DataLoader:
    """Loads TPC-E data into Cassandra."""

    def __init__(self, session: Session, data_generator: TPCEDataGenerator):
        """
        Initialize data loader.

        Args:
            session: Active Cassandra session
            data_generator: TPC-E data generator instance
        """
        self.session = session
        self.generator = data_generator
        self._prepare_statements()

    def _prepare_statements(self) -> None:
        """Prepare all insert statements."""
        self.insert_customer = self.session.prepare(
            """
            INSERT INTO customer (c_id, c_tax_id, c_st_id, c_l_name, c_f_name, c_m_name,
                c_gndr, c_tier, c_dob, c_ad_id,
                c_ctry_1, c_area_1, c_local_1, c_ext_1,
                c_ctry_2, c_area_2, c_local_2, c_ext_2,
                c_ctry_3, c_area_3, c_local_3, c_ext_3,
                c_email_1, c_email_2)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        self.insert_broker = self.session.prepare(
            """
            INSERT INTO broker (b_id, b_st_id, b_name, b_num_trades, b_comm_total)
            VALUES (?, ?, ?, ?, ?)
            """
        )

        self.insert_customer_account = self.session.prepare(
            """
            INSERT INTO customer_account (ca_id, ca_b_id, ca_c_id, ca_name, ca_tax_st, ca_bal)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        )

        self.insert_company = self.session.prepare(
            """
            INSERT INTO company (co_id, co_st_id, co_ad_id, co_name, co_in_id, co_sp_rate,
                co_ceo, co_desc, co_open_date, co_co_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        self.insert_security = self.session.prepare(
            """
            INSERT INTO security (s_symb, s_issue, s_st_id, s_name, s_ex_id, s_co_id,
                s_num_out, s_start_date, s_exch_date, s_pe,
                s_52wk_high, s_52wk_high_date, s_52wk_low, s_52wk_low_date,
                s_dividend, s_yield)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        self.insert_trade = self.session.prepare(
            """
            INSERT INTO trade (t_id, t_dts, t_st_id, t_tt_id, t_is_cash, t_s_symb, t_qty,
                t_bid_price, t_ca_id, t_exec_name, t_trade_price, t_chrg, t_comm, t_tax, t_lifo)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        self.insert_holding = self.session.prepare(
            """
            INSERT INTO holding (h_ca_id, h_s_symb, h_dts, h_t_id, h_price, h_qty)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        )

        self.insert_holding_summary = self.session.prepare(
            """
            INSERT INTO holding_summary (hs_ca_id, hs_s_symb, hs_qty)
            VALUES (?, ?, ?)
            """
        )

    def load_customers(self, batch_size: int = 100) -> int:
        """
        Load customer data.

        Returns:
            Number of customers loaded
        """
        total = self.generator.num_customers
        logger.info(f"Loading {total} customers...")

        params_list = []
        for c_id in range(1, total + 1):
            c = self.generator.generate_customer(c_id)
            params_list.append(
                [
                    c["c_id"],
                    c["c_tax_id"],
                    c["c_st_id"],
                    c["c_l_name"],
                    c["c_f_name"],
                    c["c_m_name"],
                    c["c_gndr"],
                    c["c_tier"],
                    c["c_dob"],
                    c["c_ad_id"],
                    c["c_ctry_1"],
                    c["c_area_1"],
                    c["c_local_1"],
                    c["c_ext_1"],
                    c["c_ctry_2"],
                    c["c_area_2"],
                    c["c_local_2"],
                    c["c_ext_2"],
                    c["c_ctry_3"],
                    c["c_area_3"],
                    c["c_local_3"],
                    c["c_ext_3"],
                    c["c_email_1"],
                    c["c_email_2"],
                ]
            )

            if len(params_list) >= batch_size:
                execute_concurrent_with_args(
                    self.session, self.insert_customer, params_list, concurrency=batch_size
                )
                logger.info(f"Loaded {c_id}/{total} customers")
                params_list = []

        if params_list:
            execute_concurrent_with_args(
                self.session, self.insert_customer, params_list, concurrency=len(params_list)
            )

        logger.info(f"Loaded {total} customers successfully")
        return total

    def load_brokers(self, batch_size: int = 100) -> int:
        """Load broker data."""
        total = self.generator.num_brokers
        logger.info(f"Loading {total} brokers...")

        params_list = []
        for b_id in range(1, total + 1):
            b = self.generator.generate_broker(b_id)
            params_list.append(
                [b["b_id"], b["b_st_id"], b["b_name"], b["b_num_trades"], b["b_comm_total"]]
            )

        execute_concurrent_with_args(
            self.session, self.insert_broker, params_list, concurrency=batch_size
        )
        logger.info(f"Loaded {total} brokers successfully")
        return total

    def load_securities(self, batch_size: int = 100) -> int:
        """Load security data."""
        total = self.generator.num_securities
        logger.info(f"Loading {total} securities...")

        symbols = [f"S{i:05d}" for i in range(1, total + 1)]
        params_list = []
        for i, symb in enumerate(symbols):
            co_id = (i % self.generator.num_companies) + 1
            ex_id = self.generator.EXCHANGE_IDS[i % len(self.generator.EXCHANGE_IDS)]
            s = self.generator.generate_security(symb, co_id, ex_id)
            params_list.append(
                [
                    s["s_symb"],
                    s["s_issue"],
                    s["s_st_id"],
                    s["s_name"],
                    s["s_ex_id"],
                    s["s_co_id"],
                    s["s_num_out"],
                    s["s_start_date"],
                    s["s_exch_date"],
                    s["s_pe"],
                    s["s_52wk_high"],
                    s["s_52wk_high_date"],
                    s["s_52wk_low"],
                    s["s_52wk_low_date"],
                    s["s_dividend"],
                    s["s_yield"],
                ]
            )

            if len(params_list) >= batch_size:
                execute_concurrent_with_args(
                    self.session, self.insert_security, params_list, concurrency=batch_size
                )
                logger.info(f"Loaded {i + 1}/{total} securities")
                params_list = []

        if params_list:
            execute_concurrent_with_args(
                self.session, self.insert_security, params_list, concurrency=len(params_list)
            )

        logger.info(f"Loaded {total} securities successfully")
        return total

    def load_companies(self, batch_size: int = 100) -> int:
        """Load company data."""
        total = self.generator.num_companies
        logger.info(f"Loading {total} companies...")

        params_list = []
        for co_id in range(1, total + 1):
            in_id = self.generator.INDUSTRY_IDS[co_id % len(self.generator.INDUSTRY_IDS)]
            c = self.generator.generate_company(co_id, in_id)
            params_list.append(
                [
                    c["co_id"],
                    c["co_st_id"],
                    c["co_ad_id"],
                    c["co_name"],
                    c["co_in_id"],
                    c["co_sp_rate"],
                    c["co_ceo"],
                    c["co_desc"],
                    c["co_open_date"],
                    c["co_co_id"],
                ]
            )

        execute_concurrent_with_args(
            self.session, self.insert_company, params_list, concurrency=batch_size
        )
        logger.info(f"Loaded {total} companies successfully")
        return total

    def load_trades(self, batch_size: int = 100) -> int:
        """Load trade and holding data."""
        total = self.generator.num_trades
        logger.info(f"Loading {total} trades...")

        num_accounts = self.generator.num_customers * 2
        num_securities = self.generator.num_securities
        symbols = [f"S{i:05d}" for i in range(1, num_securities + 1)]

        trade_params = []
        holding_params = []
        hs_agg: dict = {}

        for t_id in range(1, total + 1):
            ca_id = random.randint(1, num_accounts)
            s_symb = symbols[random.randint(0, num_securities - 1)]
            t = self.generator.generate_trade(t_id, ca_id, s_symb)
            trade_params.append(
                [
                    t["t_id"],
                    t["t_dts"],
                    t["t_st_id"],
                    t["t_tt_id"],
                    t["t_is_cash"],
                    t["t_s_symb"],
                    t["t_qty"],
                    t["t_bid_price"],
                    t["t_ca_id"],
                    t["t_exec_name"],
                    t["t_trade_price"],
                    t["t_chrg"],
                    t["t_comm"],
                    t["t_tax"],
                    t["t_lifo"],
                ]
            )

            h = self.generator.generate_holding(t_id, ca_id, s_symb)
            holding_params.append(
                [
                    h["h_ca_id"],
                    h["h_s_symb"],
                    h["h_dts"],
                    h["h_t_id"],
                    h["h_price"],
                    h["h_qty"],
                ]
            )

            key = (ca_id, s_symb)
            hs_agg[key] = hs_agg.get(key, 0) + h["h_qty"]

            if len(trade_params) >= batch_size:
                execute_concurrent_with_args(
                    self.session, self.insert_trade, trade_params, concurrency=batch_size
                )
                execute_concurrent_with_args(
                    self.session, self.insert_holding, holding_params, concurrency=batch_size
                )
                logger.info(f"Loaded {t_id}/{total} trades")
                trade_params = []
                holding_params = []

        if trade_params:
            execute_concurrent_with_args(
                self.session, self.insert_trade, trade_params, concurrency=len(trade_params)
            )
            execute_concurrent_with_args(
                self.session, self.insert_holding, holding_params, concurrency=len(holding_params)
            )

        # Load holding summaries
        hs_params = [[ca_id, s_symb, qty] for (ca_id, s_symb), qty in hs_agg.items()]
        if hs_params:
            execute_concurrent_with_args(
                self.session,
                self.insert_holding_summary,
                hs_params,
                concurrency=min(batch_size, len(hs_params)),
            )

        logger.info(f"Loaded {total} trades successfully")
        return total

    def load_holdings(self) -> int:
        """Load holding data independently (alias for existing trade load logic)."""
        logger.info("Holdings are loaded as part of load_trades()")
        return 0

    def load_all_data(self) -> dict:
        """
        Load all TPC-E data.

        Returns:
            Dictionary with counts of loaded records
        """
        logger.info("Starting full TPC-E data load...")

        result = {
            "customers": self.load_customers(),
            "brokers": self.load_brokers(),
            "companies": self.load_companies(),
            "securities": self.load_securities(),
            "trades": self.load_trades(),
        }

        logger.info(f"Data load complete: {result}")
        return result

"""
TPC-E data generator for Cassandra benchmark.
Generates realistic test data based on TPC-E specifications.
"""

import random
import string
from datetime import date, datetime, timedelta
from typing import Any, Dict


class TPCEDataGenerator:
    """Generates TPC-E compliant test data."""

    # TPC-E industry codes
    INDUSTRY_IDS = [f"IN{i:02d}" for i in range(1, 25)]
    SECTOR_IDS = [f"SC{i:02d}" for i in range(1, 13)]
    EXCHANGE_IDS = ["NYSE", "NASDAQ", "AMEX", "LSE"]
    STATUS_IDS = ["ACTV", "COMP", "CNCL", "PNDG", "SBMT", "INAC"]
    TRADE_TYPE_IDS = ["TMB", "TMS", "TLB", "TLS", "TSL"]
    TAXRATE_IDS = [f"TX{i:02d}" for i in range(1, 11)]

    def __init__(
        self,
        num_customers: int = 1000,
        num_brokers: int = 100,
        num_securities: int = 5000,
        num_companies: int = 1000,
        num_trades: int = 10000,
    ):
        """
        Initialize data generator with TPC-E scale factors.

        Args:
            num_customers: Number of customers
            num_brokers: Number of brokers
            num_securities: Number of securities
            num_companies: Number of companies
            num_trades: Number of trades
        """
        self.num_customers = num_customers
        self.num_brokers = num_brokers
        self.num_securities = num_securities
        self.num_companies = num_companies
        self.num_trades = num_trades

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    def _rand_str(self, min_len: int, max_len: int) -> str:
        """Generate random alphabetic string."""
        length = random.randint(min_len, max_len)
        return "".join(random.choices(string.ascii_uppercase, k=length))

    def _rand_alnum(self, length: int) -> str:
        """Generate random alphanumeric string of fixed length."""
        return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))

    def _rand_digits(self, length: int) -> str:
        """Generate random digit string."""
        return "".join(random.choices(string.digits, k=length))

    def _rand_name(self) -> str:
        """Generate random human-readable name."""
        first = [
            "James",
            "Mary",
            "Robert",
            "Patricia",
            "John",
            "Jennifer",
            "Michael",
            "Linda",
            "William",
            "Barbara",
        ]
        last = [
            "Smith",
            "Johnson",
            "Williams",
            "Brown",
            "Jones",
            "Garcia",
            "Miller",
            "Davis",
            "Wilson",
            "Taylor",
        ]
        return random.choice(first), random.choice(last)

    def _rand_date(self, years_back: int = 5) -> date:
        """Generate random date within past N years."""
        days_back = random.randint(0, years_back * 365)
        return (datetime.now() - timedelta(days=days_back)).date()

    def _rand_ts(self, days_back: int = 365) -> datetime:
        """Generate random timestamp within past N days."""
        return datetime.now() - timedelta(
            days=random.randint(0, days_back),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

    # ------------------------------------------------------------------
    # Generator methods
    # ------------------------------------------------------------------

    def generate_customer(self, c_id: int) -> Dict[str, Any]:
        """
        Generate customer record.

        Args:
            c_id: Customer ID

        Returns:
            Customer data dictionary
        """
        fname, lname = self._rand_name()
        return {
            "c_id": c_id,
            "c_tax_id": self._rand_alnum(20),
            "c_st_id": random.choice(self.STATUS_IDS),
            "c_l_name": lname,
            "c_f_name": fname,
            "c_m_name": random.choice(list(string.ascii_uppercase)),
            "c_gndr": random.choice(["M", "F", "U"]),
            "c_tier": random.randint(1, 3),
            "c_dob": self._rand_date(60),
            "c_ad_id": random.randint(1, max(1, self.num_customers // 2)),
            "c_ctry_1": self._rand_digits(3),
            "c_area_1": self._rand_digits(3),
            "c_local_1": self._rand_digits(7),
            "c_ext_1": self._rand_digits(4) if random.random() > 0.5 else "",
            "c_ctry_2": self._rand_digits(3) if random.random() > 0.5 else "",
            "c_area_2": self._rand_digits(3) if random.random() > 0.5 else "",
            "c_local_2": self._rand_digits(7) if random.random() > 0.5 else "",
            "c_ext_2": "",
            "c_ctry_3": "",
            "c_area_3": "",
            "c_local_3": "",
            "c_ext_3": "",
            "c_email_1": f"{fname.lower()}.{lname.lower()}@example.com",
            "c_email_2": f"{fname.lower()}{c_id}@mail.com",
        }

    def generate_customer_account(self, ca_id: int, b_id: int, c_id: int) -> Dict[str, Any]:
        """
        Generate customer account record.

        Args:
            ca_id: Account ID
            b_id: Broker ID
            c_id: Customer ID

        Returns:
            Customer account data dictionary
        """
        return {
            "ca_id": ca_id,
            "ca_b_id": b_id,
            "ca_c_id": c_id,
            "ca_name": f"Account-{ca_id}",
            "ca_tax_st": random.randint(0, 2),
            "ca_bal": round(random.uniform(0.0, 1_000_000.0), 2),
        }

    def generate_broker(self, b_id: int) -> Dict[str, Any]:
        """
        Generate broker record.

        Args:
            b_id: Broker ID

        Returns:
            Broker data dictionary
        """
        fname, lname = self._rand_name()
        return {
            "b_id": b_id,
            "b_st_id": random.choice(self.STATUS_IDS),
            "b_name": f"{fname} {lname}",
            "b_num_trades": random.randint(0, 50000),
            "b_comm_total": round(random.uniform(0.0, 500_000.0), 2),
        }

    def generate_security(self, s_symb: str, co_id: int, ex_id: str) -> Dict[str, Any]:
        """
        Generate security record.

        Args:
            s_symb: Security symbol
            co_id: Company ID
            ex_id: Exchange ID

        Returns:
            Security data dictionary
        """
        price_52wk_high = round(random.uniform(10.0, 500.0), 2)
        price_52wk_low = round(random.uniform(1.0, price_52wk_high), 2)
        return {
            "s_symb": s_symb,
            "s_issue": random.choice(["COMMON", "PREF", "ADR"]),
            "s_st_id": random.choice(self.STATUS_IDS),
            "s_name": f"Security {s_symb}",
            "s_ex_id": ex_id,
            "s_co_id": co_id,
            "s_num_out": random.randint(1_000_000, 1_000_000_000),
            "s_start_date": self._rand_date(20),
            "s_exch_date": self._rand_date(20),
            "s_pe": round(random.uniform(5.0, 60.0), 2),
            "s_52wk_high": price_52wk_high,
            "s_52wk_high_date": self._rand_date(1),
            "s_52wk_low": price_52wk_low,
            "s_52wk_low_date": self._rand_date(1),
            "s_dividend": round(random.uniform(0.0, 5.0), 2),
            "s_yield": round(random.uniform(0.0, 10.0), 4),
        }

    def generate_company(self, co_id: int, in_id: str) -> Dict[str, Any]:
        """
        Generate company record.

        Args:
            co_id: Company ID
            in_id: Industry ID

        Returns:
            Company data dictionary
        """
        _, lname = self._rand_name()
        return {
            "co_id": co_id,
            "co_st_id": random.choice(self.STATUS_IDS),
            "co_ad_id": random.randint(1, max(1, self.num_companies // 2)),
            "co_name": f"{lname} Corp {co_id}",
            "co_in_id": in_id,
            "co_sp_rate": random.choice(["AAA", "AA", "A", "BBB", "BB", "B", "CCC"]),
            "co_ceo": f"CEO{co_id}",
            "co_desc": self._rand_str(100, 200),
            "co_open_date": self._rand_date(30),
            "co_co_id": co_id,
        }

    def generate_trade(self, t_id: int, ca_id: int, s_symb: str) -> Dict[str, Any]:
        """
        Generate trade record.

        Args:
            t_id: Trade ID
            ca_id: Account ID
            s_symb: Security symbol

        Returns:
            Trade data dictionary
        """
        bid_price = round(random.uniform(1.0, 500.0), 2)
        return {
            "t_id": t_id,
            "t_dts": self._rand_ts(365),
            "t_st_id": random.choice(self.STATUS_IDS),
            "t_tt_id": random.choice(self.TRADE_TYPE_IDS),
            "t_is_cash": random.random() > 0.5,
            "t_s_symb": s_symb,
            "t_qty": random.randint(1, 10000),
            "t_bid_price": bid_price,
            "t_ca_id": ca_id,
            "t_exec_name": self._rand_str(6, 20),
            "t_trade_price": round(bid_price * random.uniform(0.95, 1.05), 2),
            "t_chrg": round(random.uniform(0.0, 50.0), 2),
            "t_comm": round(random.uniform(0.0, 100.0), 2),
            "t_tax": round(random.uniform(0.0, 30.0), 2),
            "t_lifo": random.random() > 0.5,
        }

    def generate_trade_history(self, t_id: int, dts: datetime) -> Dict[str, Any]:
        """
        Generate trade history record.

        Args:
            t_id: Trade ID
            dts: Datetime stamp

        Returns:
            Trade history data dictionary
        """
        return {
            "th_t_id": t_id,
            "th_dts": dts,
            "th_st_id": random.choice(self.STATUS_IDS),
        }

    def generate_daily_market(self, s_symb: str, dm_date: date) -> Dict[str, Any]:
        """
        Generate daily market record.

        Args:
            s_symb: Security symbol
            dm_date: Market date

        Returns:
            Daily market data dictionary
        """
        close = round(random.uniform(1.0, 500.0), 2)
        high = round(close * random.uniform(1.0, 1.05), 2)
        low = round(close * random.uniform(0.95, 1.0), 2)
        return {
            "dm_s_symb": s_symb,
            "dm_date": dm_date,
            "dm_close": close,
            "dm_high": high,
            "dm_low": low,
            "dm_vol": random.randint(1_000, 100_000_000),
        }

    def generate_holding(self, t_id: int, ca_id: int, s_symb: str) -> Dict[str, Any]:
        """
        Generate holding record.

        Args:
            t_id: Trade ID
            ca_id: Account ID
            s_symb: Security symbol

        Returns:
            Holding data dictionary
        """
        return {
            "h_t_id": t_id,
            "h_ca_id": ca_id,
            "h_s_symb": s_symb,
            "h_dts": self._rand_ts(730),
            "h_price": round(random.uniform(1.0, 500.0), 2),
            "h_qty": random.randint(1, 10000),
        }

    def generate_holding_summary(self, ca_id: int, s_symb: str, qty: int) -> Dict[str, Any]:
        """
        Generate holding summary record.

        Args:
            ca_id: Account ID
            s_symb: Security symbol
            qty: Quantity held

        Returns:
            Holding summary data dictionary
        """
        return {
            "hs_ca_id": ca_id,
            "hs_s_symb": s_symb,
            "hs_qty": qty,
        }

    def generate_watch_list(self, wl_id: int, c_id: int) -> Dict[str, Any]:
        """
        Generate watch list record.

        Args:
            wl_id: Watch list ID
            c_id: Customer ID

        Returns:
            Watch list data dictionary
        """
        return {
            "wl_id": wl_id,
            "wl_c_id": c_id,
        }

    def generate_address(self, ad_id: int) -> Dict[str, Any]:
        """
        Generate address record.

        Args:
            ad_id: Address ID

        Returns:
            Address data dictionary
        """
        return {
            "ad_id": ad_id,
            "ad_line1": f"{random.randint(1, 9999)} {self._rand_str(4, 10)} St",
            "ad_line2": f"Apt {random.randint(1, 999)}" if random.random() > 0.6 else "",
            "ad_town": self._rand_str(5, 15),
            "ad_div": self._rand_str(2, 2),
            "ad_zc_code": self._rand_digits(5),
            "ad_ctry": random.choice(["US", "GB", "CA", "AU", "DE", "FR"]),
        }

    def get_scale_info(self) -> Dict[str, int]:
        """
        Get information about the data scale.

        Returns:
            Dictionary with scale information
        """
        accounts_per_customer = 2
        total_accounts = self.num_customers * accounts_per_customer

        return {
            "customers": self.num_customers,
            "brokers": self.num_brokers,
            "securities": self.num_securities,
            "companies": self.num_companies,
            "trades": self.num_trades,
            "customer_accounts": total_accounts,
            "estimated_total_records": (
                self.num_customers
                + self.num_brokers
                + self.num_securities
                + self.num_companies
                + self.num_trades
                + total_accounts
                + self.num_trades * 3  # trade_history, holding, trade denorm rows
            ),
        }

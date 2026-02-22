"""
INSERT queries for TPC-E benchmark.
Implements all 20 INSERT query methods referenced in query_definitions.py.
"""

import logging
from typing import Any, Dict, List, Optional, Set
from cassandra.cluster import Session
from cassandra.query import BatchStatement, BatchType
from datetime import datetime, date

logger = logging.getLogger(__name__)


class InsertQueries:
    """Implements all TPC-E INSERT benchmark queries."""

    def __init__(self, session: Session):
        self.session = session
        self._prepared: Dict[str, Any] = {}
        self._prepare_statements()

    def _prep(self, key: str, cql: str):
        self._prepared[key] = self.session.prepare(cql)

    def _prepare_statements(self) -> None:
        """Prepare all INSERT statements."""
        self._prep('insert_customer',
                   """INSERT INTO customer (
                       c_id, c_tax_id, c_st_id, c_l_name, c_f_name, c_m_name,
                       c_gndr, c_tier, c_dob, c_ad_id, c_email_1, c_email_2)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")

        self._prep('insert_customer_account',
                   """INSERT INTO customer_account (ca_id, ca_b_id, ca_c_id, ca_name, ca_tax_st, ca_bal)
                   VALUES (?, ?, ?, ?, ?, ?)""")

        self._prep('insert_trade',
                   """INSERT INTO trade (t_id, t_dts, t_st_id, t_tt_id, t_is_cash,
                       t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name,
                       t_trade_price, t_chrg, t_comm, t_tax, t_lifo)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")

        self._prep('insert_holding',
                   """INSERT INTO holding (h_ca_id, h_s_symb, h_dts, h_t_id, h_price, h_qty)
                   VALUES (?, ?, ?, ?, ?, ?)""")

        self._prep('insert_watch_item',
                   "INSERT INTO watch_item (wi_wl_id, wi_s_symb) VALUES (?, ?)")

        self._prep('insert_market_feed',
                   "INSERT INTO market_feed (mf_s_symb, mf_dts, mf_price, mf_vol) VALUES (?, ?, ?, ?)")

        self._prep('insert_market_feed_ttl',
                   "INSERT INTO market_feed (mf_s_symb, mf_dts, mf_price, mf_vol) "
                   "VALUES (?, ?, ?, ?) USING TTL ?")

        self._prep('insert_trade_history',
                   "INSERT INTO trade_history (th_t_id, th_dts, th_st_id) VALUES (?, ?, ?)")

        self._prep('insert_trade_extended',
                   """INSERT INTO trade_extended (t_id, t_tags, t_notes, t_attributes, t_created)
                   VALUES (?, ?, ?, ?, ?)""")

        self._prep('insert_customer_extended',
                   """INSERT INTO customer_extended (c_id, c_phone_numbers, c_email_history,
                       c_preferences, c_tags, c_notes, c_created)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""")

        self._prep('insert_portfolio_snapshot',
                   """INSERT INTO portfolio_snapshot (ca_id, snap_date, snap_time,
                       account_name, account_bal, s_symb, position_qty, position_val)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""")

        self._prep('insert_trade_by_account',
                   """INSERT INTO trade_by_account (t_ca_id, t_dts, t_id, t_st_id, t_tt_id,
                       t_is_cash, t_s_symb, t_qty, t_bid_price, t_exec_name,
                       t_trade_price, t_chrg, t_comm, t_tax, t_lifo)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")

        self._prep('insert_watch_list',
                   "INSERT INTO watch_list (wl_id, wl_c_id) VALUES (?, ?)")

        self._prep('insert_trade_lwt',
                   """INSERT INTO trade (t_id, t_dts, t_st_id, t_tt_id, t_is_cash,
                       t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name,
                       t_trade_price, t_chrg, t_comm, t_tax, t_lifo)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS""")

        self._prep('insert_account_activity_counter',
                   "UPDATE account_activity SET activity_count = activity_count + ? "
                   "WHERE ca_id = ? AND activity_type = ?")

        self._prep('insert_watch_list_lwt',
                   "INSERT INTO watch_list (wl_id, wl_c_id) VALUES (?, ?) IF NOT EXISTS")

    # --- Simple INSERT (I1-I5) ---

    def insert_customer(self, customer_id: int, tax_id: str, status_id: str,
                        last_name: str, first_name: str, middle: str,
                        gender: str, tier: int, dob: date, address_id: int,
                        email1: str, email2: str) -> None:
        self.session.execute(self._prepared['insert_customer'], [
            customer_id, tax_id, status_id, last_name, first_name, middle,
            gender, tier, dob, address_id, email1, email2
        ])

    def insert_customer_account(self, account_id: int, broker_id: int,
                                customer_id: int, name: str,
                                tax_status: int, balance: float) -> None:
        self.session.execute(self._prepared['insert_customer_account'], [
            account_id, broker_id, customer_id, name, tax_status, balance
        ])

    def insert_trade(self, trade_id: int, dts: datetime, status_id: str,
                     trade_type_id: str, is_cash: bool, symbol: str,
                     qty: int, bid_price: float, account_id: int,
                     exec_name: str, trade_price: float, charge: float,
                     commission: float, tax: float, lifo: bool) -> None:
        self.session.execute(self._prepared['insert_trade'], [
            trade_id, dts, status_id, trade_type_id, is_cash, symbol,
            qty, bid_price, account_id, exec_name, trade_price,
            charge, commission, tax, lifo
        ])

    def insert_holding(self, account_id: int, symbol: str, dts: datetime,
                       trade_id: int, price: float, qty: int) -> None:
        self.session.execute(self._prepared['insert_holding'], [
            account_id, symbol, dts, trade_id, price, qty
        ])

    def insert_watch_item(self, watchlist_id: int, symbol: str) -> None:
        self.session.execute(self._prepared['insert_watch_item'], [watchlist_id, symbol])

    # --- Medium INSERT (I6-I14) ---

    def insert_trade_with_ttl(self, symbol: str, dts: datetime,
                               price: float, vol: int, ttl_seconds: int) -> None:
        self.session.execute(self._prepared['insert_market_feed_ttl'],
                             [symbol, dts, price, vol, ttl_seconds])

    def insert_trade_history_batch(self, trade_id: int,
                                   history_entries: List[Dict[str, Any]]) -> None:
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        for entry in history_entries:
            batch.add(self._prepared['insert_trade_history'],
                      [trade_id, entry['dts'], entry['status_id']])
        self.session.execute(batch)

    def insert_holding_with_collections(self, trade_id: int, tags: Set[str],
                                        notes: List[str],
                                        attributes: Dict[str, str]) -> None:
        self.session.execute(self._prepared['insert_trade_extended'], [
            trade_id, tags, notes, attributes, datetime.now()
        ])

    def insert_market_feed_ttl(self, symbol: str, dts: datetime,
                                price: float, vol: int, ttl_seconds: int) -> None:
        self.session.execute(self._prepared['insert_market_feed_ttl'],
                             [symbol, dts, price, vol, ttl_seconds])

    def insert_trade_extended_with_collections(self, trade_id: int, tags: Set[str],
                                               notes: List[str],
                                               attributes: Dict[str, str],
                                               created: datetime) -> None:
        self.session.execute(self._prepared['insert_trade_extended'], [
            trade_id, tags, notes, attributes, created
        ])

    def insert_customer_extended_with_udt(self, customer_id: int,
                                          phone_numbers: Set[str],
                                          email_history: List[str],
                                          preferences: Dict[str, str],
                                          tags: Set[str], notes: List[str],
                                          created: datetime) -> None:
        self.session.execute(self._prepared['insert_customer_extended'], [
            customer_id, phone_numbers, email_history, preferences,
            tags, notes, created
        ])

    def insert_batch_trades(self, trades: List[Dict[str, Any]]) -> None:
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        for t in trades:
            batch.add(self._prepared['insert_trade'], [
                t['trade_id'], t['dts'], 'ACTV', 'TMB', True,
                t['symbol'], t['qty'], t['price'], t['account_id'],
                'BatchExec', t['price'], 10.0, 5.0, 2.0, False
            ])
        self.session.execute(batch)

    def insert_with_timestamp(self, symbol: str, dts: datetime, price: float,
                              vol: int, timestamp_micros: int) -> None:
        cql = ("INSERT INTO market_feed (mf_s_symb, mf_dts, mf_price, mf_vol) "
               "VALUES (?, ?, ?, ?) USING TIMESTAMP ?")
        stmt = self.session.prepare(cql)
        self.session.execute(stmt, [symbol, dts, price, vol, timestamp_micros])

    def insert_account_activity_json(self, account_id: int,
                                     activity_type: str, count_increment: int) -> None:
        self.session.execute(self._prepared['insert_account_activity_counter'],
                             [count_increment, account_id, activity_type])

    # --- Complex INSERT (I15-I20) ---

    def insert_trade_lwt(self, trade_id: int, dts: datetime, status_id: str,
                         trade_type_id: str, is_cash: bool, symbol: str,
                         qty: int, bid_price: float, account_id: int,
                         exec_name: str, trade_price: float, charge: float,
                         commission: float, tax: float, lifo: bool) -> Any:
        return self.session.execute(self._prepared['insert_trade_lwt'], [
            trade_id, dts, status_id, trade_type_id, is_cash, symbol,
            qty, bid_price, account_id, exec_name, trade_price,
            charge, commission, tax, lifo
        ])

    def insert_customer_denorm_multi_table(self, customer_id: int, tax_id: str,
                                           status_id: str, last_name: str,
                                           first_name: str, middle: str,
                                           gender: str, tier: int, dob: date,
                                           address_id: int, email1: str,
                                           email2: str) -> None:
        """Insert customer and corresponding extended record."""
        self.session.execute(self._prepared['insert_customer'], [
            customer_id, tax_id, status_id, last_name, first_name, middle,
            gender, tier, dob, address_id, email1, email2
        ])
        self.session.execute(self._prepared['insert_customer_extended'], [
            customer_id, set(), [email1], {}, set(), [], datetime.now()
        ])

    def insert_portfolio_snapshot_static(self, account_id: int, snap_date: date,
                                         snap_time: datetime, account_name: str,
                                         account_bal: float, symbol: str,
                                         position_qty: int, position_val: float) -> None:
        self.session.execute(self._prepared['insert_portfolio_snapshot'], [
            account_id, snap_date, snap_time, account_name,
            account_bal, symbol, position_qty, position_val
        ])

    def insert_trade_all_collections(self, trade_id: int, tags: Set[str],
                                     notes: List[str], attributes: Dict[str, str],
                                     created: datetime, updated: datetime) -> None:
        cql = ("INSERT INTO trade_extended "
               "(t_id, t_tags, t_notes, t_attributes, t_created, t_updated) "
               "VALUES (?, ?, ?, ?, ?, ?)")
        stmt = self.session.prepare(cql)
        self.session.execute(stmt, [trade_id, tags, notes, attributes, created, updated])

    def insert_with_lwt_condition(self, watchlist_id: int, customer_id: int) -> Any:
        return self.session.execute(self._prepared['insert_watch_list_lwt'],
                                    [watchlist_id, customer_id])

    def insert_multiple_tables_batch(self, trade_id: int, dts: datetime,
                                     status_id: str, trade_type_id: str,
                                     is_cash: bool, symbol: str, qty: int,
                                     bid_price: float, account_id: int,
                                     exec_name: str, trade_price: float,
                                     charge: float, commission: float,
                                     tax: float, lifo: bool) -> None:
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(self._prepared['insert_trade'], [
            trade_id, dts, status_id, trade_type_id, is_cash, symbol,
            qty, bid_price, account_id, exec_name, trade_price,
            charge, commission, tax, lifo
        ])
        batch.add(self._prepared['insert_trade_by_account'], [
            account_id, dts, trade_id, status_id, trade_type_id,
            is_cash, symbol, qty, bid_price, exec_name,
            trade_price, charge, commission, tax, lifo
        ])
        self.session.execute(batch)

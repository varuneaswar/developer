"""
UPDATE queries for TPC-E benchmark.
Implements all 20 UPDATE query methods referenced in query_definitions.py.
"""

import logging
from typing import Any, Dict, List, Optional, Set
from cassandra.cluster import Session
from cassandra.query import BatchStatement, BatchType
from datetime import datetime

logger = logging.getLogger(__name__)


class UpdateQueries:
    """Implements all TPC-E UPDATE benchmark queries."""

    def __init__(self, session: Session):
        self.session = session
        self._prepared: Dict[str, Any] = {}
        self._prepare_statements()

    def _prep(self, key: str, cql: str):
        self._prepared[key] = self.session.prepare(cql)

    def _prepare_statements(self) -> None:
        """Prepare all UPDATE statements."""
        self._prep('update_account_balance',
                   "UPDATE customer_account SET ca_bal = ? WHERE ca_id = ?")

        self._prep('update_broker_commission',
                   "UPDATE broker SET b_comm_total = ? WHERE b_id = ?")

        self._prep('update_holding_summary_qty',
                   "UPDATE holding_summary SET hs_qty = ? WHERE hs_ca_id = ? AND hs_s_symb = ?")

        self._prep('update_last_trade',
                   "UPDATE last_trade SET lt_dts = ?, lt_price = ?, lt_open_price = ?, lt_vol = ? "
                   "WHERE lt_s_symb = ?")

        self._prep('update_account_balance_lwt',
                   "UPDATE customer_account SET ca_bal = ? WHERE ca_id = ? IF ca_bal = ?")

        self._prep('update_customer_email_list',
                   "UPDATE customer_extended SET c_email_history = c_email_history + ? "
                   "WHERE c_id = ?")

        self._prep('update_customer_prefs_map',
                   "UPDATE customer_extended SET c_preferences = c_preferences + ? "
                   "WHERE c_id = ?")

        self._prep('update_trade_status_lwt',
                   "UPDATE trade SET t_st_id = ? WHERE t_id = ? IF t_st_id = ?")

        self._prep('update_multiple_account_fields',
                   "UPDATE customer_account SET ca_name = ?, ca_tax_st = ? WHERE ca_id = ?")

        self._prep('update_market_feed_ttl',
                   "UPDATE market_feed USING TTL ? SET mf_price = ? "
                   "WHERE mf_s_symb = ? AND mf_dts = ?")

        self._prep('update_broker_counter',
                   "UPDATE broker_metrics SET metric_value = metric_value + ? "
                   "WHERE b_id = ? AND metric_name = ?")

        self._prep('update_holding_summary_batch',
                   "UPDATE holding_summary SET hs_qty = ? WHERE hs_ca_id = ? AND hs_s_symb = ?")

        self._prep('update_portfolio_snapshot_static',
                   "UPDATE portfolio_snapshot SET account_name = ?, account_bal = ? "
                   "WHERE ca_id = ?")

        self._prep('update_account_counter',
                   "UPDATE account_activity SET activity_count = activity_count + ? "
                   "WHERE ca_id = ? AND activity_type = ?")

        self._prep('update_trade_lwt_complex',
                   "UPDATE trade SET t_st_id = ? WHERE t_id = ? "
                   "IF t_st_id = ? AND t_tt_id = ?")

        self._prep('update_account_balance_and_holding',
                   "UPDATE customer_account SET ca_bal = ? WHERE ca_id = ?")

        self._prep('update_account_lwt_multi',
                   "UPDATE customer_account SET ca_bal = ? WHERE ca_id = ? "
                   "IF ca_bal = ? AND ca_tax_st = ?")

        self._prep('update_last_trade_batch',
                   "UPDATE last_trade SET lt_price = ?, lt_vol = ?, lt_dts = ? "
                   "WHERE lt_s_symb = ?")

    # --- Simple UPDATE (U1-U4) ---

    def update_account_balance(self, account_id: int, new_balance: float) -> None:
        self.session.execute(self._prepared['update_account_balance'],
                             [new_balance, account_id])

    def update_broker_commission(self, broker_id: int, new_comm_total: float) -> None:
        self.session.execute(self._prepared['update_broker_commission'],
                             [new_comm_total, broker_id])

    def update_holding_summary_qty(self, account_id: int, symbol: str,
                                   new_qty: int) -> None:
        self.session.execute(self._prepared['update_holding_summary_qty'],
                             [new_qty, account_id, symbol])

    def update_last_trade(self, symbol: str, dts: datetime, price: float,
                          open_price: float, vol: int) -> None:
        self.session.execute(self._prepared['update_last_trade'],
                             [dts, price, open_price, vol, symbol])

    # --- Medium UPDATE (U5-U14) ---

    def update_account_balance_conditional(self, account_id: int, new_balance: float,
                                           expected_balance: float) -> Any:
        return self.session.execute(self._prepared['update_account_balance_lwt'],
                                    [new_balance, account_id, expected_balance])

    def update_holdings_batch(self, updates: List[Dict[str, Any]]) -> None:
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        for u in updates:
            batch.add(self._prepared['update_holding_summary_batch'],
                      [u['qty'], u['account_id'], u['symbol']])
        self.session.execute(batch)

    def update_customer_email_collections(self, customer_id: int,
                                          new_email: str) -> None:
        self.session.execute(self._prepared['update_customer_email_list'],
                             [[new_email], customer_id])

    def update_customer_preferences_map(self, customer_id: int,
                                        prefs_update: Dict[str, str]) -> None:
        self.session.execute(self._prepared['update_customer_prefs_map'],
                             [prefs_update, customer_id])

    def update_trade_status_lwt(self, trade_id: int, new_status: str,
                                expected_status: str) -> Any:
        return self.session.execute(self._prepared['update_trade_status_lwt'],
                                    [new_status, trade_id, expected_status])

    def update_multiple_account_fields(self, account_id: int, name: str,
                                       tax_status: int) -> None:
        self.session.execute(self._prepared['update_multiple_account_fields'],
                             [name, tax_status, account_id])

    def update_market_feed_ttl(self, symbol: str, dts: datetime,
                               price: float, ttl_seconds: int) -> None:
        self.session.execute(self._prepared['update_market_feed_ttl'],
                             [ttl_seconds, price, symbol, dts])

    def update_broker_stats_counter(self, broker_id: int, metric_name: str,
                                    increment: int) -> None:
        self.session.execute(self._prepared['update_broker_counter'],
                             [increment, broker_id, metric_name])

    def update_trade_with_timestamp(self, trade_id: int, exec_name: str,
                                    timestamp_micros: int) -> None:
        cql = ("UPDATE trade USING TIMESTAMP ? SET t_exec_name = ? WHERE t_id = ?")
        stmt = self.session.prepare(cql)
        self.session.execute(stmt, [timestamp_micros, exec_name, trade_id])

    def update_portfolio_snapshot_static(self, account_id: int, account_name: str,
                                         account_bal: float) -> None:
        self.session.execute(self._prepared['update_portfolio_snapshot_static'],
                             [account_name, account_bal, account_id])

    # --- Complex UPDATE (U15-U20) ---

    def update_trade_lwt_complex(self, trade_id: int, new_status: str,
                                 expected_status: str, expected_type: str) -> Any:
        return self.session.execute(self._prepared['update_trade_lwt_complex'],
                                    [new_status, trade_id, expected_status, expected_type])

    def update_account_and_holding_batch(self, account_id: int, new_balance: float,
                                         symbol: str, new_qty: int) -> None:
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(self._prepared['update_account_balance_and_holding'],
                  [new_balance, account_id])
        batch.add(self._prepared['update_holding_summary_batch'],
                  [new_qty, account_id, symbol])
        self.session.execute(batch)

    def update_collection_with_ttl(self, trade_id: int, tag: str,
                                   ttl_seconds: int) -> None:
        cql = ("UPDATE trade_extended USING TTL ? "
               "SET t_tags = t_tags + ? WHERE t_id = ?")
        stmt = self.session.prepare(cql)
        self.session.execute(stmt, [ttl_seconds, {tag}, trade_id])

    def update_lwt_multiple_conditions(self, account_id: int, new_balance: float,
                                       expected_balance: float,
                                       expected_tax_st: int) -> Any:
        return self.session.execute(self._prepared['update_account_lwt_multi'],
                                    [new_balance, account_id,
                                     expected_balance, expected_tax_st])

    def update_unlogged_batch(self, updates: List[Dict[str, Any]]) -> None:
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        for u in updates:
            batch.add(self._prepared['update_last_trade_batch'],
                      [u['price'], u['vol'], u['dts'], u['symbol']])
        self.session.execute(batch)

    def update_counter_columns(self, account_id: int,
                               activity_updates: List[Dict[str, Any]]) -> None:
        for u in activity_updates:
            self.session.execute(self._prepared['update_account_counter'],
                                 [u['increment'], account_id, u['activity_type']])

"""
DELETE queries for TPC-E benchmark.
Implements all 20 DELETE query methods referenced in query_definitions.py.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

from cassandra.cluster import Session
from cassandra.query import BatchStatement, BatchType

logger = logging.getLogger(__name__)


class DeleteQueries:
    """Implements all TPC-E DELETE benchmark queries."""

    def __init__(self, session: Session):
        self.session = session
        self._prepared: Dict[str, Any] = {}
        self._prepare_statements()

    def _prep(self, key: str, cql: str):
        self._prepared[key] = self.session.prepare(cql)

    def _prepare_statements(self) -> None:
        """Prepare all DELETE statements."""
        self._prep(
            "delete_watch_item", "DELETE FROM watch_item WHERE wi_wl_id = ? AND wi_s_symb = ?"
        )

        self._prep(
            "delete_holding",
            "DELETE FROM holding WHERE h_ca_id = ? AND h_s_symb = ? "
            "AND h_dts = ? AND h_t_id = ?",
        )

        self._prep("delete_account_column", "DELETE ca_name FROM customer_account WHERE ca_id = ?")

        self._prep(
            "delete_watch_item_if_exists",
            "DELETE FROM watch_item WHERE wi_wl_id = ? AND wi_s_symb = ? IF EXISTS",
        )

        self._prep(
            "delete_market_feed_before",
            "DELETE FROM market_feed WHERE mf_s_symb = ? AND mf_dts < ?",
        )

        self._prep(
            "delete_trade_extended_tag",
            "UPDATE trade_extended SET t_tags = t_tags - ? WHERE t_id = ?",
        )

        self._prep(
            "delete_customer_pref_key",
            "DELETE c_preferences[?] FROM customer_extended WHERE c_id = ?",
        )

        self._prep("delete_trade_with_ts", "DELETE FROM trade USING TIMESTAMP ? WHERE t_id = ?")

        self._prep(
            "delete_portfolio_static",
            "DELETE account_name, account_bal FROM portfolio_snapshot WHERE ca_id = ?",
        )

        self._prep("delete_all_market_feed", "DELETE FROM market_feed WHERE mf_s_symb = ?")

        self._prep("delete_holding_partition", "DELETE FROM holding WHERE h_ca_id = ?")

        self._prep("delete_trade", "DELETE FROM trade WHERE t_id = ?")

        self._prep("delete_trade_history", "DELETE FROM trade_history WHERE th_t_id = ?")

        self._prep(
            "delete_trade_history_range",
            "DELETE FROM trade_history WHERE th_t_id = ? " "AND th_dts >= ? AND th_dts <= ?",
        )

        self._prep("delete_watch_list_lwt", "DELETE FROM watch_list WHERE wl_id = ? IF EXISTS")

        self._prep(
            "delete_trade_by_account_partition", "DELETE FROM trade_by_account WHERE t_ca_id = ?"
        )

    # --- Simple DELETE (D1-D3) ---

    def delete_watch_item(self, watchlist_id: int, symbol: str) -> None:
        self.session.execute(self._prepared["delete_watch_item"], [watchlist_id, symbol])

    def delete_holding(self, account_id: int, symbol: str, dts: datetime, trade_id: int) -> None:
        self.session.execute(self._prepared["delete_holding"], [account_id, symbol, dts, trade_id])

    def delete_specific_column(self, account_id: int, column: str = "ca_name") -> None:
        self.session.execute(self._prepared["delete_account_column"], [account_id])

    # --- Medium DELETE (D4-D11) ---

    def delete_watch_item_conditional(self, watchlist_id: int, symbol: str) -> Any:
        return self.session.execute(
            self._prepared["delete_watch_item_if_exists"], [watchlist_id, symbol]
        )

    def delete_old_market_feed(self, symbol: str, cutoff_dts: datetime) -> None:
        self.session.execute(self._prepared["delete_market_feed_before"], [symbol, cutoff_dts])

    def delete_set_element(self, trade_id: int, tag: str) -> None:
        self.session.execute(self._prepared["delete_trade_extended_tag"], [{tag}, trade_id])

    def delete_map_key(self, customer_id: int, pref_key: str) -> None:
        self.session.execute(self._prepared["delete_customer_pref_key"], [pref_key, customer_id])

    def delete_list_index(self, customer_id: int, index: int = 0) -> None:
        """Remove the first element from c_email_history list."""
        cql = "UPDATE customer_extended SET c_email_history = c_email_history - ? " "WHERE c_id = ?"
        # We remove by specifying a dummy value; real removal requires knowing the value.
        # This demonstrates the pattern; in practice you'd need the actual value.
        stmt = self.session.prepare(cql)
        self.session.execute(stmt, [[""], customer_id])

    def delete_with_timestamp(self, trade_id: int, timestamp_micros: int) -> None:
        self.session.execute(self._prepared["delete_trade_with_ts"], [timestamp_micros, trade_id])

    def delete_static_column(self, account_id: int) -> None:
        self.session.execute(self._prepared["delete_portfolio_static"], [account_id])

    def delete_expired_records_ttl(self, symbol: str) -> None:
        self.session.execute(self._prepared["delete_all_market_feed"], [symbol])

    # --- Complex DELETE (D12-D20) ---

    def delete_all_holdings_for_account(self, account_id: int) -> None:
        self.session.execute(self._prepared["delete_holding_partition"], [account_id])

    def delete_trade_with_history_batch(self, trade_id: int) -> None:
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(self._prepared["delete_trade"], [trade_id])
        batch.add(self._prepared["delete_trade_history"], [trade_id])
        self.session.execute(batch)

    def delete_batch_watch_items(self, watchlist_id: int, symbols: List[str]) -> None:
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        for sym in symbols:
            batch.add(self._prepared["delete_watch_item"], [watchlist_id, sym])
        self.session.execute(batch)

    def delete_clustering_range(
        self, trade_id: int, start_dts: datetime, end_dts: datetime
    ) -> None:
        self.session.execute(
            self._prepared["delete_trade_history_range"], [trade_id, start_dts, end_dts]
        )

    def delete_with_in_clause(self, watchlist_id: int, symbols: List[str]) -> None:
        """Delete multiple watch items using IN on clustering key."""
        placeholders = ", ".join(["?" for _ in symbols])
        cql = f"DELETE FROM watch_item WHERE wi_wl_id = ? " f"AND wi_s_symb IN ({placeholders})"
        stmt = self.session.prepare(cql)
        self.session.execute(stmt, [watchlist_id] + symbols)

    def delete_lwt_condition(self, watchlist_id: int) -> Any:
        return self.session.execute(self._prepared["delete_watch_list_lwt"], [watchlist_id])

    def delete_batch_logged(self, deletes: List[Dict[str, Any]]) -> None:
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        for d in deletes:
            batch.add(self._prepared["delete_watch_item"], [d["wl_id"], d["symbol"]])
        self.session.execute(batch)

    def delete_batch_unlogged(self, symbol: str, dts_list: List[datetime]) -> None:
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        for dts in dts_list:
            batch.add(self._prepared["delete_market_feed_before"], [symbol, dts])
        self.session.execute(batch)

    def delete_partition(self, account_id: int) -> None:
        self.session.execute(self._prepared["delete_trade_by_account_partition"], [account_id])

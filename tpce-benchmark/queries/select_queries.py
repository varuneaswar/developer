"""
SELECT queries for TPC-E benchmark.
Implements all 20 SELECT query methods referenced in query_definitions.py.
"""

import logging
from datetime import date, datetime
from typing import Any, Dict, List

from cassandra.cluster import Session

logger = logging.getLogger(__name__)


class SelectQueries:
    """Implements all TPC-E SELECT benchmark queries."""

    def __init__(self, session: Session):
        self.session = session
        self._prepared: Dict[str, Any] = {}
        self._prepare_statements()

    def _prep(self, key: str, cql: str):
        self._prepared[key] = self.session.prepare(cql)

    def _prepare_statements(self) -> None:
        """Prepare all SELECT statements."""
        self._prep("customer_by_id", "SELECT * FROM customer WHERE c_id = ?")
        self._prep("account_by_id", "SELECT * FROM customer_account WHERE ca_id = ?")
        self._prep("broker_by_id", "SELECT * FROM broker WHERE b_id = ?")
        self._prep("security_by_symbol", "SELECT * FROM security WHERE s_symb = ?")
        self._prep("last_trade_by_symbol", "SELECT * FROM last_trade WHERE lt_s_symb = ?")
        self._prep("status_type_by_id", "SELECT * FROM status_type WHERE st_id = ?")
        self._prep("trades_by_account", "SELECT * FROM trade_by_account WHERE t_ca_id = ? LIMIT ?")
        self._prep("holdings_by_account", "SELECT * FROM holding WHERE h_ca_id = ? LIMIT ?")
        self._prep("watch_items_by_watchlist", "SELECT * FROM watch_item WHERE wi_wl_id = ?")
        self._prep(
            "daily_market_range",
            "SELECT * FROM daily_market WHERE dm_s_symb = ? " "AND dm_date >= ? AND dm_date <= ?",
        )
        self._prep("financial_by_company", "SELECT * FROM financial WHERE fi_co_id = ? LIMIT ?")
        self._prep("holding_summary_by_account", "SELECT * FROM holding_summary WHERE hs_ca_id = ?")
        self._prep("news_by_company", "SELECT * FROM news_by_company WHERE nx_co_id = ? LIMIT ?")
        self._prep(
            "trades_by_symbol_date_range",
            "SELECT * FROM trade_by_symbol WHERE t_s_symb = ? "
            "AND t_dts >= ? AND t_dts <= ? LIMIT ?",
        )
        self._prep(
            "active_trades_with_filter",
            "SELECT * FROM trade_by_account WHERE t_ca_id = ? "
            "AND t_st_id = ? LIMIT ? ALLOW FILTERING",
        )
        self._prep("market_feed_latest", "SELECT * FROM market_feed WHERE mf_s_symb = ? LIMIT ?")
        self._prep(
            "holding_summary_for_portfolio",
            "SELECT hs_s_symb, hs_qty FROM holding_summary WHERE hs_ca_id = ?",
        )
        self._prep("last_trade_for_symbol", "SELECT lt_price FROM last_trade WHERE lt_s_symb = ?")
        self._prep("broker_metrics", "SELECT * FROM broker_metrics WHERE b_id = ?")
        self._prep(
            "companies_by_industry",
            "SELECT * FROM company WHERE co_in_id = ? LIMIT ? ALLOW FILTERING",
        )

    # --- Simple SELECT (S1-S6) ---

    def select_customer_by_id(self, customer_id: int) -> List[Any]:
        result = self.session.execute(self._prepared["customer_by_id"], [customer_id])
        return list(result)

    def select_account_by_id(self, account_id: int) -> List[Any]:
        result = self.session.execute(self._prepared["account_by_id"], [account_id])
        return list(result)

    def select_broker_by_id(self, broker_id: int) -> List[Any]:
        result = self.session.execute(self._prepared["broker_by_id"], [broker_id])
        return list(result)

    def select_security_by_symbol(self, symbol: str) -> List[Any]:
        result = self.session.execute(self._prepared["security_by_symbol"], [symbol])
        return list(result)

    def select_last_trade_by_symbol(self, symbol: str) -> List[Any]:
        result = self.session.execute(self._prepared["last_trade_by_symbol"], [symbol])
        return list(result)

    def select_status_type_by_id(self, status_id: str) -> List[Any]:
        result = self.session.execute(self._prepared["status_type_by_id"], [status_id])
        return list(result)

    # --- Medium SELECT (M1-M8) ---

    def select_trades_by_account(self, account_id: int, limit: int = 20) -> List[Any]:
        result = self.session.execute(self._prepared["trades_by_account"], [account_id, limit])
        return list(result)

    def select_holdings_by_account(self, account_id: int, limit: int = 50) -> List[Any]:
        result = self.session.execute(self._prepared["holdings_by_account"], [account_id, limit])
        return list(result)

    def select_watch_items_by_watchlist(self, watchlist_id: int) -> List[Any]:
        result = self.session.execute(self._prepared["watch_items_by_watchlist"], [watchlist_id])
        return list(result)

    def select_daily_market_range(self, symbol: str, start_date: date, end_date: date) -> List[Any]:
        result = self.session.execute(
            self._prepared["daily_market_range"], [symbol, start_date, end_date]
        )
        return list(result)

    def select_companies_by_industry(self, industry_id: str, limit: int = 20) -> List[Any]:
        result = self.session.execute(self._prepared["companies_by_industry"], [industry_id, limit])
        return list(result)

    def select_financial_by_company(self, company_id: int, limit: int = 8) -> List[Any]:
        result = self.session.execute(self._prepared["financial_by_company"], [company_id, limit])
        return list(result)

    def select_holding_summary_by_account(self, account_id: int) -> List[Any]:
        result = self.session.execute(self._prepared["holding_summary_by_account"], [account_id])
        return list(result)

    def select_news_by_company(self, company_id: int, limit: int = 10) -> List[Any]:
        result = self.session.execute(self._prepared["news_by_company"], [company_id, limit])
        return list(result)

    # --- Complex SELECT (C1-C6) ---

    def select_trades_by_symbol_date_range(
        self, symbol: str, start_dts: datetime, end_dts: datetime, limit: int = 50
    ) -> List[Any]:
        result = self.session.execute(
            self._prepared["trades_by_symbol_date_range"], [symbol, start_dts, end_dts, limit]
        )
        return list(result)

    def select_customer_by_name(self, last_name: str, limit: int = 20) -> List[Any]:
        """Scan customer table filtering by last name (ALLOW FILTERING)."""
        cql = "SELECT * FROM customer WHERE c_l_name = ? LIMIT ? ALLOW FILTERING"
        stmt = self.session.prepare(cql)
        result = self.session.execute(stmt, [last_name, limit])
        return list(result)

    def select_active_trades_with_filter(
        self, account_id: int, status_id: str, limit: int = 20
    ) -> List[Any]:
        result = self.session.execute(
            self._prepared["active_trades_with_filter"], [account_id, status_id, limit]
        )
        return list(result)

    def select_market_feed_latest(self, symbol: str, limit: int = 10) -> List[Any]:
        result = self.session.execute(self._prepared["market_feed_latest"], [symbol, limit])
        return list(result)

    def select_portfolio_value(self, account_id: int) -> Dict[str, Any]:
        """Calculate portfolio value by joining holdings with last trade prices."""
        holdings = self.session.execute(
            self._prepared["holding_summary_for_portfolio"], [account_id]
        )
        total_value = 0.0
        positions = []
        for row in holdings:
            price_rows = self.session.execute(
                self._prepared["last_trade_for_symbol"], [row.hs_s_symb]
            )
            price_row = price_rows.one()
            price = float(price_row.lt_price) if price_row else 0.0
            val = price * row.hs_qty
            total_value += val
            positions.append(
                {"symbol": row.hs_s_symb, "qty": row.hs_qty, "price": price, "value": val}
            )
        return {"account_id": account_id, "total_value": total_value, "positions": positions}

    def select_broker_performance(self, broker_id: int) -> Dict[str, Any]:
        """Get broker performance metrics from counter table."""
        broker_rows = self.session.execute(self._prepared["broker_by_id"], [broker_id])
        broker = broker_rows.one()
        metrics_rows = self.session.execute(self._prepared["broker_metrics"], [broker_id])
        metrics = {row.metric_name: row.metric_value for row in metrics_rows}
        return {
            "broker_id": broker_id,
            "broker_name": broker.b_name if broker else None,
            "num_trades": broker.b_num_trades if broker else 0,
            "comm_total": float(broker.b_comm_total) if broker else 0.0,
            "metrics": metrics,
        }

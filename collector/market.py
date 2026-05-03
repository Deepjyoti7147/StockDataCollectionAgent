"""
Market data collector — fetch methods ported directly from stock_fetcher.py.
Uses ticker.history() (not yf.download()) with a sliding-window rate limiter
to safely stay under Yahoo Finance's 1,000 requests/hour limit.
Fetches 1-year EOD history per symbol: OHLCV + Adj Close + Dividends + Stock Splits.
"""

import logging
import os
import time
from collections import deque
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

logger = logging.getLogger("market_collector.yf")

_YAHOO_MAX_REQUESTS_PER_HOUR = 1000
_YAHOO_WINDOW_SECONDS = 3600


class MarketCollector:
    def __init__(self, csv_path: str = "data/tickerInfo.csv"):
        self.csv_path = csv_path
        self._yahoo_request_timestamps: deque = deque()
        self._last_rate_limit_notice_at: float = 0.0
        self.symbol_company_map: dict = {}
        self.ticker_map = self._load_tickers()

    # ── Rate limiter (from stock_fetcher._acquire_yahoo_request_slot) ─────────

    def _acquire_yahoo_request_slot(self) -> bool:
        """
        Enforce max 1,000 Yahoo Finance requests per hour using a sliding window.
        Ported from stock_fetcher.py.
        """
        now = time.time()

        while (
            self._yahoo_request_timestamps
            and (now - self._yahoo_request_timestamps[0]) >= _YAHOO_WINDOW_SECONDS
        ):
            self._yahoo_request_timestamps.popleft()

        if len(self._yahoo_request_timestamps) >= _YAHOO_MAX_REQUESTS_PER_HOUR:
            if now - self._last_rate_limit_notice_at >= 60:
                retry_after = int(
                    _YAHOO_WINDOW_SECONDS - (now - self._yahoo_request_timestamps[0])
                )
                logger.warning(
                    "Yahoo request limit reached (%d/hr). Retry in ~%ds.",
                    _YAHOO_MAX_REQUESTS_PER_HOUR,
                    max(retry_after, 1),
                )
                self._last_rate_limit_notice_at = now
            return False

        self._yahoo_request_timestamps.append(now)
        return True

    # ── MultiIndex-safe column extractor (from stock_fetcher._safe_series) ────

    def _safe_series(self, data: pd.DataFrame, column_name: str) -> pd.Series:
        """
        Return a numeric Series for a yfinance column, handling MultiIndex output.
        Ported from stock_fetcher.py.
        """
        if data is None or data.empty:
            return pd.Series(dtype="float64")

        if isinstance(data.columns, pd.MultiIndex):
            matches = [col for col in data.columns if col[0] == column_name]
            if not matches:
                return pd.Series(dtype="float64")
            series = data[matches[0]]
        else:
            if column_name not in data.columns:
                return pd.Series(dtype="float64")
            series = data[column_name]

        if isinstance(series, pd.DataFrame):
            series = series.iloc[:, 0]

        return pd.to_numeric(series, errors="coerce").dropna()

    # ── Ticker loader (from stock_fetcher._get_nse_symbols) ──────────────────

    def _load_tickers(self) -> dict:
        """
        Load NSE symbols and company names from CSV.
        Filters to EQ series only. Ported from stock_fetcher._get_nse_symbols.
        """
        if not os.path.exists(self.csv_path):
            logger.warning("CSV not found at %s. Using empty map.", self.csv_path)
            return {}

        try:
            df = pd.read_csv(self.csv_path)
            df.columns = df.columns.str.strip()

            if "SERIES" in df.columns:
                df = df[df["SERIES"].astype(str).str.strip().eq("EQ")]

            company_col = None
            for candidate in ["NAME OF COMPANY", "NAME_OF_COMPANY", "COMPANY NAME", "COMPANY_NAME"]:
                if candidate in df.columns:
                    company_col = candidate
                    break

            mapping: dict = {}
            for _, row in df.iterrows():
                raw_symbol = str(row.get("SYMBOL", "")).strip()
                if not raw_symbol:
                    continue
                ticker_symbol = raw_symbol.upper()
                if not (ticker_symbol.endswith(".NS") or ticker_symbol.endswith(".BO")):
                    ticker_symbol = f"{ticker_symbol}.NS"

                if ticker_symbol not in mapping:
                    company_name = raw_symbol
                    if company_col:
                        raw_company = row.get(company_col)
                        if pd.notna(raw_company):
                            raw_company_text = str(raw_company).strip()
                            if raw_company_text:
                                company_name = raw_company_text
                    mapping[ticker_symbol] = company_name

            logger.info("Loaded %d tickers from CSV", len(mapping))
            return mapping

        except Exception as e:
            logger.error("Failed to load tickers from CSV: %s", e)
            return {}

    # ── Per-symbol EOD fetch (from stock_fetcher.get_eod_data) ───────────────

    def _fetch_symbol_fallback(self, symbol: str) -> pd.DataFrame | None:
        """
        Fallback using yahooquery, which bypasses yfinance rate limits
        due to different endpoints and session handling.
        """
        try:
            from yahooquery import Ticker as YQTicker
        except ImportError:
            logger.warning("yahooquery not installed. Cannot use fallback.")
            return None
            
        try:
            logger.info("Using yahooquery fallback for %s", symbol)
            t = YQTicker(symbol)
            df = t.history(period="1y", interval="1d")
            
            if df is None or df.empty or isinstance(df, dict):
                return None
                
            if "symbol" in df.index.names:
                df = df.reset_index(level="symbol", drop=True)
                
            rename_map = {
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
                "adjclose": "Adj Close",
                "dividends": "Dividends",
                "splits": "Stock Splits"
            }
            df = df.rename(columns=rename_map)
            df.index = pd.to_datetime(df.index)
            return df
        except Exception as e:
            logger.error("Fallback failed for %s: %s", symbol, e)
            return None

    def _fetch_symbol(self, symbol: str) -> pd.DataFrame | None:
        """Fetch 1 year of EOD history for a single symbol. Falls back to yahooquery on rate limit."""
        import yfinance.exceptions as yf_exc

        if not self._acquire_yahoo_request_slot():
            logger.warning("Sliding-window limit reached — attempting fallback for %s", symbol)
            return self._fetch_symbol_fallback(symbol)

        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(
                period="1y",
                interval="1d",
                auto_adjust=False,  # keeps raw OHLC + separate Adj Close column
            )

            if data is None or data.empty:
                logger.debug("No data for %s", symbol)
                return None

            return data

        except yf_exc.YFRateLimitError:
            logger.warning("Rate limited on %s — attempting fallback", symbol)
            return self._fetch_symbol_fallback(symbol)

        except Exception as e:
            logger.error("Error fetching %s: %s", symbol, e)
            return None




    # ── Bulk collector called by the scheduler ────────────────────────────────

    def fetch_all_prices(self, db, interval: str = "1d", delay: float = 5.0) -> tuple[int, int]:
        """
        Fetch EOD data for every loaded symbol one at a time.
        After each symbol is fetched, its records are immediately saved to the DB
        before moving on to the next symbol — no in-memory batch accumulation.

        Returns (total_rows_fetched, total_rows_inserted).
        """
        symbols = list(self.ticker_map.keys())
        total = len(symbols)
        total_fetched = 0
        total_inserted = 0

        logger.info("Starting sequential EOD fetch for %d symbols (delay=%.1fs)", total, delay)

        for i, symbol in enumerate(symbols, 1):
            try:
                data = self._fetch_symbol(symbol)

                if data is None:
                    continue

                company_name = self.ticker_map.get(symbol, symbol)
                records = []

                for timestamp, row in data.iterrows():

                    def _val(col: str, _row=row, _data=data) -> float | None:
                        """Safely read one scalar value, handling MultiIndex columns."""
                        if isinstance(_data.columns, pd.MultiIndex):
                            matches = [c for c in _data.columns if c[0] == col]
                            if not matches:
                                return None
                            v = _row[matches[0]]
                        else:
                            if col not in _data.columns:
                                return None
                            v = _row[col]
                        try:
                            f = float(v)
                            return None if pd.isna(f) else f
                        except (TypeError, ValueError):
                            return None

                    # Normalize daily timestamps to midnight to prevent yfinance vs yahooquery duplicates
                    ts = timestamp.replace(hour=0, minute=0, second=0, microsecond=0) if interval == "1d" else timestamp
                    records.append({
                        "symbol":       symbol,
                        "company_name": company_name,
                        "timestamp":    ts.to_pydatetime(),
                        "open":         _val("Open"),
                        "high":         _val("High"),
                        "low":          _val("Low"),
                        "close":        _val("Close"),
                        "adj_close":    _val("Adj Close"),
                        "volume":       int(_val("Volume")) if _val("Volume") is not None else None,
                        "dividends":    _val("Dividends"),
                        "stock_splits": _val("Stock Splits"),
                        "interval":     interval,
                    })

                # ── Save immediately after each symbol ────────────────────────
                inserted = db.save_prices(records)
                total_fetched += len(records)
                total_inserted += inserted
                logger.info(
                    "Saved %s — fetched=%d inserted=%d (%d/%d)",
                    symbol, len(records), inserted, i, total,
                )

            except Exception as e:
                logger.error("Unexpected error for %s: %s", symbol, e)

            finally:
                if i < total:
                    time.sleep(delay)

        logger.info(
            "Fetch complete. total_fetched=%d total_inserted=%d",
            total_fetched, total_inserted,
        )
        return total_fetched, total_inserted

    def get_fundamentals(self, symbol: str, db) -> dict:
        """Fetch fundamental data for a symbol (DB first, then API)."""
        import numpy as np
        
        cached = db.get_fundamentals(symbol)
        if cached:
            return cached
            
        logger.info("Fetching fundamentals for %s from API...", symbol)
        try:
            from yahooquery import Ticker as YQTicker
            t = YQTicker(symbol)
        except ImportError:
            logger.error("yahooquery is not installed.")
            return {}
        
        def _clean_df(df) -> list | None:
            if not isinstance(df, pd.DataFrame):
                return None
            if df.empty:
                return None
            try:
                if "symbol" in df.index.names:
                    df = df.reset_index(level="symbol", drop=True)
                
                df = df.replace({np.nan: None})
                
                # Reset date index to column if it exists to preserve the date
                if df.index.name in ["date", "asOfDate"] or isinstance(df.index, pd.DatetimeIndex):
                    df = df.reset_index()
                
                records = df.to_dict(orient="records")
                for record in records:
                    for k, v in record.items():
                        if isinstance(v, pd.Timestamp):
                            record[k] = str(v)
                return records
            except Exception as e:
                logger.error("Error cleaning DataFrame: %s", e)
                return None
        
        try:
            bs_a = _clean_df(t.balance_sheet())
            bs_q = _clean_df(t.balance_sheet(frequency="q"))
            cf_a = _clean_df(t.cash_flow())
            cf_q = _clean_df(t.cash_flow(frequency="q"))
            
            profile = t.asset_profile
            if isinstance(profile, dict):
                if symbol in profile:
                    profile = profile[symbol]
                    if isinstance(profile, str):  # Sometimes returns an error message as string
                        profile = None
                else:
                    # If the symbol isn't in the dict (e.g. invalid '{TCS}.NS' split by yahooquery), discard the garbage dict
                    profile = None
                
            data = {
                "balance_sheet_annual": bs_a,
                "balance_sheet_quarterly": bs_q,
                "cash_flow_annual": cf_a,
                "cash_flow_quarterly": cf_q,
                "asset_profile": profile
            }
            
            # Prevent saving a completely empty record to the database
            if not any(v for v in data.values() if v is not None):
                logger.warning("No fundamental data found for %s. Skipping DB save.", symbol)
                return {}
            
            db.save_fundamentals(symbol, data)
            
            # Re-fetch from DB to ensure format matches what the API expects (e.g., loaded JSON)
            return db.get_fundamentals(symbol) or data
            
        except Exception as e:
            logger.error("Failed to fetch fundamentals for %s: %s", symbol, e)
            return {}

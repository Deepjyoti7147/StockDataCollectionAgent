"""
Stock data fetcher for Indian stocks using Yahoo Finance (yfinance)
- Real-time stock quotes for NSE (Nifty 50)
- Historical EOD (End of Day) data for technical analysis
- No API key required - Free data from Yahoo Finance
"""
import yfinance as yf
import pandas as pd
import os
import time
import logging
from collections import deque
from datetime import datetime, timedelta
from config.settings import LOOKBACK_PERIOD, DIP_THRESHOLD, HEALTHY_DIP_RANGE, EOD_ANALYSIS_PERIOD
from src.db_handler import DatabaseHandler
from src.logger import setup_logger

class StockFetcher:
    def __init__(self):
        self.logger = setup_logger("stock_fetcher", "stock_fetcher.log")
        self.lookback_period = LOOKBACK_PERIOD
        self.yahoo_max_requests_per_hour = 1000
        self.yahoo_window_seconds = 3600
        self.cache_ttl_seconds = 86400  # 1 day
        self._yahoo_request_timestamps = deque()
        self._last_rate_limit_notice_at = 0
        self.symbol_company_map = {}
        self.db = DatabaseHandler()
        self.nse_symbols = self._get_nse_symbols()
        self.logger.info("StockFetcher initialized. symbols_loaded=%d", len(self.nse_symbols))


    def _acquire_yahoo_request_slot(self):
        """Enforce max 1000 Yahoo Finance requests per hour."""
        now = time.time()

        while self._yahoo_request_timestamps and (now - self._yahoo_request_timestamps[0]) >= self.yahoo_window_seconds:
            self._yahoo_request_timestamps.popleft()

        if len(self._yahoo_request_timestamps) >= self.yahoo_max_requests_per_hour:
            if now - self._last_rate_limit_notice_at >= 60:
                retry_after = int(self.yahoo_window_seconds - (now - self._yahoo_request_timestamps[0]))
                self.logger.warning(
                    f"Yahoo request limit reached ({self.yahoo_max_requests_per_hour}/hour). "
                    f"Try again in about {max(retry_after, 1)} seconds."
                )
                self._last_rate_limit_notice_at = now
            return False

        self._yahoo_request_timestamps.append(now)
        return True

    def _safe_series(self, data, column_name):
        """Return a numeric Series for a yfinance column, handling MultiIndex output."""
        if data is None or data.empty:
            return pd.Series(dtype="float64")

        if isinstance(data.columns, pd.MultiIndex):
            matches = [column for column in data.columns if column[0] == column_name]
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
    
    def _get_nse_symbols(self):
        """Load NSE symbols from data/EQUITY_L.csv."""
        csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "EQUITY_L.csv")
        try:
            if not os.path.exists(csv_path):
                self.logger.error("Symbol CSV not found: %s", csv_path)
                return []

            df = pd.read_csv(csv_path)
            if "SYMBOL" not in df.columns:
                self.logger.error("EQUITY_L.csv is missing required column: SYMBOL")
                return []

            if "SERIES" in df.columns:
                df = df[df["SERIES"].astype(str).str.strip().eq("EQ")]

            company_col = None
            for candidate in ["NAME OF COMPANY", "NAME_OF_COMPANY", "COMPANY NAME", "COMPANY_NAME"]:
                if candidate in df.columns:
                    company_col = candidate
                    break

            symbols = []
            self.symbol_company_map = {}
            for _, row in df.iterrows():
                raw_symbol = str(row.get("SYMBOL", "")).strip()
                if not raw_symbol:
                    continue
                ticker_symbol = raw_symbol.upper()
                if not (ticker_symbol.endswith(".NS") or ticker_symbol.endswith(".BO")):
                    ticker_symbol = f"{ticker_symbol}.NS"

                if ticker_symbol not in self.symbol_company_map:
                    company_name = raw_symbol
                    if company_col:
                        raw_company = row.get(company_col)
                        if pd.notna(raw_company):
                            raw_company_text = str(raw_company).strip()
                            if raw_company_text:
                                company_name = raw_company_text
                    self.symbol_company_map[ticker_symbol] = company_name

                symbols.append(ticker_symbol)

            symbols = list(dict.fromkeys(symbols))

            self.logger.info("Loaded symbols from CSV. total_symbols=%d", len(symbols))
            return symbols
        except Exception as e:
            self.logger.exception("Error loading symbols from EQUITY_L.csv: %s", e)
            return []

    def get_company_name(self, symbol):
        return self.symbol_company_map.get(symbol, symbol.replace(".NS", "").replace(".BO", ""))
    
    def get_stock_data(self, symbol, period="1y"):
        """Fetch historical stock data"""
        try:
            cache_key = f"stock_data:{symbol}:{period}:1d:adj_false"
            cached = self.db.load_cached_data(cache_key, self.cache_ttl_seconds)
            if cached is not None:
                self.logger.info("Cache hit for key=%s", cache_key)
                return cached

            if not self._acquire_yahoo_request_slot():
                self.logger.warning("Skipped get_stock_data for %s due to rate limit", symbol)
                return None
            ticker = yf.Ticker(symbol)
            data = ticker.history(period=period, interval="1d", auto_adjust=False)
            self.db.save_cached_data(cache_key, data, self.cache_ttl_seconds)
            self.logger.info("Cache saved for key=%s rows=%d", cache_key, len(data) if data is not None else 0)
            return data
        except Exception as e:
            self.logger.exception("Error fetching data for %s: %s", symbol, e)
            return None
    
    def calculate_dip(self, symbol):
        """Calculate current dip percentage"""
        try:
            data = self.get_stock_data(symbol, period="1y")
            close = self._safe_series(data, "Close")
            if close.empty or len(close) < 2:
                return None
            current_price = float(close.iloc[-1])
            high_52w = float(close.tail(min(252, len(close))).max())
            if high_52w <= 0:
                return None

            dip_percentage = ((current_price - high_52w) / high_52w) * 100
            return dip_percentage
        except Exception as e:
            self.logger.error(f"Error calculating dip for {symbol}: {e}")
            return None
    
    def identify_healthy_dips(self, progress_callback=None, symbols=None, per_symbol_delay=0.0):
        """Identify stocks with healthy dips.

        Args:
            progress_callback: Optional callback invoked after each processed symbol.
            symbols: Optional subset of symbols to scan. Defaults to all loaded symbols.
            per_symbol_delay: Optional delay in seconds after each symbol scan.
        """
        healthy_dip_stocks = []
        target_symbols = symbols if symbols is not None else self.nse_symbols
        total_symbols = len(target_symbols)
        self.logger.info("Healthy dip scan started. total_symbols=%d", total_symbols)
        
        for idx, symbol in enumerate(target_symbols, start=1):
            status = "UNKNOWN"
            dip_record = None
            try:
                data = self.get_stock_data(symbol, period="2y")
                if data is None or data.empty:
                    status = "NO_DATA_OR_RATE_LIMIT"
                    continue

                close = self._safe_series(data, "Close")
                volume = self._safe_series(data, "Volume")

                if close.empty or volume.empty or len(close) < 30 or len(volume) < 30:
                    status = "INSUFFICIENT_DATA"
                    continue
                
                # Calculate metrics
                current_price = float(close.iloc[-1])
                high_52w = float(close.tail(min(252, len(close))).max())  # 52 weeks
                volume_current = float(volume.iloc[-1])
                volume_avg = float(volume.tail(30).mean())

                if high_52w <= 0 or volume_avg <= 0:
                    status = "INVALID_METRICS"
                    continue
                
                dip_percentage = ((current_price - high_52w) / high_52w) * 100
                
                # Check if it's a healthy dip
                if HEALTHY_DIP_RANGE[0] <= dip_percentage <= HEALTHY_DIP_RANGE[1]:
                    if volume_current >= volume_avg * 0.8:
                        volume_ratio = volume_current / volume_avg if volume_avg > 0 else 0
                        dip_record = {
                            'symbol': symbol,
                            'company_name': self.get_company_name(symbol),
                            'current_price': current_price,
                            'high_52w': high_52w,
                            'dip_percentage': dip_percentage,
                            'volume_current': volume_current,
                            'volume_avg': volume_avg,
                            'volume_ratio': volume_ratio,
                            'dip_percentage_recent': self._calculate_recent_dip(pd.DataFrame({'Close': close}))
                        }
                        healthy_dip_stocks.append(dip_record)
                        status = "HEALTHY_DIP"
                    else:
                        status = "LOW_VOLUME"
                else:
                    status = "OUTSIDE_DIP_RANGE"
            except Exception as e:
                status = "ERROR"
                self.logger.exception("Error analyzing %s: %s", symbol, e)
            finally:
                if callable(progress_callback):
                    try:
                        progress_callback({
                            "processed": idx,
                            "total": total_symbols,
                            "symbol": symbol,
                            "status": status,
                            "healthy_count": len(healthy_dip_stocks),
                            "dip": dip_record,
                        })
                    except Exception:
                        pass

                self.logger.info(
                    "Scan progress %d/%d symbol=%s status=%s healthy_count=%d",
                    idx,
                    total_symbols,
                    symbol,
                    status,
                    len(healthy_dip_stocks),
                )

                if per_symbol_delay and per_symbol_delay > 0:
                    time.sleep(float(per_symbol_delay))
        
        results = sorted(healthy_dip_stocks, key=lambda x: x['dip_percentage'])
        self.logger.info("Healthy dip scan completed. total_healthy_dips=%d", len(results))
        return results
    
    def _calculate_recent_dip(self, data, days=5):
        """Calculate dip in recent days"""
        if len(data) < days:
            return 0
        close = self._safe_series(data, "Close")
        if close.empty:
            return 0
        recent_high = float(close.tail(days).max())
        current = float(close.iloc[-1])
        return ((current - recent_high) / recent_high) * 100
    
    def get_eod_data(self, symbol, days=365):
        """Get End of Day (EOD) historical data for analysis
        
        Args:
            symbol: Stock symbol (e.g., "RELIANCE.NS")
            days: Number of days of EOD data to fetch (default: 1 year)
        
        Returns:
            DataFrame with columns: Open, High, Low, Close, Volume, Adj Close
        """
        try:
            cache_key = f"eod_data:{symbol}:{days}:1d:adj_false"
            cached = self.db.load_cached_data(cache_key, self.cache_ttl_seconds)
            if cached is not None:
                data = cached.copy()
                self.logger.info("Cache hit for key=%s", cache_key)
            else:
                data = None

            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
        
            # Fetch EOD data
            if data is None:
                if not self._acquire_yahoo_request_slot():
                    self.logger.warning("Skipped get_eod_data for %s due to rate limit", symbol)
                    return None
                ticker = yf.Ticker(symbol)
                data = ticker.history(start=start_date, end=end_date, interval="1d", auto_adjust=False)
                self.db.save_cached_data(cache_key, data, self.cache_ttl_seconds)
                self.logger.info("Cache saved for key=%s", cache_key)
        
            if data.empty:
                return None
        
            # Add technical indicators for analysis
            data['SMA_20'] = data['Close'].rolling(window=20).mean()
            data['SMA_50'] = data['Close'].rolling(window=50).mean()
            data['SMA_200'] = data['Close'].rolling(window=200).mean()
        
            return data
        except Exception as e:
            self.logger.exception("Error fetching EOD data for %s: %s", symbol, e)
            return None
    
    def analyze_eod_trend(self, symbol):
        """Analyze EOD data to determine trend
        
        Returns:
            Dictionary with trend analysis:
            - trend: UPTREND, DOWNTREND, CONSOLIDATION
            - strength: 0-100 (trend strength)
            - support_level: Estimated support price
            - resistance_level: Estimated resistance price
        """
        try:
            data = self.get_eod_data(symbol, days=100)
            close = self._safe_series(data, "Close")
            high = self._safe_series(data, "High")
            low = self._safe_series(data, "Low")

            if close.empty or high.empty or low.empty or len(close) < 50:
                return None
        
            # Get latest price and moving averages
            current_price = float(close.iloc[-1])
            sma_20 = float(data['SMA_20'].iloc[-1]) if 'SMA_20' in data.columns else None
            sma_50 = float(data['SMA_50'].iloc[-1]) if 'SMA_50' in data.columns else None
            sma_200 = float(data['SMA_200'].iloc[-1]) if 'SMA_200' in data.columns else None

            if sma_20 is None or sma_50 is None or sma_200 is None:
                return None
        
            # Determine trend
            if sma_20 > sma_50 > sma_200:
                trend = "UPTREND"
                strength = 80
            elif sma_20 < sma_50 < sma_200:
                trend = "DOWNTREND"
                strength = 80
            else:
                trend = "CONSOLIDATION"
                strength = 50
        
            # Find support and resistance (last 60 days)
            support_level = float(low.tail(60).min())
            resistance_level = float(high.tail(60).max())

            if support_level <= 0 or resistance_level <= 0:
                return None
        
            return {
                'symbol': symbol,
                'trend': trend,
                'strength': strength,
                'current_price': current_price,
                'support_level': support_level,
                'resistance_level': resistance_level,
                'sma_20': sma_20,
                'sma_50': sma_50,
                'sma_200': sma_200,
                'distance_from_support': ((current_price - support_level) / support_level) * 100,
                'distance_from_resistance': ((resistance_level - current_price) / resistance_level) * 100
            }
        except Exception as e:
            self.logger.exception("Error analyzing EOD trend for %s: %s", symbol, e)
            return None
    
    def get_real_time_data(self, symbol):
        """Get real-time data for a symbol"""
        try:
            if not self._acquire_yahoo_request_slot():
                self.logger.warning("Skipped get_real_time_data for %s due to rate limit", symbol)
                return None
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="1d")

            info = {}
            if self._acquire_yahoo_request_slot():
                info = ticker.info
            else:
                self.logger.warning("Skipped ticker.info for %s due to rate limit", symbol)
            
            return {
                'symbol': symbol,
                'current_price': info.get('currentPrice', data['Close'].iloc[-1] if len(data) > 0 else 0),
                'open': info.get('open', 0),
                'high': info.get('dayHigh', 0),
                'low': info.get('dayLow', 0),
                'volume': info.get('volume', 0),
                'market_cap': info.get('marketCap', 0),
                'pe_ratio': info.get('trailingPE', 0),
                'dividend_yield': info.get('dividendYield', 0)
            }
        except Exception as e:
            self.logger.exception("Error fetching real-time data for %s: %s", symbol, e)
            return None

    def get_stock_full_data(self, symbol):
        """Fetch a comprehensive stock data package for dashboard + LLM analysis."""
        try:
            if not self._acquire_yahoo_request_slot():
                self.logger.warning("Skipped get_stock_full_data for %s due to rate limit", symbol)
                return None
            ticker = yf.Ticker(symbol)

            info = {}
            if self._acquire_yahoo_request_slot():
                info = ticker.info or {}
            else:
                self.logger.warning("Skipped ticker.info in get_stock_full_data for %s due to rate limit", symbol)

            cache_key = f"stock_full_history:{symbol}:1y:1d:adj_false"
            history_1y = self.db.load_cached_data(cache_key, self.cache_ttl_seconds)
            if history_1y is None:
                if not self._acquire_yahoo_request_slot():
                    self.logger.warning("Skipped history fetch in get_stock_full_data for %s due to rate limit", symbol)
                    return None
                history_1y = ticker.history(period="1y", interval="1d", auto_adjust=False)
                self.db.save_cached_data(cache_key, history_1y, self.cache_ttl_seconds)
            else:
                self.logger.info("Cache hit for key=%s", cache_key)

            if history_1y is None or history_1y.empty:
                return None

            history_6mo = history_1y.tail(126).copy()
            history_3mo = history_1y.tail(63).copy()

            close = self._safe_series(history_1y, "Close")
            volume = self._safe_series(history_1y, "Volume")

            if close.empty:
                return None

            current_price = float(close.iloc[-1])
            high_52w = float(close.tail(min(252, len(close))).max()) if len(close) > 0 else 0
            low_52w = float(close.tail(min(252, len(close))).min()) if len(close) > 0 else 0
            avg_volume_30 = float(volume.tail(min(30, len(volume))).mean()) if not volume.empty else 0
            current_volume = float(volume.iloc[-1]) if not volume.empty else 0

            # Technical data from existing trend method
            trend_data = self.analyze_eod_trend(symbol) or {}

            summary = {
                "symbol": symbol,
                "name": info.get("longName") or info.get("shortName") or symbol,
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "current_price": current_price,
                "day_high": info.get("dayHigh"),
                "day_low": info.get("dayLow"),
                "market_cap": info.get("marketCap"),
                "trailing_pe": info.get("trailingPE"),
                "forward_pe": info.get("forwardPE"),
                "beta": info.get("beta"),
                "dividend_yield": info.get("dividendYield"),
                "fifty_two_week_high": high_52w,
                "fifty_two_week_low": low_52w,
                "avg_volume_30": avg_volume_30,
                "current_volume": current_volume,
                "volume_ratio": (current_volume / avg_volume_30) if avg_volume_30 > 0 else 0,
                "recommendation_key": info.get("recommendationKey"),
                "target_mean_price": info.get("targetMeanPrice"),
                "target_high_price": info.get("targetHighPrice"),
                "target_low_price": info.get("targetLowPrice"),
                "profit_margins": info.get("profitMargins"),
                "revenue_growth": info.get("revenueGrowth"),
            }

            return {
                "summary": summary,
                "trend": trend_data,
                "history_1y": history_1y,
                "history_6mo": history_6mo,
                "history_3mo": history_3mo,
            }
        except Exception as e:
            self.logger.exception("Error fetching full stock data for %s: %s", symbol, e)
            return None

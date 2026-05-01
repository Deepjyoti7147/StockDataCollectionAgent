"""
Market data collector using yfinance.
"""

import logging
from datetime import datetime
import yfinance as yf
import pandas as pd

logger = logging.getLogger("market_collector.yf")

# Default Indian symbols (NSE)
DEFAULT_SYMBOLS = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "ICICIBANK.NS", "INFY.NS",
    "BHARTIARTL.NS", "SBI.NS", "LICI.NS", "ITC.NS", "HINDUNILVR.NS",
    "LT.NS", "BAJFINANCE.NS", "HCLTECH.NS", "MARUTI.NS", "SUNPHARMA.NS",
    "ADANIENT.NS", "KOTAKBANK.NS", "TITAN.NS", "ONGC.NS", "TATASTEEL.NS",
    "^NSEI", "^BSESN" # Nifty 50 and Sensex
]

class MarketCollector:
    def __init__(self, symbols: list[str] = None):
        self.symbols = symbols or DEFAULT_SYMBOLS

    def fetch_latest_prices(self, interval: str = "5m", period: str = "1d") -> list[dict]:
        """Fetch OHLCV data for all symbols."""
        all_data = []
        
        logger.info("Fetching %s data for %d symbols", interval, len(self.symbols))
        
        for symbol in self.symbols:
            try:
                ticker = yf.Ticker(symbol)
                df = ticker.history(period=period, interval=interval)
                
                if df.empty:
                    continue
                
                # Convert DataFrame to list of dicts for DB insertion
                for timestamp, row in df.iterrows():
                    all_data.append({
                        "symbol": symbol,
                        "timestamp": timestamp.to_pydatetime(),
                        "open": float(row['Open']),
                        "high": float(row['High']),
                        "low": float(row['Low']),
                        "close": float(row['Close']),
                        "volume": int(row['Volume']),
                        "interval": interval
                    })
            except Exception as e:
                logger.error("Failed to fetch %s: %s", symbol, e)
                
        return all_data

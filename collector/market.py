"""
Market data collector using yfinance with bulk downloading and CSV support.
"""

import logging
import time
import os
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime

logger = logging.getLogger("market_collector.yf")

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

class MarketCollector:
    def __init__(self, csv_path: str = "data/tickerInfo.csv"):
        self.csv_path = csv_path
        self.ticker_map = self._load_tickers() # Map: SYMBOL.NS -> Company Name

    def _load_tickers(self) -> dict:
        """Load symbols and names from CSV and append .NS for Yahoo Finance."""
        if not os.path.exists(self.csv_path):
            logger.warning("CSV not found at %s. Using empty map.", self.csv_path)
            return {}
        
        try:
            df = pd.read_csv(self.csv_path)
            # Handle potential whitespace in headers
            df.columns = df.columns.str.strip()
            
            # Map SYMBOL.NS -> NAME OF COMPANY
            mapping = {
                f"{str(row['SYMBOL']).strip()}.NS": str(row['NAME OF COMPANY']).strip()
                for _, row in df.iterrows()
            }
            logger.info("Loaded %d tickers from CSV", len(mapping))
            return mapping
        except Exception as e:
            logger.error("Failed to load CSV: %s", e)
            return {}

    def fetch_all_prices(self, interval: str = "1d", delay: float = 4.0) -> list[dict]:
        """
        Fetch OHLCV data sequentially one-by-one to meet strict rate limits:
        - 1,000 calls/hour max
        - ~4.0s delay between calls
        """
        symbols = list(self.ticker_map.keys())
        all_records = []
        
        # Create a session with a browser-like User-Agent
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        
        logger.info("Starting sequential EOD fetch for %d symbols (delay=%ss)", len(symbols), delay)

        for i, symbol in enumerate(symbols, 1):
            try:
                # Sequential download one symbol at a time
                data = yf.download(
                    tickers=symbol,
                    period="1d",
                    interval=interval,
                    auto_adjust=True,
                    prepost=False,
                    threads=False,
                    progress=False,
                    session=session
                )

                if data.empty:
                    continue

                # For single ticker, yf.download returns flat columns (Open, High, Low, Close, Volume)
                symbol_df = data.dropna()
                company_name = self.ticker_map.get(symbol)
                
                for timestamp, row in symbol_df.iterrows():
                    all_records.append({
                        "symbol": symbol,
                        "company_name": company_name,
                        "timestamp": timestamp.to_pydatetime(),
                        "open": float(row['Open']),
                        "high": float(row['High']),
                        "low": float(row['Low']),
                        "close": float(row['Close']),
                        "volume": int(row['Volume']),
                        "interval": interval
                    })

                # Politeness delay to ensure we stay under 1,000 requests per hour
                if i < len(symbols):
                    time.sleep(delay)

            except Exception as e:
                logger.error("Error fetching %s: %s", symbol, e)

        logger.info("Fetch complete. Total records gathered: %d", len(all_records))
        return all_records

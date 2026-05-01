"""
Market data collector using yfinance with bulk downloading and CSV support.
"""

import logging
import time
import os
import pandas as pd
import yfinance as yf
from datetime import datetime

logger = logging.getLogger("market_collector.yf")

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

    def fetch_all_prices(self, interval: str = "5m", chunk_size: int = 100, delay: float = 2.0) -> list[dict]:
        """
        Fetch OHLCV data in chunks to avoid rate limits.
        Returns a list of records ready for DB insertion.
        """
        symbols = list(self.ticker_map.keys())
        all_records = []
        
        # Split symbols into chunks
        chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]
        
        logger.info("Starting bulk fetch for %d symbols in %d chunks", len(symbols), len(chunks))

        for i, chunk in enumerate(chunks, 1):
            try:
                logger.debug("Fetching chunk %d/%d (%d symbols)", i, len(chunks), len(chunk))
                
                # Bulk download entire chunk in one request
                data = yf.download(
                    tickers=chunk,
                    period="1d",
                    interval=interval,
                    group_by='ticker',
                    auto_adjust=True,
                    prepost=False,
                    threads=True, # Use threads for speed
                    progress=False
                )

                if data.empty:
                    continue

                # Parse the multi-index DataFrame
                for symbol in chunk:
                    if symbol not in data:
                        continue
                        
                    symbol_df = data[symbol].dropna()
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

                # Politeness delay between chunk requests
                if i < len(chunks):
                    time.sleep(delay)

            except Exception as e:
                logger.error("Error in chunk %d: %s", i, e)

        logger.info("Fetch complete. Total records gathered: %d", len(all_records))
        return all_records

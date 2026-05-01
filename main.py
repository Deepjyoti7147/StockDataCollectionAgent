"""
StockDataCollectionAgent – Main Entry Point
"""

import logging
import os
import sys
import signal
from datetime import datetime
import pytz

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

from collector.db import DBHandler
from collector.market import MarketCollector

# ── Logging ──────────────────────────────────────────────────────────────────

def _setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format='{"time":"%(asctime)s","level":"%(levelname)s","msg":%(message)r}',
        stream=sys.stdout
    )

logger = logging.getLogger("market_collector")

# ── Market Hours Check ────────────────────────────────────────────────────────

def is_market_open() -> bool:
    """Checks if NSE market is currently open (9:15 AM - 3:30 PM IST)."""
    tz = pytz.timezone("Asia/Kolkata")
    now = datetime.now(tz)
    
    # Monday = 0, Sunday = 6
    if now.weekday() >= 5:
        return False
        
    start_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
    end_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
    
    return start_time <= now <= end_time

# ── Globals ───────────────────────────────────────────────────────────────────

_db = None
_collector = None
_scheduler = None

# ── Jobs ──────────────────────────────────────────────────────────────────────

def collect_market_data() -> None:
    """Fetch and store latest price data."""
    if not is_market_open():
        logger.info("Market is currently closed. Skipping run.")
        return

    try:
        logger.info("Starting market data collection...")
        prices = _collector.fetch_all_prices(interval="5m", chunk_size=100, delay=2.0)
        inserted = _db.save_prices(prices)
        logger.info("Saved %d new price records", inserted)
    except Exception as e:
        logger.exception("Market collection failed: %s", e)

# ── Lifecycle ─────────────────────────────────────────────────────────────────

def _shutdown(signum, frame) -> None:
    logger.info("Shutting down...")
    if _scheduler: _scheduler.shutdown(wait=False)
    if _db: _db.close()
    sys.exit(0)

def main() -> None:
    global _db, _collector, _scheduler
    
    load_dotenv()
    _setup_logging(os.environ.get("LOG_LEVEL", "INFO"))

    pg_dsn = os.environ.get("POSTGRES_DSN")
    if not pg_dsn:
        pg_dsn = (
            f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
            f"port={os.environ.get('POSTGRES_PORT', '5432')} "
            f"dbname={os.environ.get('POSTGRES_DB', 'newsdb')} "
            f"user={os.environ.get('POSTGRES_USER', 'newsuser')} "
            f"password={os.environ.get('POSTGRES_PASSWORD', '')} "
        )

    _db = DBHandler(dsn=pg_dsn)
    _db.connect()
    
    _collector = MarketCollector()
    
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    _scheduler = BlockingScheduler(timezone="Asia/Kolkata")
    
    # Run every 5 minutes
    _scheduler.add_job(
        collect_market_data,
        trigger=IntervalTrigger(minutes=5),
        id="collect_prices",
        next_run_time=datetime.now()
    )

    logger.info("StockDataCollectionAgent started")
    try:
        _scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        _shutdown(0, None)

if __name__ == "__main__":
    main()

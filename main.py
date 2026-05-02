"""
StockDataCollectionAgent – Main Entry Point with API Trigger
"""

import logging
import os
import sys
import signal
from datetime import datetime
import pytz

from fastapi import FastAPI, BackgroundTasks
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
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
    if now.weekday() >= 5: return False
    start_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
    end_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return start_time <= now <= end_time

# ── Globals & App ─────────────────────────────────────────────────────────────

_db = None
_collector = None
_scheduler = None

app = FastAPI(title="StockDataCollectionAgent API")

# ── Jobs ──────────────────────────────────────────────────────────────────────

def collect_market_data(force: bool = False) -> None:
    """Fetch and store latest EOD price data."""
    try:
        logger.info("Starting EOD market data collection (force=%s)...", force)
        # Fetch sequentially to keep rate limit below 1000/hr (delay=4.0)
        prices = _collector.fetch_all_prices(interval="1d", delay=4.0)
        inserted = _db.save_prices(prices)
        logger.info("Saved %d new price records", inserted)
    except Exception as e:
        logger.exception("Market collection failed: %s", e)

# ── API Endpoints ─────────────────────────────────────────────────────────────

@app.get("/status")
def get_status():
    """Check health and last run status."""
    return {
        "status": "running",
        "market_open": is_market_open(),
        "time_ist": datetime.now(pytz.timezone("Asia/Kolkata")).isoformat()
    }

@app.post("/collect")
def trigger_collection(background_tasks: BackgroundTasks):
    """Manually trigger a collection cycle in the background."""
    background_tasks.add_task(collect_market_data, force=True)
    return {"message": "Collection triggered successfully"}

# ── Lifecycle ─────────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup_event():
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
    
    # Start Background Scheduler
    _scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
    _scheduler.add_job(
        collect_market_data,
        trigger=CronTrigger(hour=16, minute=0), # EOD run at 4:00 PM IST
        id="collect_prices"
    )
    _scheduler.start()
    logger.info("StockDataCollectionAgent Background Scheduler started")

@app.on_event("shutdown")
def shutdown_event():
    logger.info("Shutting down...")
    if _scheduler: _scheduler.shutdown()
    if _db: _db.close()

if __name__ == "__main__":
    # Run API server
    port = int(os.environ.get("PORT", "8001"))
    uvicorn.run(app, host="0.0.0.0", port=port)

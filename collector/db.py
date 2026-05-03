"""
PostgreSQL database handler for StockDataCollectionAgent.
"""

import logging
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional

import psycopg2
import psycopg2.extras
from psycopg2 import pool

logger = logging.getLogger("market_collector.db")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS stock_prices (
    id              BIGSERIAL PRIMARY KEY,
    symbol          TEXT        NOT NULL,
    company_name    TEXT,
    timestamp       TIMESTAMPTZ NOT NULL,
    open            REAL,
    high            REAL,
    low             REAL,
    close           REAL,
    adj_close       REAL,
    volume          BIGINT,
    dividends       REAL,
    stock_splits    REAL,
    interval        TEXT        NOT NULL, -- '1m', '5m', '1d'
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT stock_prices_uq UNIQUE (symbol, timestamp, interval)
);

CREATE INDEX IF NOT EXISTS idx_prices_symbol_time ON stock_prices (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_prices_timestamp   ON stock_prices (timestamp DESC);
"""

class DBHandler:
    _pool: Optional[pool.ThreadedConnectionPool] = None

    def __init__(self, dsn: str):
        self.dsn = dsn

    def connect(self, retries: int = 5, delay: float = 5.0) -> None:
        for attempt in range(1, retries + 1):
            try:
                self._pool = pool.ThreadedConnectionPool(1, 3, dsn=self.dsn)
                self._init_schema()
                return
            except Exception as exc:
                logger.warning("DB connect attempt %d/%d failed: %s", attempt, retries, exc)
                time.sleep(delay)
        raise RuntimeError("Could not connect to PostgreSQL")

    @contextmanager
    def _get_conn(self):
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    def _init_schema(self) -> None:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA_SQL)
                cur.execute("ALTER TABLE stock_prices ADD COLUMN IF NOT EXISTS adj_close REAL;")
                cur.execute("ALTER TABLE stock_prices ADD COLUMN IF NOT EXISTS dividends REAL;")
                cur.execute("ALTER TABLE stock_prices ADD COLUMN IF NOT EXISTS stock_splits REAL;")

    def save_prices(self, price_data: list[dict]) -> int:
        """Bulk insert prices; returns number of new rows inserted."""
        if not price_data:
            return 0
        
        inserted = 0
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                for row in price_data:
                    try:
                        cur.execute(
                            """
                            INSERT INTO stock_prices 
                                (symbol, company_name, timestamp, open, high, low, close,
                                 adj_close, volume, dividends, stock_splits, interval)
                            VALUES 
                                (%(symbol)s, %(company_name)s, %(timestamp)s, %(open)s, %(high)s,
                                 %(low)s, %(close)s, %(adj_close)s, %(volume)s, %(dividends)s,
                                 %(stock_splits)s, %(interval)s)
                            ON CONFLICT (symbol, timestamp, interval) DO NOTHING
                            """,
                            row
                        )
                        if cur.rowcount:
                            inserted += 1
                    except Exception as e:
                        logger.error("Error inserting %s: %s", row.get('symbol'), e)
                        conn.rollback()
        return inserted

    def close(self):
        if self._pool:
            self._pool.closeall()

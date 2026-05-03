# StockDataCollectionAgent 📈

A high-performance, memory-efficient market data collector designed to run on low-resource VMs (1vCPU, 1GB RAM). It fetches data for 2,300+ Indian stocks from Yahoo Finance and persists them into a PostgreSQL database.

## 🚀 Features

- **Bulk Data Fetching**: Optimized to handle 2,300+ tickers using chunked bulk downloads via `yfinance`.
- **Market Hours Aware**: Intelligent scheduling that only polls the NSE during trading hours (9:15 AM - 3:30 PM IST) to save CPU and bandwidth.
- **API Triggered Collection**: Built-in FastAPI server allowing manual triggers via `POST /collect`.
- **Intelligent Deduplication**: Uses a unique constraint on `(symbol, timestamp, interval)` to ensure zero duplicate price records in the database.
- **Rate-Limit Protection & Fallback**: Implements a polite fetching strategy with a 5-second delay. If `yfinance` rate limits are hit, automatically falls back to `yahooquery` without missing a beat.
- **Automated Data Retention**: Automatically runs a scheduled database cleanup every Sunday at 2:00 AM IST to permanently delete records older than 18 months.
- **CSV Driven**: Easily manage your stock list by updating `data/tickerInfo.csv`.
- **Dockerized**: Pre-configured for Docker Compose with strict memory limits (256MB).

## 🛠️ Architecture

- **Backend**: Python 3.12, FastAPI, APScheduler
- **Data Source**: Yahoo Finance (`yfinance`)
- **Database**: PostgreSQL
- **Deployment**: GitHub Actions + Docker Compose

## 📡 API Endpoints

- `GET /status`: Returns current agent health, market status, and server time.
- `POST /collect`: Manually triggers a full collection cycle for all 2,300+ stocks (bypasses market hours check).

## 📋 Environment Variables

Set these in your `.env` file or GitHub Secrets:

| Variable | Description | Default |
|---|---|---|
| `POSTGRES_HOST` | Database VM IP/Hostname | `localhost` |
| `POSTGRES_DB` | Database Name | `newsdb` |
| `POSTGRES_USER` | Database User | `newsuser` |
| `POSTGRES_PASSWORD` | Database Password | - |
| `LOG_LEVEL` | Logging detail (INFO/DEBUG) | `INFO` |
| `PORT` | API Server Port | `8001` |

## 📦 Deployment

This project is configured for automated deployment via GitHub Actions.

1. Ensure the following secrets are set in your GitHub repository:
   - `SERVER_IP`
   - `SERVER_SSH` (Private Key)
   - `POSTGRES_HOST`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
2. Push to the `main` branch to trigger the CI/CD pipeline.

## 🗄️ Database Schema

The agent automatically manages the `stock_prices` table:

```sql
CREATE TABLE stock_prices (
    id              BIGSERIAL PRIMARY KEY,
    symbol          TEXT NOT NULL,
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
    interval        TEXT NOT NULL,
    fetched_at      TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT stock_prices_uq UNIQUE (symbol, timestamp, interval)
);
```

## 📄 License
MIT

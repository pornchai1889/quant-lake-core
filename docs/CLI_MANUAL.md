# Crypto ETL Pipeline - CLI Manual

**Version:** 1.0.0  
**Last Updated:** January 2026  
**Module:** `scripts/run_crypto_etl.py`

---

## Overview

The **Crypto ETL Pipeline** is a robust utility designed to extract historical OHLCV (Open, High, Low, Close, Volume) market data from the Binance Exchange, normalize it, and persist it into the **QuantLake** TimescaleDB instance.

This script supports two primary modes of operation:
1.  **Incremental Load (Default):** Fetches the most recent data based on a lookback window (e.g., last 24 hours). Ideal for scheduled Cron jobs.
2.  **Historical Backfill:** Fetches data for a specific date range (e.g., "January 2024"). Ideal for populating historical datasets or repairing data gaps.

> **Note:** The fetching engine implements **automatic pagination**, allowing for the retrieval of extensive datasets (exceeding the standard 1,000 candle limit) in a single execution without data loss.

---

## Usage Syntax

Run the script from the project root directory using the Python interpreter:

```bash
python scripts/run_crypto_etl.py [OPTIONS]

```

### Command-Line Arguments

| Argument | Type | Format | Default | Description |
| --- | --- | --- | --- | --- |
| `--symbols` | List | `SYM/USDT` | *Config* | A space-separated list of trading pairs to process. |
| `--interval` | String | `1m`, `1h`, `1d` | *Config* | The timeframe interval for the OHLCV candles. |
| `--days` | Integer | `N` | *Config* | **Incremental Mode:** Number of days to look back from the current time. Ignored if `--start-date` is set. |
| `--start-date` | Date | `YYYY-MM-DD` | `None` | **Backfill Mode:** The specific start date for data retrieval (UTC). |
| `--end-date` | Date | `YYYY-MM-DD` | `NOW` | **Backfill Mode:** The specific end date. If omitted, defaults to the current UTC timestamp. |

---

## Usage Examples

### 1. Standard Incremental Run (Daily Cron)

Executes the pipeline using defaults defined in `configs/etl_config.yaml`. This is best for automated daily updates.

```bash
python scripts/run_crypto_etl.py

```

### 2. Manual Short-Term Lookback

Fetches data for the last 7 days for the default symbols. Useful for quick data repairs.

```bash
python scripts/run_crypto_etl.py --days 7

```

### 3. Historical Backfill (Specific Month)

Retrieves a full month of data for a specific period. The system handles pagination automatically.

```bash
# Example: Fetch January 2024 data
python scripts/run_crypto_etl.py \
  --start-date 2024-01-01 \
  --end-date 2024-01-31

```

### 4. Backfill from Date to Present

Fetches all data starting from a specific date up to the current moment.

```bash
# Fetch from Jan 1st, 2025 to NOW
python scripts/run_crypto_etl.py --start-date 2025-01-01

```

### 5. Specific Assets & Intervals

Overrides the configuration to fetch specific pairs and timeframes.

```bash
# Fetch 15-minute candles for BTC and ETH only
python scripts/run_crypto_etl.py \
  --symbols BTC/USDT ETH/USDT \
  --interval 15m \
  --days 3

```

---

## Important Notes

* **Timezone:** All dates provided via CLI are treated as **UTC**. The database stores all timestamps in UTC to ensure consistency across global markets.
* **Data Integrity (Upsert):** The pipeline uses an *Upsert* strategy (Update on Conflict). If you re-run the script over an existing period, it will update the existing records rather than creating duplicates.
* **API Limits:** While the script handles pagination, aggressive backfilling (e.g., 5 years of 1-minute data) may hit Binance API rate limits. It is recommended to backfill in monthly or yearly chunks for very large datasets.

#!/usr/bin/env python3
"""
Yahoo Finance ETL Execution Script.

This script manages the extraction of historical market data (OHLCV) from Yahoo Finance
and loads it into the centralized data lake. It is designed to handle various asset classes
supported by Yahoo Finance (Stocks, Indices, ETFs).

Architecture Principles:
    1. Separation of Concerns: This script handles DATA ingestion only. It assumes ASSETS
       are already registered in the database (Master Data Management).
    2. Idempotency: The loading process uses UPSERT operations, allowing re-runs without
       creating duplicate records.

Usage:
    1. Standard Run (Default from config):
       python scripts/run_yahoo_etl.py

    2. Run for specific symbols (e.g., Thai Stocks):
       python scripts/run_yahoo_etl.py --symbols PTT.BK AOT.BK --days 30

    3. Backfill Historical Data:
       python scripts/run_yahoo_etl.py --symbols AAPL --start-date 2020-01-01 --end-date 2023-12-31
"""

import argparse
import logging
import sys
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import yaml

import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

# ------------------------------------------------------------------------------
# Path Setup
# ------------------------------------------------------------------------------
# Add the project root to sys.path to allow imports from 'src'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.core.config import settings  # noqa: E402
from src.database.connection import SessionLocal  # noqa: E402
from src.database.models import Asset, MarketQuote  # noqa: E402
from src.data_ingestion.yahoo.yfinance_fetcher import YahooFinanceFetcher  # noqa: E402


# ------------------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("yahoo_etl")


def load_etl_config(config_path: str = "configs/etl_config.yaml") -> Dict[str, Any]:
    """
    Load ETL configuration from a YAML file.

    Args:
        config_path (str): Relative path to the config file.

    Returns:
        Dict[str, Any]: The configuration dictionary.
    """
    try:
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        full_path = os.path.join(base_path, config_path)

        with open(full_path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config file: {e}")
        return {}


def get_asset(session: Session, symbol: str) -> Optional[Asset]:
    """
    Retrieve an asset from the database by its symbol.

    Note: Unlike crypto, Yahoo assets might belong to various exchanges (SET, NASDAQ).
    This function searches primarily by symbol.

    Args:
        session (Session): The database session.
        symbol (str): The asset symbol (e.g., 'AAPL', 'PTT.BK').

    Returns:
        Optional[Asset]: The SQLAlchemy Asset object if found, otherwise None.
    """
    return (
        session.query(Asset)
        .filter(Asset.symbol == symbol)
        # Optional: Exclude crypto exchanges if you want strict separation
        # .filter(Asset.exchange != 'BINANCE') 
        .one_or_none()
    )


def save_market_data(session: Session, asset_id: int, df: pd.DataFrame) -> int:
    """
    Bulk upsert market data into the database.
    Uses PostgreSQL 'ON CONFLICT DO UPDATE' to handle duplicate timestamps.

    Args:
        session (Session): The database session.
        asset_id (int): The foreign key ID of the asset.
        df (pd.DataFrame): The DataFrame containing OHLCV data.

    Returns:
        int: Number of records processed.
    """
    if df.empty:
        return 0

    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        records.append(
            {
                "time": row["time"],
                "asset_id": asset_id,
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
            }
        )

    stmt = insert(MarketQuote).values(records)

    # Upsert Logic: Update OHLCV if record already exists for (time, asset_id)
    stmt = stmt.on_conflict_do_update(
        index_elements=["time", "asset_id"],
        set_={
            "open": stmt.excluded.open,
            "high": stmt.excluded.high,
            "low": stmt.excluded.low,
            "close": stmt.excluded.close,
            "volume": stmt.excluded.volume,
        },
    )

    session.execute(stmt)
    session.commit()

    return len(records)


def run_etl(
    symbols: List[str],
    interval: str,
    start_date: datetime,
    end_date: datetime,
) -> None:
    """
    Execute the ETL pipeline for the specified parameters.

    Args:
        symbols (List[str]): List of stock tickers.
        interval (str): Timeframe interval (e.g., '1d', '1h').
        start_date (datetime): Start datetime (UTC).
        end_date (datetime): End datetime (UTC).
    """
    logger.info(
        f"Starting Yahoo ETL Job for {len(symbols)} symbols. "
        f"Interval: {interval}. Range: {start_date} to {end_date}"
    )

    # 1. Initialize Fetcher
    fetcher = YahooFinanceFetcher()

    # 2. Database Session
    session = SessionLocal()
    try:
        for symbol in symbols:
            try:
                logger.info(f"Processing {symbol}...")

                # Step A: Validate Asset Existence
                asset = get_asset(session, symbol)

                if not asset:
                    logger.error(f"Asset '{symbol}' NOT FOUND in database. Skipping.")
                    logger.error(
                        "ACTION REQUIRED: Please register this asset in 'configs/assets.yaml' "
                        "and run 'seed_assets.py'."
                    )
                    continue
                
                if not asset.is_active:
                    logger.warning(f"Asset '{symbol}' is marked as inactive. Skipping.")
                    continue

                # Step B: Extract (Fetch Data)
                # Note: yfinance fetcher handles the datetime precision internally now.
                df = fetcher.fetch_ohlcv(
                    symbol=symbol,
                    interval=interval,
                    start_date=start_date,
                    end_date=end_date
                )

                if df.empty:
                    logger.warning(f"No data returned for {symbol}.")
                    continue

                # Step C: Load (Save to DB)
                count = save_market_data(session, asset.id, df)
                logger.info(f"Successfully saved {count} records for {symbol}.")

            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")
                continue

    finally:
        session.close()
        logger.info("Yahoo ETL Job Completed.")


def parse_date_arg(date_str: str) -> datetime:
    """
    Parse CLI date argument into UTC datetime.
    """
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        msg = f"Not a valid date: '{date_str}'. Expected format: YYYY-MM-DD."
        raise argparse.ArgumentTypeError(msg)


if __name__ == "__main__":
    # 1. Load Configuration
    config = load_etl_config()
    yahoo_config = config.get("yahoo", {}) # Expecting a 'yahoo' key in config

    # Defaults
    default_symbols = yahoo_config.get("symbols", ["SPY"]) # Default fallback
    default_interval = yahoo_config.get("intervals", ["1d"])[0]
    default_days = yahoo_config.get("lookback_days", 1)

    # 2. Setup CLI Argument Parser
    parser = argparse.ArgumentParser(description="Run Yahoo Finance ETL Pipeline")

    parser.add_argument(
        "--symbols",
        nargs="+",
        default=default_symbols,
        help="List of symbols to fetch (e.g. AAPL MSFT PTT.BK)",
    )
    parser.add_argument(
        "--interval",
        type=str,
        default=default_interval,
        help="Timeframe interval (e.g. 1d, 1h)",
    )

    # Date Range Arguments
    parser.add_argument(
        "--start-date",
        type=parse_date_arg,
        help="Start date (YYYY-MM-DD) for backfilling.",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date_arg,
        help="End date (YYYY-MM-DD). Defaults to NOW if not specified.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=default_days,
        help="Number of lookback days (only used if --start-date is NOT provided).",
    )

    args = parser.parse_args()

    # 3. Determine Time Range
    now_utc = datetime.now(timezone.utc)

    if args.start_date:
        start_date = args.start_date
        end_date = args.end_date if args.end_date else now_utc
    else:
        end_date = now_utc
        start_date = end_date - timedelta(days=args.days)

    # 4. Execute
    run_etl(
        symbols=args.symbols,
        interval=args.interval,
        start_date=start_date,
        end_date=end_date,
    )
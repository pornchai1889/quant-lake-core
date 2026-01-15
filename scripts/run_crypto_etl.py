#!/usr/bin/env python3
"""
Crypto ETL Execution Script.

This script serves as the entry point for the Cryptocurrency ETL (Extract, Transform, Load) pipeline.
It orchestrates the retrieval of historical OHLCV data and persists it into the time-series database.

Features:
- Supports incremental loading via lookback days (default).
- Supports historical backfilling via specific start/end dates (CLI arguments).
- Handles efficient bulk upserts to prevent data duplication.

Usage:
    1. Standard Run (Default from config):
       python scripts/run_crypto_etl.py

    2. Manual Override (Last 5 days):
       python scripts/run_crypto_etl.py --days 5

    3. Backfill Specific Period:
       python scripts/run_crypto_etl.py --start-date 2025-01-01 --end-date 2025-01-31
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
from src.database.models import Asset, MarketQuote, AssetClass  # noqa: E402
from src.data_ingestion.crypto.binance_fetcher import BinanceFetcher  # noqa: E402


# ------------------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("crypto_etl")


def load_etl_config(config_path: str = "configs/etl_config.yaml") -> Dict[str, Any]:
    """
    Load ETL configuration from a YAML file.

    Args:
        config_path (str): Relative path to the config file.

    Returns:
        Dict[str, Any]: The configuration dictionary.
    """
    try:
        # Resolve absolute path relative to the project root
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        full_path = os.path.join(base_path, config_path)

        with open(full_path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config file: {e}")
        return {}


def get_or_create_asset(session: Session, symbol: str) -> Asset:
    """
    Retrieve an asset from the database or create it if it doesn't exist.

    Args:
        session (Session): The database session.
        symbol (str): The asset symbol (e.g., 'BTC/USDT').

    Returns:
        Asset: The SQLAlchemy Asset object.
    """
    # Check if asset exists
    asset = (
        session.query(Asset)
        .filter(Asset.symbol == symbol, Asset.exchange == "BINANCE")
        .one_or_none()
    )

    if asset:
        return asset

    # Create new asset if not found
    logger.info(f"Asset '{symbol}' not found in DB. Creating new Master Data entry.")
    new_asset = Asset(
        symbol=symbol,
        asset_class=AssetClass.CRYPTO,
        exchange="BINANCE",
        name=f"Crypto {symbol}",
        is_active=True,
    )
    session.add(new_asset)
    session.commit()
    session.refresh(new_asset)
    return new_asset


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

    # Convert DataFrame to list of dictionaries for bulk insertion
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

    # Prepare SQLAlchemy Upsert Statement
    stmt = insert(MarketQuote).values(records)

    # Define conflict resolution: Update values if (time, asset_id) already exists.
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
        symbols (List[str]): List of trading pairs.
        interval (str): Timeframe interval.
        start_date (datetime): Start datetime (UTC).
        end_date (datetime): End datetime (UTC).
    """
    logger.info(
        f"Starting ETL Job for {len(symbols)} symbols. "
        f"Interval: {interval}. Range: {start_date} to {end_date}"
    )

    # 1. Initialize Fetcher
    fetcher = BinanceFetcher()

    # 2. Database Session Management
    session = SessionLocal()
    try:
        for symbol in symbols:
            try:
                logger.info(f"Processing {symbol}...")

                # Step A: Ensure Asset Exists (Master Data)
                asset = get_or_create_asset(session, symbol)

                # Step B: Extract (Fetch Data)
                # Note: The fetcher now handles pagination automatically
                df = fetcher.fetch_ohlcv(
                    symbol=symbol,
                    interval=interval,
                    start_date=start_date,
                    end_date=end_date,
                    limit=1000,
                )

                if df.empty:
                    logger.warning(f"No data found for {symbol} in the specified range.")
                    continue

                # Step C: Load (Save to DB)
                count = save_market_data(session, asset.id, df)
                logger.info(f"Successfully saved {count} records for {symbol}.")

            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")
                # Continue to next symbol even if one fails
                continue

    finally:
        session.close()
        logger.info("ETL Job Completed.")


def parse_date_arg(date_str: str) -> datetime:
    """
    Helper function for argparse to parse date strings into UTC datetime objects.

    Args:
        date_str (str): Date string in 'YYYY-MM-DD' format.

    Returns:
        datetime: Timezone-aware (UTC) datetime object.

    Raises:
        argparse.ArgumentTypeError: If format is invalid.
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
    crypto_config = config.get("crypto", {})

    # Defaults from YAML
    default_symbols = crypto_config.get("symbols", ["BTC/USDT"])
    default_interval = crypto_config.get("intervals", ["1h"])[0]
    default_days = crypto_config.get("lookback_days", 1)

    # 2. Setup CLI Argument Parser
    parser = argparse.ArgumentParser(description="Run Crypto ETL Pipeline")

    parser.add_argument(
        "--symbols",
        nargs="+",
        default=default_symbols,
        help="List of symbols to fetch (e.g. BTC/USDT ETH/USDT)",
    )
    parser.add_argument(
        "--interval",
        type=str,
        default=default_interval,
        help="Timeframe interval (e.g. 1m, 1h, 1d)",
    )

    # Date Range Arguments
    parser.add_argument(
        "--start-date",
        type=parse_date_arg,
        help="Start date (YYYY-MM-DD) for backfilling. Overrides --days.",
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

    # 3. Determine Time Range Logic
    now_utc = datetime.now(timezone.utc)

    if args.start_date:
        # Mode: Backfill / Specific Range
        start_date = args.start_date
        # If end_date is not provided, default to NOW
        end_date = args.end_date if args.end_date else now_utc
    else:
        # Mode: Incremental / Recent History
        end_date = now_utc
        start_date = end_date - timedelta(days=args.days)

    # 4. Execute ETL
    run_etl(
        symbols=args.symbols,
        interval=args.interval,
        start_date=start_date,
        end_date=end_date,
    )
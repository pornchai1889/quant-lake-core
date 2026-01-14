#!/usr/bin/env python3
"""
Crypto ETL Execution Script.

This script serves as the entry point for the Cryptocurrency ETL (Extract, Transform, Load) pipeline.
It orchestrates the following steps:
1.  Initializes the Binance Data Fetcher.
2.  Retrieves historical OHLCV data for specified symbols.
3.  Ensures the asset exists in the Master Data table ('assets').
4.  Persists the market data into the Time-Series Database ('market_quotes') using efficient bulk upserts.

Usage:
    python scripts/run_crypto_etl.py --symbols BTC/USDT ETH/USDT --interval 1h --days 30
"""

import argparse
import logging
import sys
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
import yaml

import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

# ------------------------------------------------------------------------------
# Path Setup (To allow importing from 'src' when running as a script)
# ------------------------------------------------------------------------------
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
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def load_etl_config(config_path: str = "configs/etl_config.yaml") -> Dict[str, Any]:
    """Load ETL configuration from a YAML file."""
    try:
        # หา path ให้เจอไม่ว่าจะรันจาก folder ไหน
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
    # Normalize symbol to be consistent (Binance uses '/', we keep it or strip it depending on convention)
    # Here we assume the input symbol matches the exchange format.
    
    # Check if asset exists
    asset = session.query(Asset).filter(
        Asset.symbol == symbol,
        Asset.exchange == "BINANCE"
    ).one_or_none()

    if asset:
        return asset

    # Create new asset if not found
    logger.info(f"Asset '{symbol}' not found in DB. Creating new Master Data entry.")
    new_asset = Asset(
        symbol=symbol,
        asset_class=AssetClass.CRYPTO,
        exchange="BINANCE",
        name=f"Crypto {symbol}",
        is_active=True
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
        records.append({
            "time": row["time"],
            "asset_id": asset_id,
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
        })

    # Prepare SQLAlchemy Upsert Statement
    # Ref: https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#insert-on-conflict
    stmt = insert(MarketQuote).values(records)
    
    # Define conflict resolution: If (time, asset_id) exists, update the values.
    # This handles cases where data might be re-fetched or corrected.
    stmt = stmt.on_conflict_do_update(
        index_elements=['time', 'asset_id'],
        set_={
            'open': stmt.excluded.open,
            'high': stmt.excluded.high,
            'low': stmt.excluded.low,
            'close': stmt.excluded.close,
            'volume': stmt.excluded.volume,
        }
    )

    result = session.execute(stmt)
    session.commit()
    
    return len(records)


def run_etl(symbols: List[str], interval: str, days_back: int) -> None:
    """
    Main ETL function.

    Args:
        symbols (List[str]): List of trading pairs.
        interval (str): Timeframe interval.
        days_back (int): Number of days of history to fetch.
    """
    logger.info(f"Starting ETL Job for {len(symbols)} symbols. Interval: {interval}")

    # 1. Initialize Fetcher
    # API Keys can be loaded from settings if needed for higher limits/private data
    # api_key = settings.BINANCE_API_KEY
    fetcher = BinanceFetcher()

    # Calculate start time
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days_back)

    # 2. Database Session Management
    session = SessionLocal()
    try:
        for symbol in symbols:
            try:
                logger.info(f"Processing {symbol}...")

                # Step A: Ensure Asset Exists (Master Data)
                asset = get_or_create_asset(session, symbol)

                # Step B: Extract (Fetch Data)
                df = fetcher.fetch_ohlcv(
                    symbol=symbol,
                    interval=interval,
                    start_date=start_date,
                    end_date=end_date,
                    limit=1000 # Batch size handling handled by ccxt internally or simple limit
                )
                
                if df.empty:
                    logger.warning(f"No data found for {symbol}")
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

if __name__ == "__main__":
    # Load Config
    config = load_etl_config()
    crypto_config = config.get("crypto", {})
    
    # Get defaults from YAML
    default_symbols = crypto_config.get("symbols", ["BTC/USDT"])
    default_interval = crypto_config.get("intervals", ["1h"])[0] # เอาค่าแรก
    default_days = crypto_config.get("lookback_days", 30)

    parser = argparse.ArgumentParser(description="Run Crypto ETL Pipeline")
    
    # Implement YAML-based default configuration fallback
    parser.add_argument("--symbols", nargs="+", default=default_symbols)
    parser.add_argument("--interval", type=str, default=default_interval)
    parser.add_argument("--days", type=int, default=default_days)

    args = parser.parse_args()

    run_etl(symbols=args.symbols, interval=args.interval, days_back=args.days)
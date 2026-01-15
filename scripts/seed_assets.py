#!/usr/bin/env python3
"""
Asset Seeding Script (Smart Auto-Discovery).

This script initializes the Master Data (Assets) in the database based on the 
configuration file (configs/etl_config.yaml).

It acts as an intelligent 'Registrar' that:
1. Loads static seeds (Indices, Macro) explicitly defined in config.
2. Auto-detects asset classes for trading pairs (e.g., differentiating Stocks from Forex/Indices).
3. Ensures no duplication of assets in the database (Idempotent operation).

Usage:
    python scripts/seed_assets.py
"""

import logging
import sys
import os
import yaml
from typing import Dict, Any, List

# Add project root to sys.path to ensure module imports work correctly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy.orm import Session
from src.database.connection import SessionLocal
from src.database.models import Asset, AssetClass

# ------------------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("seed_assets")


class AssetClassifier:
    """
    Helper class to determine the correct AssetClass based on symbol patterns and source.
    Encapsulates the business logic for asset categorization.
    """

    @staticmethod
    def classify(symbol: str, source: str) -> AssetClass:
        """
        Determine the asset class.

        Args:
            symbol (str): The ticker symbol (e.g., '^DJI', 'AAPL', 'BTC/USDT').
            source (str): The data source name (e.g., 'BINANCE', 'YAHOO').

        Returns:
            AssetClass: The appropriate enum member.
        """
        # Rule 1: Binance symbols are strictly CRYPTO in this pipeline
        if source.upper() == "BINANCE":
            return AssetClass.CRYPTO

        # Rule 2: Heuristics for Yahoo Finance symbols
        if source.upper() == "YAHOO":
            symbol_upper = symbol.upper()
            
            # Index Convention: Starts with '^' (e.g., ^DJI, ^SET.BK)
            if symbol_upper.startswith("^"):
                return AssetClass.INDEX
            
            # Forex Convention: Ends with '=X' (e.g., EURUSD=X)
            if symbol_upper.endswith("=X"):
                return AssetClass.FOREX
            
            # Commodity/Future Convention: Ends with '=F' (e.g., GC=F for Gold)
            if symbol_upper.endswith("=F"):
                return AssetClass.COMMODITY
            
            # US Dollar Index Special Case
            if "DX-Y" in symbol_upper:
                return AssetClass.FOREX

            # Default to Stock for standard tickers (e.g., AAPL, PTT.BK)
            return AssetClass.STOCK

        # Fallback for unknown sources
        logger.warning(f"Unknown source '{source}' for symbol '{symbol}'. Defaulting to STOCK.")
        return AssetClass.STOCK


def load_config(config_path: str = "configs/etl_config.yaml") -> Dict[str, Any]:
    """
    Load and validate the YAML configuration file.

    Args:
        config_path (str): Relative path to the config file.

    Returns:
        Dict[str, Any]: The parsed configuration dictionary.
    """
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    full_path = os.path.join(base_path, config_path)
    
    if not os.path.exists(full_path):
        logger.error(f"Config file not found at: {full_path}")
        sys.exit(1)

    try:
        with open(full_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.error(f"Failed to parse config file: {e}")
        sys.exit(1)


def upsert_asset(session: Session, asset_data: Dict[str, Any]) -> None:
    """
    Insert a new asset or skip if it already exists (Idempotent).
    
    Args:
        session (Session): Database session.
        asset_data (Dict[str, Any]): Dictionary containing asset details.
                                     Must include 'symbol', 'exchange', 'asset_class'.
    """
    symbol = asset_data["symbol"]
    exchange = asset_data["exchange"]
    
    # Ensure asset_class is a valid Enum member
    raw_class = asset_data["asset_class"]
    try:
        # Handle both string input ("STOCK") and Enum input (AssetClass.STOCK)
        asset_class_enum = (
            raw_class if isinstance(raw_class, AssetClass) 
            else AssetClass(raw_class)
        )
    except ValueError:
        logger.error(f"Invalid Asset Class '{raw_class}' for {symbol}. Skipping.")
        return

    name = asset_data.get("name", symbol)
    description = asset_data.get("description", "")

    # Check for existence
    existing_asset = (
        session.query(Asset)
        .filter(Asset.symbol == symbol, Asset.exchange == exchange)
        .one_or_none()
    )

    if existing_asset:
        # Optional: update logic could go here if we wanted to force updates
        logger.info(f"Skipping existing asset: {symbol} ({exchange})")
    else:
        logger.info(f"Registering new Asset: {symbol} [{asset_class_enum.value}] via {exchange}")
        new_asset = Asset(
            symbol=symbol,
            exchange=exchange,
            asset_class=asset_class_enum,
            name=name,
            description=description,
            is_active=True
        )
        session.add(new_asset)


def seed_master_data() -> None:
    """
    Main execution routine.
    Orchestrates the loading of config and processing of all asset sections.
    """
    config = load_config()
    session = SessionLocal()
    
    try:
        # ----------------------------------------------------------------------
        # 1. Process Special/Static Seeds (Indices, Macro)
        # ----------------------------------------------------------------------
        seeds_config = config.get("seeds", {})
        
        for category, items in seeds_config.items():
            if not isinstance(items, list):
                continue
            
            for item in items:
                if "symbol" in item and "asset_class" in item:
                    upsert_asset(session, item)

        # ----------------------------------------------------------------------
        # 2. Process Active Trading Assets (Crypto)
        # ----------------------------------------------------------------------
        crypto_config = config.get("crypto", {})
        crypto_source = crypto_config.get("source", "BINANCE")
        
        for symbol in crypto_config.get("symbols", []):
            asset_class = AssetClassifier.classify(symbol, crypto_source)
            asset_data = {
                "symbol": symbol,
                "exchange": crypto_source,
                "asset_class": asset_class,
                "name": f"Crypto {symbol}",
                "description": "Auto-seeded active trading pair"
            }
            upsert_asset(session, asset_data)

        # ----------------------------------------------------------------------
        # 3. Process Active Trading Assets (Yahoo Finance: Stocks, Forex, etc.)
        # ----------------------------------------------------------------------
        # Updated to look for 'yahoo_finance' key instead of 'stocks'
        yahoo_config = config.get("yahoo_finance", {})
        yahoo_source = yahoo_config.get("source", "YAHOO")
        
        for symbol in yahoo_config.get("symbols", []):
            # Intelligent classification based on Symbol Pattern
            # e.g., '^SET.BK' -> INDEX, 'EURUSD=X' -> FOREX
            asset_class = AssetClassifier.classify(symbol, yahoo_source)
            
            asset_data = {
                "symbol": symbol,
                "exchange": yahoo_source,
                "asset_class": asset_class,
                "name": f"{asset_class.value.capitalize()} {symbol}",
                "description": "Auto-seeded active trading asset"
            }
            upsert_asset(session, asset_data)

        # Commit all changes atomically
        session.commit()
        logger.info("Master Data Seeding completed successfully.")

    except Exception as e:
        session.rollback()
        logger.exception(f"Critical error during seeding: {e}")
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    seed_master_data()
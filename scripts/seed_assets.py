#!/usr/bin/env python3
"""
Asset Seeding Script.

This script initializes the Master Data (Assets) in the database based on the 
configuration file (configs/etl_config.yaml).

It acts as the 'Registrar' ensuring that:
1. Special assets (Indices, Macro) exist.
2. Active trading assets (Crypto, Stocks) exist.

Usage:
    python scripts/seed_assets.py
"""

import logging
import sys
import os
import yaml
from typing import Dict, Any, List

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy.orm import Session
from src.database.connection import SessionLocal
from src.database.models import Asset, AssetClass

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("seed_assets")


def load_config(config_path: str = "configs/etl_config.yaml") -> Dict[str, Any]:
    """Load the YAML configuration file."""
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    full_path = os.path.join(base_path, config_path)
    
    if not os.path.exists(full_path):
        logger.error(f"Config file not found: {full_path}")
        sys.exit(1)

    with open(full_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def upsert_asset(session: Session, asset_data: Dict[str, Any]) -> None:
    """
    Insert or Update an asset in the database.
    Checks existence by (symbol, exchange).
    """
    symbol = asset_data["symbol"]
    exchange = asset_data["exchange"]
    asset_class = asset_data["asset_class"]
    name = asset_data.get("name", symbol)
    description = asset_data.get("description", "")

    # Check if asset exists
    existing_asset = (
        session.query(Asset)
        .filter(Asset.symbol == symbol, Asset.exchange == exchange)
        .one_or_none()
    )

    if existing_asset:
        logger.info(f"Asset already exists: {symbol} ({exchange}) - Skipping.")
        # Optional: Update name/description if needed
        # existing_asset.name = name
        # existing_asset.description = description
    else:
        logger.info(f"Creating new Asset: {symbol} ({exchange}) [{asset_class}]")
        new_asset = Asset(
            symbol=symbol,
            exchange=exchange,
            asset_class=AssetClass(asset_class), # Validate against Enum
            name=name,
            description=description,
            is_active=True
        )
        session.add(new_asset)


def seed_master_data() -> None:
    """Main seeding logic."""
    config = load_config()
    session = SessionLocal()
    
    try:
        # -----------------------------------------------------
        # 1. Seed Special Assets (Seeds Section)
        # -----------------------------------------------------
        seeds_config = config.get("seeds", {})
        
        # Process Indices
        for item in seeds_config.get("indices", []):
            upsert_asset(session, item)

        # Process Macro
        for item in seeds_config.get("macro", []):
            upsert_asset(session, item)

        # -----------------------------------------------------
        # 2. Seed Active Assets (Crypto Section)
        # -----------------------------------------------------
        crypto_config = config.get("crypto", {})
        for symbol in crypto_config.get("symbols", []):
            # Auto-construct asset data for Crypto
            asset_data = {
                "symbol": symbol,
                "exchange": crypto_config.get("source", "BINANCE"),
                "asset_class": "CRYPTO",
                "name": f"Crypto {symbol}",
                "description": "Auto-seeded from crypto config"
            }
            upsert_asset(session, asset_data)

        # -----------------------------------------------------
        # 3. Seed Active Assets (Stocks Section)
        # -----------------------------------------------------
        stocks_config = config.get("stocks", {})
        for symbol in stocks_config.get("symbols", []):
            # Auto-construct asset data for Stocks
            asset_data = {
                "symbol": symbol,
                "exchange": stocks_config.get("source", "YAHOO"),
                "asset_class": "STOCK",
                "name": f"Stock {symbol}",
                "description": "Auto-seeded from stocks config"
            }
            upsert_asset(session, asset_data)

        session.commit()
        logger.info("Seeding completed successfully.")

    except Exception as e:
        session.rollback()
        logger.exception(f"Seeding failed: {e}")
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    seed_master_data()
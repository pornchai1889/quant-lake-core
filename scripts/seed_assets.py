#!/usr/bin/env python3
"""
Master Asset Seeding Script.

This script acts as the primary 'Registrar' for the system.
It reads the Master Data Registry from 'configs/assets.yaml' and ensures
all assets are correctly registered in the database with their canonical
identities (Symbol + Exchange + Asset Class).

This script is idempotent: running it multiple times is safe and will not
duplicate data.

Usage:
    python scripts/seed_assets.py
"""

import logging
import sys
import os
import yaml
from typing import Dict, Any, List, Optional

# ------------------------------------------------------------------------------
# Path Setup
# ------------------------------------------------------------------------------
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


def load_config(config_path: str = "configs/assets.yaml") -> Dict[str, Any]:
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
        logger.error(f"Master Registry file not found at: {full_path}")
        logger.error("Please ensure 'configs/assets.yaml' exists.")
        sys.exit(1)

    try:
        with open(full_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.error(f"Failed to parse config file: {e}")
        sys.exit(1)


def upsert_asset(session: Session, asset_data: Dict[str, Any]) -> None:
    """
    Insert a new asset or update if it exists (Idempotent).
    
    Args:
        session (Session): Database session.
        asset_data (Dict[str, Any]): Asset details. Must include:
                                     - symbol
                                     - exchange
                                     - asset_class
    """
    try:
        symbol = asset_data["symbol"]
        exchange = asset_data["exchange"]
        raw_class = asset_data["asset_class"]
        
        # Validate Asset Class Enum
        try:
            asset_class_enum = (
                raw_class if isinstance(raw_class, AssetClass) 
                else AssetClass(raw_class)
            )
        except ValueError:
            logger.error(f"Invalid Asset Class '{raw_class}' for {symbol}. Skipping.")
            return

        name = asset_data.get("name", symbol)
        description = asset_data.get("description", "")

        # Check for existence (Composite Key: Symbol + Exchange)
        existing_asset = (
            session.query(Asset)
            .filter(Asset.symbol == symbol, Asset.exchange == exchange)
            .one_or_none()
        )

        if existing_asset:
            # We skip updates to preserve manual edits in DB, 
            # or uncomment below to force update descriptions from config.
            logger.debug(f"Asset already exists: {symbol} ({exchange})")
        else:
            logger.info(f"Registering: {symbol:<10} | {exchange:<10} | {asset_class_enum.value}")
            new_asset = Asset(
                symbol=symbol,
                exchange=exchange,
                asset_class=asset_class_enum,
                name=name,
                description=description,
                is_active=True
            )
            session.add(new_asset)

    except KeyError as e:
        logger.error(f"Missing required field in asset data: {e} - Data: {asset_data}")


def process_asset_group(
    session: Session, 
    group_data: Dict[str, Any], 
    group_name: str
) -> None:
    """
    Process a group of assets that share default properties.
    
    Handles structure:
        default_exchange: "..."
        default_class: "..."
        items: [ ... ]

    Args:
        session (Session): DB Session.
        group_data (Dict): The configuration section for this group.
        group_name (str): Name of the group for logging (e.g., 'cryptocurrencies').
    """
    if not group_data:
        return

    # Extract defaults
    default_exchange = group_data.get("default_exchange")
    default_class = group_data.get("default_class")
    items = group_data.get("items", [])

    if not items:
        logger.warning(f"No items found in group '{group_name}'.")
        return

    for item in items:
        # Merge item data with defaults. Item specific data takes precedence.
        # Ensure we have specific values or fallback to defaults.
        item_exchange = item.get("exchange", default_exchange)
        item_class = item.get("asset_class", default_class)

        if not item_exchange or not item_class:
            logger.error(
                f"Asset '{item.get('symbol')}' in '{group_name}' is missing "
                "exchange or asset_class, and no defaults provided."
            )
            continue

        # Prepare payload
        asset_payload = {
            "symbol": item["symbol"],
            "name": item.get("name"),
            "description": item.get("description"),
            "exchange": item_exchange,
            "asset_class": item_class
        }
        
        upsert_asset(session, asset_payload)


def seed_master_data() -> None:
    """
    Main execution routine.
    Iterates through the new 'assets.yaml' structure.
    """
    config = load_config()
    session = SessionLocal()
    
    logger.info("Starting Master Data Seeding...")
    
    try:
        # ----------------------------------------------------------------------
        # 1. Simple Lists (Explicit Definition)
        # Structure: List[Dict]
        # ----------------------------------------------------------------------
        # Special Assets & Market Indices usually defined explicitly in list
        simple_sections = ["special_assets", "market_indices", "seeds"] 
        
        for section in simple_sections:
            if section in config:
                # Handle nested keys if present (e.g. seeds.indices in old config, or direct list)
                data = config[section]
                
                # If it's a list, process directly
                if isinstance(data, list):
                    for item in data:
                        upsert_asset(session, item)
                
                # If it's a dict (like old seeds: {indices: [], macro: []}), handle sub-lists
                elif isinstance(data, dict):
                     for sub_key, sub_items in data.items():
                         if isinstance(sub_items, list):
                             for item in sub_items:
                                 upsert_asset(session, item)

        # ----------------------------------------------------------------------
        # 2. Asset Groups (With Defaults)
        # Structure: { default_..., items: [...] }
        # ----------------------------------------------------------------------
        # Cryptocurrencies, Commodities, Forex
        standard_groups = ["cryptocurrencies", "commodities", "forex"]
        
        for group in standard_groups:
            if group in config:
                process_asset_group(session, config[group], group)

        # ----------------------------------------------------------------------
        # 3. Nested Asset Groups (Stocks by Sector)
        # Structure: stocks: { us_tech: { ... }, th_bluechips: { ... } }
        # ----------------------------------------------------------------------
        stocks_config = config.get("stocks", {})
        
        # Iterate over each sector (us_tech, th_bluechips)
        for sector_name, sector_data in stocks_config.items():
            if isinstance(sector_data, dict) and "items" in sector_data:
                process_asset_group(session, sector_data, f"stocks.{sector_name}")

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
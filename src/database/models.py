"""
SQLAlchemy Models Definition.

This module defines the database schema using SQLAlchemy ORM (Object Relational Mapper).
It maps Python classes to the PostgreSQL/TimescaleDB tables defined in the initialization scripts.

The models strictly follow the schema defined in 'database/init/*.sql'.
Ref: SQLAlchemy 2.0 Declarative Mapping
"""

from datetime import datetime, date
from enum import Enum as PyEnum
from typing import Optional, List, Any

from sqlalchemy import (
    String,
    Integer,
    Double,
    Boolean,
    DateTime,
    Date,
    ForeignKey,
    JSON,
    Enum,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from src.database.connection import Base


# ------------------------------------------------------------------------------
# Enums (Must match database/init/02_schema_master.sql)
# ------------------------------------------------------------------------------
class AssetClass(str, PyEnum):
    """
    Enumeration for Asset Classes.
    Matches the PostgreSQL ENUM type 'asset_class_enum'.
    """

    CRYPTO = "CRYPTO"
    STOCK = "STOCK"
    FOREX = "FOREX"
    COMMODITY = "COMMODITY"


# ------------------------------------------------------------------------------
# Master Data Models
# ------------------------------------------------------------------------------
class Asset(Base):
    """
    Represents a financial instrument (Master Data).
    Maps to the 'assets' table.
    """

    __tablename__ = "assets"

    # Primary Key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Core Identity Fields
    symbol: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    asset_class: Mapped[AssetClass] = mapped_column(
        Enum(AssetClass, name="asset_class_enum"), nullable=False, index=True
    )
    exchange: Mapped[str] = mapped_column(String(50), nullable=False)

    # Metadata
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    # Audit Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Table Constraints (Must match SQL Schema)
    __table_args__ = (
        UniqueConstraint("symbol", "exchange", name="uq_asset_symbol_exchange"),
    )

    # Relationships (One-to-Many)
    # Using cascade="all, delete-orphan" to clean up child data if an asset is deleted (conceptually).
    quotes: Mapped[List["MarketQuote"]] = relationship(back_populates="asset")
    financials: Mapped[List["FinancialStatement"]] = relationship(
        back_populates="asset"
    )

    def __repr__(self) -> str:
        return f"<Asset(symbol='{self.symbol}', exchange='{self.exchange}', class='{self.asset_class.value}')>"


# ------------------------------------------------------------------------------
# Time-Series Data Models (Hypertables)
# ------------------------------------------------------------------------------
class MarketQuote(Base):
    """
    Represents OHLCV market data.
    Maps to the 'market_quotes' hypertable in TimescaleDB.
    """

    __tablename__ = "market_quotes"

    # Composite Primary Key (Time + Asset) for TimescaleDB
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("assets.id"), primary_key=True)

    # OHLCV Data
    open: Mapped[float] = mapped_column(Double, nullable=False)
    high: Mapped[float] = mapped_column(Double, nullable=False)
    low: Mapped[float] = mapped_column(Double, nullable=False)
    close: Mapped[float] = mapped_column(Double, nullable=False)
    volume: Mapped[float] = mapped_column(Double, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relationship
    asset: Mapped["Asset"] = relationship(back_populates="quotes")

    def __repr__(self) -> str:
        return f"<MarketQuote(time='{self.time}', asset_id={self.asset_id}, close={self.close})>"


class FinancialStatement(Base):
    """
    Represents fundamental financial data (Quarterly/Yearly Reports).
    Maps to the 'financial_statements' table.
    """

    __tablename__ = "financial_statements"

    # Composite Primary Key
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("assets.id"), primary_key=True)
    period_type: Mapped[str] = mapped_column(String(10), primary_key=True)  # 'Q' or 'Y'

    # Reporting Period
    period_end: Mapped[date] = mapped_column(Date, nullable=False)

    # Key Metrics (Nullable because not all assets report all metrics)
    revenue: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    net_income: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    eps: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    total_assets: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    total_liabilities: Mapped[Optional[float]] = mapped_column(Double, nullable=True)

    # Flexible storage for extra fields
    raw_data: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=True)

    # Relationship
    asset: Mapped["Asset"] = relationship(back_populates="financials")

    def __repr__(self) -> str:
        return f"<FinancialStatement(asset_id={self.asset_id}, period='{self.period_type}', date='{self.period_end}')>"


class MacroIndicator(Base):
    """
    Represents global economic indicators (e.g., CPI, GDP, Interest Rates).
    Maps to the 'macro_indicators' table.
    """

    __tablename__ = "macro_indicators"

    # Composite Primary Key
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    country: Mapped[str] = mapped_column(String(3), primary_key=True)  # ISO code
    indicator: Mapped[str] = mapped_column(String(50), primary_key=True)

    # Data
    value: Mapped[float] = mapped_column(Double, nullable=False)
    frequency: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    def __repr__(self) -> str:
        return f"<MacroIndicator(country='{self.country}', indicator='{self.indicator}', value={self.value})>"

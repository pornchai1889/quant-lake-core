"""
Base Data Ingestion Module.

This module defines the abstract base class for all data fetchers.
It enforces a consistent interface for retrieving financial data (OHLCV, Fundamentals)
from various sources (e.g., Binance, Yahoo Finance, AlphaVantage).

Strict type hinting and Pandas integration are used to ensure compatibility
with the downstream processing pipeline.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, List, Any, Dict

import pandas as pd


class BaseDataFetcher(ABC):
    """
    Abstract Base Class for Financial Data Fetchers.

    All specific data source implementations (e.g., BinanceFetcher, YahooFetcher)
    must inherit from this class and implement its abstract methods.
    """

    def __init__(self, source_name: str, api_key: Optional[str] = None):
        """
        Initialize the fetcher.

        Args:
            source_name (str): Identifier for the data source (e.g., 'BINANCE').
            api_key (Optional[str]): API Key for authentication (if required).
        """
        self.source_name = source_name.upper()
        self.api_key = api_key

    @abstractmethod
    def fetch_ohlcv(
        self,
        symbol: str,
        interval: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 1000
    ) -> pd.DataFrame:
        """
        Fetch historical OHLCV (Open, High, Low, Close, Volume) data.

        This method must be implemented by subclasses to retrieve time-series data
        and return it in a standardized Pandas DataFrame format.

        Standardized DataFrame Columns:
            - time (datetime, timezone-aware UTC)
            - open (float)
            - high (float)
            - low (float)
            - close (float)
            - volume (float)

        Args:
            symbol (str): The instrument symbol (e.g., 'BTC/USDT', 'AAPL').
            interval (str): Timeframe interval (e.g., '1h', '1d').
            start_date (Optional[datetime]): Start time for fetching data.
            end_date (Optional[datetime]): End time for fetching data.
            limit (int): Maximum number of data points to retrieve per request.

        Returns:
            pd.DataFrame: A standardized DataFrame containing OHLCV data.
        
        Raises:
            NotImplementedError: If the subclass does not implement this method.
            ValueError: If the response data is invalid or empty.
        """
        pass

    @abstractmethod
    def fetch_fundamental(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch fundamental data (e.g., Financial Statements, Market Cap).

        Args:
            symbol (str): The instrument symbol.

        Returns:
            Dict[str, Any]: A dictionary containing fundamental metrics.
                            Example: {'revenue': 1000, 'pe_ratio': 15.5}
        """
        pass

    def validate_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Utility method to validate the structure of the returned DataFrame.
        
        This ensures that the concrete implementation returns data compatible
        with the database schema.

        Args:
            df (pd.DataFrame): The DataFrame to validate.

        Returns:
            bool: True if the DataFrame has the required columns.

        Raises:
            ValueError: If required columns are missing.
        """
        required_columns = {'time', 'open', 'high', 'low', 'close', 'volume'}
        if not required_columns.issubset(df.columns):
            missing = required_columns - set(df.columns)
            raise ValueError(f"DataFrame is missing required columns: {missing}")
        
        if df.empty:
            # Depending on policy, empty might be valid (no data) or error.
            # Here we just log a warning conceptually, but return True.
            return True

        return True
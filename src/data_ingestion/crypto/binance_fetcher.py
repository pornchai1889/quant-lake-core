"""
Binance Data Fetcher Implementation.

This module implements the specific logic for retrieving cryptocurrency data
from the Binance exchange using the CCXT library. It inherits from the
abstract BaseDataFetcher to ensure consistency with the system's architecture.

The implementation handles pagination to support fetching large historical datasets
exceeding the exchange's API limits per request.

Dependencies:
    - ccxt: For unified exchange API handling.
    - pandas: For data structuring.
"""

from datetime import datetime
from typing import Optional, List, Any, Dict, Union

import ccxt
import pandas as pd

from src.data_ingestion.base import BaseDataFetcher


class BinanceFetcher(BaseDataFetcher):
    """
    Data fetcher implementation for Binance Exchange (Spot & Futures).

    Attributes:
        exchange (ccxt.binance): The CCXT exchange instance.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        use_futures: bool = False,
    ) -> None:
        """
        Initialize the Binance fetcher.

        Args:
            api_key (Optional[str]): Binance API Key (required for private endpoints).
            api_secret (Optional[str]): Binance API Secret.
            use_futures (bool): If True, connects to Binance Futures API. Default is Spot.
        """
        super().__init__(source_name="BINANCE", api_key=api_key)

        # Configure CCXT options
        options: Dict[str, Any] = {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,  # CCXT handles rate limiting automatically
            "options": {"defaultType": "future" if use_futures else "spot"},
        }

        # Initialize the exchange instance
        self.exchange = ccxt.binance(options)

        # Load markets to ensure symbols are available
        # This might take a moment on initialization
        self.exchange.load_markets()

    def fetch_ohlcv(
        self,
        symbol: str,
        interval: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 1000,
    ) -> pd.DataFrame:
        """
        Fetch OHLCV data from Binance with pagination support.

        This method automatically handles API limits by iterating through pages
        until the target end date is reached or no more data is available.

        Args:
            symbol (str): Trading pair symbol (e.g., 'BTC/USDT').
            interval (str): Timeframe (e.g., '1m', '1h', '1d').
            start_date (Optional[datetime]): Start time for fetching data.
            end_date (Optional[datetime]): End time for fetching data.
            limit (int): Number of candles to fetch per API call.

        Returns:
            pd.DataFrame: A standardized DataFrame containing OHLCV data.

        Raises:
            RuntimeError: If the API request fails or returns invalid data.
        """
        # Convert datetime to timestamp (milliseconds) for CCXT
        since: Optional[int] = None
        if start_date:
            since = int(start_date.timestamp() * 1000)

        end_timestamp: Optional[int] = None
        if end_date:
            end_timestamp = int(end_date.timestamp() * 1000)

        all_ohlcv: List[List[Union[int, float]]] = []

        # ---------------------------------------------------------
        # Pagination Loop
        # ---------------------------------------------------------
        while True:
            try:
                # Fetch a batch of data using CCXT
                # Structure: [[timestamp, open, high, low, close, volume], ...]
                batch = self.exchange.fetch_ohlcv(
                    symbol=symbol,
                    timeframe=interval,
                    since=since,
                    limit=limit,
                )

                if not batch:
                    break

                all_ohlcv.extend(batch)

                # Identify the timestamp of the last candle in the batch
                last_timestamp = int(batch[-1][0])

                # Check termination conditions:
                # 1. If we've reached or passed the end_date
                if end_timestamp and last_timestamp >= end_timestamp:
                    break

                # 2. If the batch size is smaller than the limit, we've reached the end
                if len(batch) < limit:
                    break

                # Update 'since' for the next iteration.
                # Adding 1ms ensures we don't re-fetch the exact same candle,
                # preventing infinite loops on duplicate timestamps.
                since = last_timestamp + 1

            except ccxt.BaseError as e:
                raise RuntimeError(
                    f"Failed to fetch data from Binance for {symbol}: {str(e)}"
                ) from e

        if not all_ohlcv:
            return pd.DataFrame()

        # ---------------------------------------------------------
        # Data Normalization
        # ---------------------------------------------------------
        # Convert to DataFrame
        df = pd.DataFrame(
            all_ohlcv,
            columns=["timestamp_ms", "open", "high", "low", "close", "volume"],
        )

        # Filter strictly by end_date if provided (to clip any overshoot)
        if end_timestamp:
            df = df[df["timestamp_ms"] <= end_timestamp]

        # 1. Convert timestamp (ms) to datetime objects (UTC)
        df["time"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)

        # 2. Drop the raw timestamp column
        df.drop(columns=["timestamp_ms"], inplace=True)

        # 3. Ensure column order matches the BaseDataFetcher schema
        df = df[["time", "open", "high", "low", "close", "volume"]]

        # 4. Remove duplicates based on time (safety check for pagination overlaps)
        df.drop_duplicates(subset=["time"], inplace=True)

        # 5. Validate schema using the base class utility
        self.validate_dataframe(df)

        return df

    def fetch_fundamental(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch 'fundamental-like' data for Cryptocurrency.

        Since crypto assets do not possess traditional balance sheets, this method
        retrieves 24h ticker statistics and market metadata as a proxy.

        Args:
            symbol (str): Trading pair symbol.

        Returns:
            Dict[str, Any]: Dictionary containing ticker statistics (e.g., 24h volume, vwap).

        Raises:
            RuntimeError: If the ticker data cannot be retrieved.
        """
        try:
            ticker: Dict[str, Any] = self.exchange.fetch_ticker(symbol)

            # Extract relevant metrics
            fundamentals = {
                "symbol": symbol,
                "last_price": ticker.get("last"),
                "24h_high": ticker.get("high"),
                "24h_low": ticker.get("low"),
                "24h_volume": ticker.get("baseVolume"),  # Volume in base asset
                "24h_quote_volume": ticker.get("quoteVolume"),  # Volume in quote asset
                "percentage_change": ticker.get("percentage"),
                "timestamp": datetime.now(),
            }
            return fundamentals

        except ccxt.BaseError as e:
            raise RuntimeError(
                f"Failed to fetch ticker for {symbol}: {str(e)}"
            ) from e
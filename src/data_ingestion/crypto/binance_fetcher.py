"""
Binance Data Fetcher Implementation.

This module implements the specific logic for retrieving cryptocurrency data
from the Binance exchange using the CCXT library. It inherits from the
abstract BaseDataFetcher to ensure consistency with the system's architecture.

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
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        use_futures: bool = False
    ):
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
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,  # CCXT handles rate limiting automatically
            'options': {
                'defaultType': 'future' if use_futures else 'spot'
            }
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
        limit: int = 1000
    ) -> pd.DataFrame:
        """
        Fetch OHLCV data from Binance.

        Maps CCXT's raw list response to the standardized DataFrame schema.

        Args:
            symbol (str): Trading pair symbol (e.g., 'BTC/USDT').
            interval (str): Timeframe (e.g., '1m', '1h', '1d').
            start_date (Optional[datetime]): Start time.
            end_date (Optional[datetime]): End time (not strictly used by CCXT basic fetch, mostly limit).
            limit (int): Number of candles to fetch.

        Returns:
            pd.DataFrame: Standardized OHLCV data.

        Raises:
            Exception: If API request fails (CCXT errors).
        """
        # Convert datetime to timestamp (milliseconds) for CCXT
        since: Optional[int] = None
        if start_date:
            since = int(start_date.timestamp() * 1000)

        # Fetch data using CCXT
        # structure: [[timestamp, open, high, low, close, volume], ...]
        try:
            ohlcv_data: List[List[Union[int, float]]] = self.exchange.fetch_ohlcv(
                symbol=symbol,
                timeframe=interval,
                since=since,
                limit=limit
            )
        except ccxt.BaseError as e:
            # Re-raise or log the error. For now, we propagate it.
            raise RuntimeError(f"Failed to fetch data from Binance for {symbol}: {str(e)}") from e

        if not ohlcv_data:
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(
            ohlcv_data,
            columns=['timestamp_ms', 'open', 'high', 'low', 'close', 'volume']
        )

        # Normalize Columns to adhere to BaseDataFetcher schema
        # 1. Convert timestamp (ms) to datetime objects (UTC)
        df['time'] = pd.to_datetime(df['timestamp_ms'], unit='ms', utc=True)
        
        # 2. Drop the raw timestamp column
        df.drop(columns=['timestamp_ms'], inplace=True)

        # 3. Ensure column order
        df = df[['time', 'open', 'high', 'low', 'close', 'volume']]

        # 4. Validate schema
        self.validate_dataframe(df)

        return df

    def fetch_fundamental(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch 'fundamental' data for Crypto.
        
        Since crypto doesn't have balance sheets like stocks, this fetches
        24h ticker statistics and market info as a proxy for fundamentals.

        Args:
            symbol (str): Trading pair symbol.

        Returns:
            Dict[str, Any]: Dictionary containing ticker statistics (e.g., 24h volume, vwap).
        """
        try:
            ticker: Dict[str, Any] = self.exchange.fetch_ticker(symbol)
            
            # Extract relevant 'fundamental-like' metrics
            fundamentals = {
                'symbol': symbol,
                'last_price': ticker.get('last'),
                '24h_high': ticker.get('high'),
                '24h_low': ticker.get('low'),
                '24h_volume': ticker.get('baseVolume'), # Volume in base currency (e.g., BTC)
                '24h_quote_volume': ticker.get('quoteVolume'), # Volume in quote currency (e.g., USDT)
                'percentage_change': ticker.get('percentage'),
                'timestamp': datetime.now()
            }
            return fundamentals
            
        except ccxt.BaseError as e:
            raise RuntimeError(f"Failed to fetch ticker for {symbol}: {str(e)}") from e
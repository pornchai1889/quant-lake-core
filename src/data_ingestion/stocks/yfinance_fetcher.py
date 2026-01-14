"""
Yahoo Finance Data Fetcher Implementation.

This module implements the logic for retrieving Stock market data and
Fundamental data using the yfinance library. It adheres to the
BaseDataFetcher interface to ensure uniform data processing.

Dependencies:
    - yfinance: For retrieving stock data from Yahoo Finance.
    - pandas: For data manipulation.
"""

from datetime import datetime
from typing import Optional, Any, Dict

import pandas as pd
import yfinance as yf

from src.data_ingestion.base import BaseDataFetcher


class YahooFinanceFetcher(BaseDataFetcher):
    """
    Data fetcher implementation for Stock Markets using Yahoo Finance.
    Supports fetching OHLCV time-series and Fundamental data (Financial info).
    """

    def __init__(self):
        """
        Initialize the Yahoo Finance fetcher.
        Note: yfinance does not typically require an API key for basic usage.
        """
        super().__init__(source_name="YAHOO", api_key=None)

    def fetch_ohlcv(
        self,
        symbol: str,
        interval: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 1000  # yfinance handles limits differently, but we keep the arg for consistency
    ) -> pd.DataFrame:
        """
        Fetch OHLCV data from Yahoo Finance.

        Args:
            symbol (str): Stock ticker symbol (e.g., 'AAPL', 'TSLA').
            interval (str): Timeframe (e.g., '1d', '1h'). 
                            Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo.
            start_date (Optional[datetime]): Start date.
            end_date (Optional[datetime]): End date.
            limit (int): Unused in yfinance, but kept for interface compatibility.

        Returns:
            pd.DataFrame: Standardized OHLCV data.
        """
        # Convert datetime to string format YYYY-MM-DD if present
        start_str = start_date.strftime('%Y-%m-%d') if start_date else None
        end_str = end_date.strftime('%Y-%m-%d') if end_date else None

        try:
            # Fetch data using yfinance
            # auto_adjust=True ensures we get split/dividend adjusted prices (Better for analysis)
            df = yf.download(
                tickers=symbol,
                start=start_str,
                end=end_str,
                interval=interval,
                auto_adjust=True,
                progress=False
            )
        except Exception as e:
            raise RuntimeError(f"Failed to fetch stock data for {symbol}: {str(e)}") from e

        if df.empty:
            return pd.DataFrame()

        # ---------------------------------------------------------
        # Data Normalization Process
        # ---------------------------------------------------------
        
        # 1. Reset Index (Date is usually the index in yfinance)
        df.reset_index(inplace=True)

        # 2. Rename columns to match our standard schema (lowercase)
        # Yahoo columns: 'Date'/'Datetime', 'Open', 'High', 'Low', 'Close', 'Volume'
        df.rename(columns={
            'Date': 'time',
            'Datetime': 'time',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        }, inplace=True)

        # 3. Handle Timezone
        # yfinance often returns naive datetime or local timezone. We standardize to UTC.
        if 'time' in df.columns:
            if df['time'].dt.tz is None:
                # If naive, assume UTC (or market local time converted to UTC conceptually)
                df['time'] = df['time'].dt.tz_localize('UTC')
            else:
                # If aware, convert to UTC
                df['time'] = df['time'].dt.tz_convert('UTC')

        # 4. Select and Reorder required columns
        # Note: yfinance might not return 'volume' for some indices, fill with 0 if missing
        if 'volume' not in df.columns:
            df['volume'] = 0.0

        df = df[['time', 'open', 'high', 'low', 'close', 'volume']]

        # 5. Validate Schema
        self.validate_dataframe(df)

        return df

    def fetch_fundamental(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch fundamental data for a stock.
        
        Retrieves key financial metrics like PE Ratio, Market Cap, EPS, Sector.

        Args:
            symbol (str): Stock ticker symbol.

        Returns:
            Dict[str, Any]: Dictionary of fundamental metrics.
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            # Extract only the most relevant fields for our database
            fundamentals = {
                'symbol': symbol,
                'short_name': info.get('shortName'),
                'sector': info.get('sector'),
                'industry': info.get('industry'),
                'market_cap': info.get('marketCap'),
                'pe_ratio': info.get('trailingPE'),
                'eps': info.get('trailingEps'),
                'dividend_yield': info.get('dividendYield'),
                '52_week_high': info.get('fiftyTwoWeekHigh'),
                '52_week_low': info.get('fiftyTwoWeekLow'),
                'currency': info.get('currency'),
                'timestamp': datetime.now()
            }
            return fundamentals

        except Exception as e:
            raise RuntimeError(f"Failed to fetch fundamentals for {symbol}: {str(e)}") from e
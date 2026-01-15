"""
Financial Statement Parser Module.

This module provides functionality to parse financial statements from Excel files.
It is designed to handle historical fundamental data (Balance Sheet, Income Statement)
and normalize it into a structure compatible with the 'financial_statements' database table.

The parser assumes a standard format where:
- Columns represent financial periods (dates).
- Rows represent financial metrics (e.g., Revenue, Net Income).
- Or a transposed format (configurable).
"""

import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional, Union

import pandas as pd
import numpy as np

# Configure logger
logger = logging.getLogger(__name__)


class ExcelStatementParser:
    """
    Parser for extracting financial data from Excel spreadsheets.
    """

    # Default mapping from Excel row labels to Database columns.
    # Key: Database Column Name
    # Value: List of possible Excel labels (Case-insensitive)
    DEFAULT_MAPPING = {
        'revenue': ['Total Revenue', 'Revenue', 'Sales', 'Gross Income'],
        'net_income': ['Net Income', 'Net Profit', 'Profit/Loss'],
        'eps': ['EPS', 'Earnings Per Share', 'Basic EPS'],
        'total_assets': ['Total Assets', 'Assets'],
        'total_liabilities': ['Total Liabilities', 'Liabilities'],
    }

    def __init__(self, column_mapping: Optional[Dict[str, List[str]]] = None):
        """
        Initialize the parser.

        Args:
            column_mapping (Optional[Dict[str, List[str]]]): Custom mapping for metrics.
                                                              If None, uses DEFAULT_MAPPING.
        """
        self.column_mapping = column_mapping or self.DEFAULT_MAPPING

    def parse(self, file_path: str, symbol: str, sheet_name: Union[str, int] = 0) -> List[Dict[str, Any]]:
        """
        Parse an Excel file and extract financial statements.

        Assumes the Excel structure:
            - Header row contains dates (e.g., '2023-12-31').
            - First column contains metric names (e.g., 'Total Revenue').

        Args:
            file_path (str): Path to the .xlsx file.
            symbol (str): Ticker symbol associated with this file.
            sheet_name (Union[str, int]): Sheet to read (default is first sheet).

        Returns:
            List[Dict[str, Any]]: A list of financial records ready for database insertion.
        """
        try:
            # Read Excel file
            # header=0 implies the first row contains dates/headers
            df = pd.read_excel(file_path, sheet_name=sheet_name, index_col=0)
            
            logger.info(f"Loaded Excel file for {symbol}: {file_path}")

        except Exception as e:
            logger.error(f"Failed to read Excel file {file_path}: {e}")
            raise RuntimeError(f"Error reading financial statement file: {e}") from e

        # ---------------------------------------------------------
        # Data Extraction Logic
        # ---------------------------------------------------------
        parsed_data: List[Dict[str, Any]] = []

        # Iterate over columns (which are assumed to be Dates/Periods)
        for col_date in df.columns:
            
            # 1. Parse Period Date
            period_end = self._parse_date(col_date)
            if not period_end:
                logger.warning(f"Skipping column '{col_date}': Could not parse date.")
                continue

            # 2. Initialize Record
            record: Dict[str, Any] = {
                'asset_id': None,  # Will be filled by the repository layer/ingestion script
                'symbol': symbol,  # Temporary field for reference
                'time': datetime.combine(period_end, datetime.min.time()), # Report time (default to period end)
                'period_end': period_end,
                'period_type': 'Y', # Default assumption, logic can be improved to detect Q/Y
                'raw_data': {}
            }

            # 3. Extract Metrics based on Mapping
            # Iterate through the DataFrame index (Row Labels)
            col_data = df[col_date]
            
            for db_field, keywords in self.column_mapping.items():
                value = self._find_value_fuzzy(col_data, keywords)
                if value is not None:
                    record[db_field] = value
                else:
                    record[db_field] = None
            
            # 4. Validation: Only add if we have at least some data
            # Check if at least one key metric is present
            if any(record.get(k) is not None for k in self.column_mapping.keys()):
                parsed_data.append(record)
            else:
                logger.warning(f"Skipping period {period_end}: No matching metrics found.")

        logger.info(f"Successfully parsed {len(parsed_data)} records for {symbol}.")
        return parsed_data

    def _find_value_fuzzy(self, series: pd.Series, keywords: List[str]) -> Optional[float]:
        """
        Search for a value in a Series by matching index labels against keywords.
        """
        # Normalize index to lower case for comparison
        series_index_lower = series.index.astype(str).str.lower().str.strip()
        
        for keyword in keywords:
            keyword_lower = keyword.lower().strip()
            
            # Find matches
            matches = series[series_index_lower == keyword_lower]
            
            if not matches.empty:
                val = matches.iloc[0]
                return self._clean_numeric(val)
        
        return None

    def _clean_numeric(self, value: Any) -> Optional[float]:
        """
        Convert Excel values (strings, dashes, NaNs) to float.
        """
        if pd.isna(value) or value == '-' or value == '':
            return None
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_date(self, value: Any) -> Optional[date]:
        """
        Convert column header to a date object.
        Handles datetime objects, strings, etc.
        """
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        
        # Try parsing string formats
        if isinstance(value, str):
            try:
                # Common formats: '2023-12-31', '31/12/2023'
                return pd.to_datetime(value).date()
            except Exception:
                pass
        
        return None
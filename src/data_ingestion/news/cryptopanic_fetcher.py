"""
CryptoPanic News Fetcher Module.

This module implements the specific logic for retrieving cryptocurrency news
from the CryptoPanic Aggregator API. It inherits from the BaseNewsFetcher
to ensure data consistency with the rest of the ingestion pipeline.

API Reference: https://cryptopanic.com/developers/api/
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.core.config import settings
from src.data_ingestion.news.base_news import BaseNewsFetcher, NewsArticle

# Configure logger
logger = logging.getLogger(__name__)


class CryptoPanicFetcher(BaseNewsFetcher):
    """
    Data fetcher implementation for CryptoPanic API.
    
    Attributes:
        base_url (str): The CryptoPanic API endpoint.
        session (requests.Session): HTTP session with retry logic.
    """

    BASE_URL = "https://cryptopanic.com/api/v1/posts/"

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the CryptoPanic fetcher.

        Args:
            api_key (Optional[str]): The API Key. Defaults to settings.CRYPTOPANIC_API_KEY.
        
        Raises:
            ValueError: If no API key is provided in args or config.
        """
        # Load API key from Config if not provided explicitly
        _key = api_key or settings.CRYPTOPANIC_API_KEY
        
        if not _key:
            raise ValueError(
                "CryptoPanic API Key is missing. "
                "Please set CRYPTOPANIC_API_KEY in .env or src/core/config.py"
            )

        super().__init__(source_name="CRYPTOPANIC", api_key=_key)
        
        # Setup robust HTTP session with retries
        self.session = self._create_retry_session()

    def _create_retry_session(self, retries: int = 3, backoff_factor: float = 0.3) -> requests.Session:
        """
        Create a requests Session with automatic retry logic.
        This helps handle transient network glitches or API rate limits.
        """
        session = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=(500, 502, 504),
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def fetch_news(
        self,
        symbol: Optional[str] = None,
        limit: int = 50
    ) -> List[NewsArticle]:
        """
        Fetch news from CryptoPanic.

        Args:
            symbol (Optional[str]): Asset symbol (e.g., 'BTC', 'ETH').
            limit (int): Max number of articles to return.
                         Note: CryptoPanic returns 20 items per page. 
                         We handle pagination automatically to meet the limit.

        Returns:
            List[NewsArticle]: Standardized news articles.
        """
        articles: List[NewsArticle] = []
        page = 1
        current_url = self.BASE_URL

        # Prepare query parameters
        params: Dict[str, Union[str, int]] = {
            "auth_token": self.api_key,
            "public": "true",  # Public API mode
            # 'kind': 'news',  # Optional: strictly news (exclude media) if needed
        }

        if symbol:
            # CryptoPanic uses 'currencies' filter (e.g., BTC,ETH)
            # We strip '/USDT' if passed as pair (e.g., 'BTC/USDT' -> 'BTC')
            clean_symbol = symbol.split("/")[0].upper()
            params["currencies"] = clean_symbol

        logger.info(f"Fetching news from CryptoPanic for {symbol or 'ALL'} (Limit: {limit})...")

        try:
            while len(articles) < limit:
                # Make the request
                # Note: 'params' are only needed for the first page request constructed manually.
                # Subsequent 'next' URLs from API already contain params.
                if page == 1:
                    response = self.session.get(current_url, params=params, timeout=10)
                else:
                    response = self.session.get(current_url, timeout=10)

                response.raise_for_status()
                data = response.json()

                # Parse results
                results = data.get("results", [])
                if not results:
                    break  # No more data

                for item in results:
                    article = self._parse_item(item)
                    if article and self.validate_article(article):
                        articles.append(article)
                    
                    if len(articles) >= limit:
                        break

                # Pagination Logic
                next_url = data.get("next")
                if next_url and len(articles) < limit:
                    current_url = next_url
                    page += 1
                else:
                    break

            logger.info(f"Successfully retrieved {len(articles)} articles from CryptoPanic.")
            return articles

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching CryptoPanic news: {e}")
            # In production, we might want to re-raise or return partial results
            # Here we return what we have so far
            return articles
        except Exception as e:
            logger.exception(f"Unexpected error in CryptoPanic fetcher: {e}")
            return articles

    def _parse_item(self, item: Dict[str, Any]) -> Optional[NewsArticle]:
        """
        Convert raw CryptoPanic JSON item to NewsArticle DTO.
        
        Args:
            item (Dict): Raw JSON dictionary from API.

        Returns:
            Optional[NewsArticle]: The standardized object, or None if parsing fails.
        """
        try:
            # Parse Date (ISO 8601 format: 2024-01-15T12:00:00Z)
            published_at_str = item.get("published_at")
            if published_at_str:
                # Replace 'Z' with '+00:00' for standard ISO parsing compatibility in older Pythons
                published_at = datetime.fromisoformat(published_at_str.replace("Z", "+00:00"))
            else:
                published_at = datetime.utcnow()

            # Extract Source Name
            # item['source'] is usually a dict: {'title': 'CoinTelegraph', ...}
            source_info = item.get("source", {})
            source_name = source_info.get("title", "Unknown") if isinstance(source_info, dict) else "CryptoPanic"

            return NewsArticle(
                title=item.get("title", ""),
                url=item.get("url", ""),  # Note: This is usually a cryptopanic shortlink
                source=f"CryptoPanic-{source_name}",
                published_at=published_at,
                summary=None, # CryptoPanic public API rarely gives full summary
                raw_data=item # Keep raw data for auditing
            )

        except Exception as e:
            logger.warning(f"Failed to parse CryptoPanic item ID {item.get('id')}: {e}")
            return None
"""
Base News Ingestion Module.

This module defines the abstract base class for all news fetchers.
It enforces a consistent data structure (DTO) for unstructured news content,
ensuring that downstream consumers (AI Engine, Database) receive uniform data
regardless of the source (CryptoPanic, NewsAPI, Twitter, etc.).
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, asdict


@dataclass
class NewsArticle:
    """
    Data Transfer Object (DTO) representing a single news article.
    
    Attributes:
        title (str): The headline of the article.
        url (str): Link to the original source.
        source (str): Name of the provider (e.g., 'CryptoPanic').
        published_at (datetime): UTC timestamp of publication.
        summary (Optional[str]): Short description or content snippet.
        raw_data (Optional[Dict]): The original JSON payload (for debugging).
    """
    title: str
    url: str
    source: str
    published_at: datetime
    summary: Optional[str] = None
    raw_data: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert the DTO to a dictionary for easy database insertion."""
        return asdict(self)


class BaseNewsFetcher(ABC):
    """
    Abstract Base Class for News Fetchers.
    
    All specific news implementations MUST inherit from this class
    and implement the `fetch_news` method.
    """

    def __init__(self, source_name: str, api_key: Optional[str] = None):
        """
        Initialize the fetcher.

        Args:
            source_name (str): A unique identifier for this source (e.g., 'CRYPTOPANIC').
            api_key (Optional[str]): API credentials (if required).
        """
        self.source_name = source_name.upper()
        self.api_key = api_key

    @abstractmethod
    def fetch_news(
        self,
        symbol: Optional[str] = None,
        limit: int = 50
    ) -> List[NewsArticle]:
        """
        Retrieve news articles from the external source.

        Args:
            symbol (Optional[str]): The asset symbol to filter by (e.g., 'BTC').
                                    If None, fetch general market news.
            limit (int): The maximum number of articles to return.

        Returns:
            List[NewsArticle]: A list of standardized NewsArticle objects.
        
        Raises:
            Exception: If the API request fails or connection errors occur.
        """
        pass

    def validate_article(self, article: NewsArticle) -> bool:
        """
        Helper method to filter out invalid or low-quality articles.
        
        Args:
            article (NewsArticle): The article to check.

        Returns:
            bool: True if the article is valid, False otherwise.
        """
        # Rule 1: Must have a non-empty title
        if not article.title or not article.title.strip():
            return False
        
        # Rule 2: Must have a valid timestamp
        if not isinstance(article.published_at, datetime):
            return False

        return True
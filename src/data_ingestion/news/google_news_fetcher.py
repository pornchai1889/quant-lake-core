"""
Google News Fetcher Module.

This module provides an interface to fetch news articles from Google News via RSS feeds.
It utilizes the 'gnews' library to retrieve and parse news items, ensuring compliance
with the BaseNewsFetcher interface and returning standardized NewsArticle DTOs.

Dependencies:
    - gnews: For interacting with Google News RSS.
    - python-dateutil: For robust datetime parsing.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

# Third-party imports
from gnews import GNews
from dateutil import parser as date_parser

# Internal imports
from src.core.config import settings
from src.data_ingestion.news.base_news import BaseNewsFetcher, NewsArticle

# Configure logger
logger = logging.getLogger(__name__)


class GoogleNewsFetcher(BaseNewsFetcher):
    """
    Fetcher implementation for Google News.
    
    Attributes:
        client (GNews): The GNews client instance configured with settings.
    """

    def __init__(self):
        """
        Initialize the Google News fetcher with configuration settings.
        Note: Google News (via RSS) typically does not require an API key.
        """
        # Call parent constructor (source_name='GOOGLE_NEWS')
        super().__init__(source_name="GOOGLE_NEWS", api_key=None)

        # Initialize GNews client
        self.client = GNews(
            language=settings.GOOGLE_NEWS_LANG,
            country=settings.GOOGLE_NEWS_COUNTRY,
            period=settings.GOOGLE_NEWS_PERIOD,
            max_results=settings.GOOGLE_NEWS_MAX_RESULTS,
            exclude_websites=None # Can be configured to exclude specific domains
        )
        logger.info(f"Initialized GoogleNewsFetcher ({settings.GOOGLE_NEWS_LANG}-{settings.GOOGLE_NEWS_COUNTRY})")

    def fetch_news(
        self,
        symbol: Optional[str] = None,
        limit: int = 50
    ) -> List[NewsArticle]:
        """
        Fetch news articles from Google News based on a query or symbol.

        Args:
            symbol (Optional[str]): The keyword/symbol to search for (e.g., 'Bitcoin').
                                    If None, fetches top headlines ('Business' topic).
            limit (int): Maximum number of articles to return.

        Returns:
            List[NewsArticle]: A list of standardized news objects.
        """
        articles: List[NewsArticle] = []
        
        try:
            # Update limit in client (GNews uses max_results per call)
            self.client.max_results = limit
            
            raw_news: List[Dict[str, Any]] = []

            if symbol:
                # Search for specific topic/coin
                logger.info(f"Searching Google News for: '{symbol}'")
                raw_news = self.client.get_news(symbol)
            else:
                # Get Top News (Business/Finance context)
                logger.info("Fetching Top Business Headlines from Google News...")
                raw_news = self.client.get_news_by_topic('BUSINESS')

            # Process and normalize data
            for item in raw_news:
                article = self._parse_item(item)
                if article and self.validate_article(article):
                    articles.append(article)

            logger.info(f"Successfully retrieved {len(articles)} articles from Google News.")
            return articles

        except Exception as e:
            logger.exception(f"Failed to fetch data from Google News: {e}")
            return []

    def _parse_item(self, item: Dict[str, Any]) -> Optional[NewsArticle]:
        """
        Convert raw GNews dictionary to NewsArticle DTO.

        Args:
            item (Dict): Raw news item from GNews (keys: title, published date, etc.)

        Returns:
            Optional[NewsArticle]: Standardized DTO or None if parsing fails.
        """
        try:
            # Parse Datetime
            # GNews returns date strings like: "Fri, 15 Jan 2024 12:00:00 GMT"
            raw_date = item.get("published date", "")
            if raw_date:
                try:
                    published_at = date_parser.parse(raw_date)
                    # Ensure timezone awareness (UTC)
                    if published_at.tzinfo is None:
                        published_at = published_at.replace(tzinfo=None) # naive approach or set UTC
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse date '{raw_date}'. Using current time.")
                    published_at = datetime.utcnow()
            else:
                published_at = datetime.utcnow()

            # Extract Source
            # 'publisher' is usually a dict like {'href': '...', 'title': 'Bloomberg'}
            publisher_data = item.get("publisher", {})
            source_name = "GoogleNews"
            if isinstance(publisher_data, dict):
                source_name = publisher_data.get("title", "GoogleNews")
            elif isinstance(publisher_data, str):
                source_name = publisher_data

            return NewsArticle(
                title=item.get("title", ""),
                url=item.get("url", ""),
                source=f"GoogleNews-{source_name}",
                published_at=published_at,
                summary=item.get("description", ""), # GNews provides a short description snippet
                raw_data=item
            )

        except Exception as e:
            logger.warning(f"Error parsing Google News item: {e}")
            return None
"""
Sentiment Analysis Engine.

This module implements the core business logic for converting unstructured text
into quantitative sentiment metrics using an LLM.

It orchestrates the interaction between the Prompt Templates and the LLM Client,
handling data validation and parsing to ensure downstream reliability.
"""

import logging
import json
from typing import Dict, Optional, Any
from dataclasses import dataclass

from src.ai_analysis.llm_client import OllamaClient
from src.ai_analysis.prompt_templates import (
    SENTIMENT_ANALYSIS_SYSTEM_PROMPT,
    SENTIMENT_USER_PROMPT_TEMPLATE,
)

logger = logging.getLogger(__name__)


@dataclass
class SentimentResult:
    """
    Data Transfer Object (DTO) for sentiment analysis results.
    Ensures strict typing for the return values.
    """
    sentiment_score: float
    impact_score: float
    confidence: float
    raw_response: Optional[str] = None


class SentimentAnalyzer:
    """
    Analyzer class responsible for processing news text via LLM.
    """

    def __init__(self, client: Optional[OllamaClient] = None):
        """
        Initialize the analyzer.

        Args:
            client (Optional[OllamaClient]): The LLM client instance. 
                                             If None, creates a default instance.
                                             Injectable for testing.
        """
        self.client = client or OllamaClient()

    def analyze(self, text: str) -> Optional[SentimentResult]:
        """
        Analyze the sentiment of a given text.

        Args:
            text (str): The news headline or content to analyze.

        Returns:
            Optional[SentimentResult]: Structured sentiment metrics or None if failed.
        """
        if not text or not text.strip():
            logger.warning("Empty text provided for sentiment analysis.")
            return None

        # Construct the prompt
        prompt = SENTIMENT_USER_PROMPT_TEMPLATE.format(text=text.strip())

        try:
            # Call LLM
            # We request JSON format to ensure parsability
            response_data = self.client.generate(
                prompt=prompt,
                system_prompt=SENTIMENT_ANALYSIS_SYSTEM_PROMPT,
                format="json"
            )

            if not response_data:
                logger.error("Received empty response from LLM.")
                return None

            return self._parse_response(response_data)

        except Exception as e:
            logger.exception(f"Sentiment analysis failed for text: '{text[:30]}...': {e}")
            return None

    def _parse_response(self, data: Dict[str, Any]) -> Optional[SentimentResult]:
        """
        Validate and parse the raw JSON response from the LLM.

        Args:
            data (Dict[str, Any]): The raw JSON dictionary from LLM.

        Returns:
            Optional[SentimentResult]: Validated result object.
        """
        try:
            # Extract fields with default fallbacks for safety
            # Note: The LLM might return keys in slightly different case/naming if not strictly followed,
            # but usually 'format="json"' with a good system prompt is reliable.
            sentiment = float(data.get("sentiment_score", 0.0))
            impact = float(data.get("impact_score", 0.0))
            confidence = float(data.get("confidence", 0.0))

            # Basic Validation: Ensure scores are within expected bounds
            # This aligns with the database CHECK constraints.
            sentiment = max(-1.0, min(1.0, sentiment))
            impact = max(0.0, min(1.0, impact))
            confidence = max(0.0, min(1.0, confidence))

            return SentimentResult(
                sentiment_score=sentiment,
                impact_score=impact,
                confidence=confidence
            )

        except (ValueError, TypeError) as e:
            logger.error(f"Failed to parse LLM response values: {data} - Error: {e}")
            return None
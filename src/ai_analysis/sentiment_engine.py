"""
Sentiment Analysis Engine.

This module implements the core business logic for converting unstructured text
into quantitative sentiment metrics using an LLM.

It orchestrates the interaction between the Prompt Templates and the LLM Client.
"""

import logging
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
                                             Injectable for testing.
        """
        # If client is provided (e.g. Mock for tests), use it.
        # Otherwise, create a real OllamaClient.
        self.client = client or OllamaClient()

        # [AUTO-HEALING LOGIC]
        # Only perform the model check if we are using the real client (not a mock).
        # This prevents unit tests from trying to connect to Docker.
        if client is None:
            self.client.ensure_model_exists()

    def analyze(self, text: str) -> Optional[SentimentResult]:
        """
        Analyze the sentiment of a given text.
        """
        if not text or not text.strip():
            logger.warning("Empty text provided for sentiment analysis.")
            return None

        prompt = SENTIMENT_USER_PROMPT_TEMPLATE.format(text=text.strip())

        try:
            response_data = self.client.generate(
                prompt=prompt,
                system_prompt=SENTIMENT_ANALYSIS_SYSTEM_PROMPT,
                format="json"
            )

            if not response_data:
                return None

            return self._parse_response(response_data)

        except Exception as e:
            logger.exception(f"Sentiment analysis failed: {e}")
            return None

    def _parse_response(self, data: Dict[str, Any]) -> Optional[SentimentResult]:
        """
        Validate and parse the raw JSON response from the LLM.
        """
        try:
            sentiment = float(data.get("sentiment_score", 0.0))
            impact = float(data.get("impact_score", 0.0))
            confidence = float(data.get("confidence", 0.0))

            # Clamp values to valid ranges
            sentiment = max(-1.0, min(1.0, sentiment))
            impact = max(0.0, min(1.0, impact))
            confidence = max(0.0, min(1.0, confidence))

            return SentimentResult(
                sentiment_score=sentiment,
                impact_score=impact,
                confidence=confidence
            )

        except (ValueError, TypeError):
            logger.error(f"Failed to parse LLM response values: {data}")
            return None
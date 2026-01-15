"""
Unit Tests for Sentiment Engine.

Tests the logic of the SentimentAnalyzer, ensuring it correctly handles
LLM responses, parsing errors, and edge cases.
Uses mocking to avoid actual API calls during testing.
"""

import unittest
from unittest.mock import MagicMock, patch

from src.ai_analysis.sentiment_engine import SentimentAnalyzer, SentimentResult
from src.ai_analysis.llm_client import OllamaClient


class TestSentimentAnalyzer(unittest.TestCase):
    
    def setUp(self):
        """Set up the test environment before each test."""
        # Create a mock client to inject into the analyzer
        self.mock_client = MagicMock(spec=OllamaClient)
        self.analyzer = SentimentAnalyzer(client=self.mock_client)

    def test_analyze_success(self):
        """Test a successful analysis with valid JSON response."""
        # Arrange: Setup the mock to return a specific JSON
        self.mock_client.generate.return_value = {
            "sentiment_score": 0.85,
            "impact_score": 0.9,
            "confidence": 0.95
        }

        # Act: Call the method
        result = self.analyzer.analyze("Bitcoin hits all-time high!")

        # Assert: Verify the results
        self.assertIsNotNone(result)
        self.assertIsInstance(result, SentimentResult)
        self.assertEqual(result.sentiment_score, 0.85)
        self.assertEqual(result.impact_score, 0.9)
        self.assertEqual(result.confidence, 0.95)

    def test_analyze_empty_text(self):
        """Test handling of empty input text."""
        result = self.analyzer.analyze("")
        self.assertIsNone(result)
        # Ensure LLM was NOT called
        self.mock_client.generate.assert_not_called()

    def test_analyze_llm_failure(self):
        """Test handling when LLM returns None (e.g., timeout)."""
        self.mock_client.generate.return_value = None
        
        result = self.analyzer.analyze("Some news")
        self.assertIsNone(result)

    def test_analyze_malformed_json_values(self):
        """Test handling of JSON with invalid data types."""
        # Arrange: LLM returns string instead of float
        self.mock_client.generate.return_value = {
            "sentiment_score": "not_a_number", 
            "impact_score": 0.5,
            "confidence": 0.5
        }

        result = self.analyzer.analyze("Some news")
        # Should gracefully return None or default, depending on implementation.
        # In our code, float conversion raises ValueError -> returns None
        self.assertIsNone(result)

    def test_boundary_clamping(self):
        """Test that scores out of range are clamped to valid bounds."""
        # Arrange: LLM hallucinates a score of 999.0
        self.mock_client.generate.return_value = {
            "sentiment_score": 999.0,  # Should become 1.0
            "impact_score": -5.0,      # Should become 0.0
            "confidence": 2.0          # Should become 1.0
        }

        result = self.analyzer.analyze("Hype news")
        
        self.assertEqual(result.sentiment_score, 1.0)
        self.assertEqual(result.impact_score, 0.0)
        self.assertEqual(result.confidence, 1.0)

if __name__ == "__main__":
    unittest.main()
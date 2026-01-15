"""
Ollama API Client Module.

This module provides a robust client for interacting with the Ollama Inference Server.
It abstracts the HTTP communication logic, error handling, and response parsing,
allowing the rest of the application to treat LLM calls as simple function invocations.

Standard:
- Uses 'requests' for synchronous HTTP calls (compatible with ETL scripts).
- Implements strict type hinting and logging.
"""

import logging
import json
from typing import Optional, Dict, Any

import requests
from requests.exceptions import RequestException, Timeout

from src.core.config import settings

# Configure logger for this module
logger = logging.getLogger(__name__)


class OllamaClient:
    """
    Client for communicating with the Ollama API (running in Docker or Local).
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Initialize the Ollama Client.

        Args:
            base_url (Optional[str]): The API endpoint (e.g., 'http://localhost:11434').
                                      Defaults to settings.OLLAMA_BASE_URL.
            model (Optional[str]): The default model name to use.
                                   Defaults to settings.OLLAMA_MODEL.
            timeout (Optional[float]): Request timeout in seconds.
        """
        self.base_url = (base_url or settings.OLLAMA_BASE_URL).rstrip("/")
        self.model = model or settings.OLLAMA_MODEL
        self.timeout = timeout or settings.OLLAMA_TIMEOUT
        
        self.generate_endpoint = f"{self.base_url}/api/generate"

    def check_health(self) -> bool:
        """
        Check if the Ollama service is reachable.

        Returns:
            bool: True if service is up (HTTP 200), False otherwise.
        """
        try:
            # Ollama root endpoint usually returns a simple status message
            response = requests.get(self.base_url, timeout=5.0)
            return response.status_code == 200
        except RequestException as e:
            logger.warning(f"Ollama health check failed: {e}")
            return False

    def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        format: str = "json",
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Send a prompt to the LLM and retrieve the generated response.

        Args:
            prompt (str): The user input or news text to analyze.
            system_prompt (Optional[str]): Context/Persona instructions for the model.
            format (str): Desired output format. Defaults to 'json' (critical for Quant work).
            options (Optional[Dict]): Additional model parameters (temperature, seed, etc.).

        Returns:
            Optional[Dict[str, Any]]: The parsed JSON response from the model,
                                      or None if the request failed.
        """
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,  # Disable streaming to get a single full JSON response
            "format": format,
        }

        if system_prompt:
            payload["system"] = system_prompt

        if options:
            payload["options"] = options

        try:
            logger.debug(f"Sending request to Ollama ({self.model})...")
            
            response = requests.post(
                self.generate_endpoint,
                json=payload,
                timeout=self.timeout
            )
            
            # Raise an error for 4xx or 5xx status codes
            response.raise_for_status()

            # Parse the response body
            result = response.json()
            
            # Ollama returns the actual generated text in the 'response' field
            # Since we requested JSON format, we try to parse that inner string as JSON
            raw_response_text = result.get("response", "")
            
            if format == "json":
                try:
                    return json.loads(raw_response_text)
                except json.JSONDecodeError:
                    logger.error("Failed to parse LLM output as JSON. Raw output: %s", raw_response_text)
                    return None
            
            # If not JSON format, return wrapped in a dict
            return {"text": raw_response_text}

        except Timeout:
            logger.error(f"Ollama request timed out after {self.timeout}s.")
            return None
        except RequestException as e:
            logger.error(f"Ollama API request failed: {e}")
            return None
        except Exception as e:
            logger.exception(f"Unexpected error during LLM inference: {e}")
            return None
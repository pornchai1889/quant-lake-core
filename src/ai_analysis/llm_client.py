"""
Ollama API Client Module.

This module provides a robust client for interacting with the Ollama Inference Server.
It abstracts the HTTP communication logic, error handling, and response parsing.

Features:
- Auto-pull models if missing (Self-healing).
- Strict type hinting and error logging.
"""

import logging
import json
import time
from typing import Optional, Dict, Any, List

import requests
from requests.exceptions import RequestException, Timeout

from src.core.config import settings

# Configure logger
logger = logging.getLogger(__name__)


class OllamaClient:
    """
    Client for communicating with the Ollama API.
    Handles inference requests and model management.
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
            base_url (Optional[str]): The API endpoint. Defaults to settings.
            model (Optional[str]): The model name to use. Defaults to settings.
            timeout (Optional[float]): Inference timeout in seconds.
        """
        self.base_url = (base_url or settings.OLLAMA_BASE_URL).rstrip("/")
        self.model = model or settings.OLLAMA_MODEL
        self.timeout = timeout or settings.OLLAMA_TIMEOUT
        
        self.generate_endpoint = f"{self.base_url}/api/generate"
        self.tags_endpoint = f"{self.base_url}/api/tags"
        self.pull_endpoint = f"{self.base_url}/api/pull"

    def ensure_model_exists(self) -> None:
        """
        Check if the configured model exists locally. If not, trigger an automatic pull.
        
        This mechanism ensures 'Zero Config' deployment: users don't need to manually
        pull models via CLI. The system heals itself on startup.
        """
        if self._check_model_availability():
            return

        logger.info(f"Model '{self.model}' not found in Ollama. Starting automatic download...")
        self._pull_model()

    def _check_model_availability(self) -> bool:
        """
        Query Ollama to see if the model is already downloaded.
        
        Returns:
            bool: True if model exists, False otherwise.
        """
        try:
            response = requests.get(self.tags_endpoint, timeout=10.0)
            if response.status_code == 200:
                data = response.json()
                models: List[Dict[str, Any]] = data.get("models", [])
                
                # Check if our model name appears in the list
                # Note: Ollama might return 'qwen2.5:3b' or 'qwen2.5:3b-instruct'
                # We do a substring match to be safe.
                exists = any(self.model in m.get("name", "") for m in models)
                
                if exists:
                    logger.info(f"Model '{self.model}' is ready.")
                    return True
            
            return False

        except RequestException as e:
            logger.warning(f"Could not check model availability (Ollama might be down): {e}")
            return False

    def _pull_model(self) -> None:
        """
        Trigger the model download. This blocks until completion or timeout.
        """
        try:
            logger.info(f"Pulling model '{self.model}'. This may take a few minutes...")
            
            # We set stream=False to wait for the full download.
            # Timeout is set to 30 minutes (1800s) to accommodate large models/slow networks.
            payload = {"name": self.model, "stream": False}
            
            response = requests.post(
                self.pull_endpoint, 
                json=payload, 
                timeout=1800.0  # Hardcoded long timeout for downloading
            )
            
            response.raise_for_status()
            logger.info(f"Successfully downloaded model '{self.model}'.")

        except Exception as e:
            logger.error(
                f"Failed to auto-pull model '{self.model}': {e}. "
                "Please check your internet connection or pull manually."
            )
            # We don't raise here to allow the app to try running anyway (it might fail later)

    def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        format: str = "json",
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Send a prompt to the LLM and retrieve the generated response.
        """
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "format": format,
        }

        if system_prompt:
            payload["system"] = system_prompt

        if options:
            payload["options"] = options

        try:
            logger.debug(f"Sending inference request to Ollama ({self.model})...")
            
            response = requests.post(
                self.generate_endpoint,
                json=payload,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            result = response.json()
            raw_response_text = result.get("response", "")
            
            if format == "json":
                try:
                    return json.loads(raw_response_text)
                except json.JSONDecodeError:
                    logger.error("Failed to parse LLM output as JSON.")
                    return None
            
            return {"text": raw_response_text}

        except Exception as e:
            logger.error(f"Ollama inference failed: {e}")
            return None
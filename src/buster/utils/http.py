import logging
import os
from typing import Any, Dict, Optional, cast

import requests


def send_request(
    url: str,
    payload: dict,
    api_key: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> dict:
    """
    Sends a POST request to the specified URL with the given payload and API key.
    If api_key is not provided, it attempts to load it from the BUSTER_API_KEY
    environment variable.
    """
    if not api_key:
        api_key = os.environ.get("BUSTER_API_KEY")

    if not api_key:
        if logger:
            logger.error("API key not provided for HTTP request")
        raise ValueError("Buster API key must be provided via argument or 'BUSTER_API_KEY' environment variable.")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    if logger:
        logger.debug(f"Sending POST request to {url}")
        logger.debug(f"Payload keys: {list(payload.keys())}")

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()

        if logger:
            logger.debug(f"Response status: {response.status_code}")

        return cast(Dict[Any, Any], response.json())

    except requests.exceptions.HTTPError as e:
        if logger:
            logger.error(f"HTTP error occurred: {e}")
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response body: {e.response.text}")
        raise
    except requests.exceptions.RequestException as e:
        if logger:
            logger.error(f"Request failed: {e}")
        raise

import logging
import os
from typing import IO, Any, Dict, Optional, Tuple, Union, cast

import requests


def send_request(
    url: str,
    payload: dict,
    api_key: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
    files: Optional[Dict[str, Union[IO[bytes], Tuple[str, IO[bytes]], Tuple[str, IO[bytes], str]]]] = None,
) -> dict:
    """
    Sends a POST request to the specified URL with the given payload and API key.
    If api_key is not provided, it attempts to load it from the BUSTER_API_KEY
    environment variable.

    Args:
        url: The URL to send the request to
        payload: The data payload to send
        api_key: Optional API key for authorization
        logger: Optional logger for debugging
        files: Optional dict of files to upload. Supports:
            - {'name': file_object}
            - {'name': ('filename', file_object)}
            - {'name': ('filename', file_object, 'content_type')}
    """
    if not api_key:
        api_key = os.environ.get("BUSTER_API_KEY")

    if not api_key:
        if logger:
            logger.error("API key not provided for HTTP request")
        raise ValueError("Buster API key must be provided via argument or 'BUSTER_API_KEY' environment variable.")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

    # Only set Content-Type for JSON when not uploading files
    # (multipart/form-data is set automatically by requests when files are present)
    if not files:
        headers["Content-Type"] = "application/json"

    if logger:
        logger.debug(f"Sending POST request to {url}")
        logger.debug(f"Payload keys: {list(payload.keys())}")
        if files:
            logger.debug(f"Files to upload: {list(files.keys())}")

    try:
        if files:
            # Use data parameter for payload when uploading files
            response = requests.post(url, data=payload, files=files, headers=headers, timeout=30)
        else:
            # Use json parameter for regular JSON requests
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

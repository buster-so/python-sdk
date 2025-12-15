"""
Configuration validation utilities for the Buster SDK.

This module contains validation logic for API keys and configuration objects.
"""

import logging
import os
from typing import Optional

from .types import AirflowReportConfig


def validate_and_get_api_key(
    api_key: Optional[str],
    logger: logging.Logger,
) -> str:
    """
    Validate and retrieve the Buster API key from parameter or environment.

    Args:
        api_key: Optional API key passed as parameter
        logger: Logger instance for debug output

    Returns:
        The validated API key

    Raises:
        ValueError: If API key is not found in parameter or environment variable
    """
    # 1. Try param
    if api_key:
        logger.debug("API key loaded from parameter")
        return api_key

    # 2. Try env var
    env_api_key = os.environ.get("BUSTER_API_KEY")
    if env_api_key:
        logger.debug("API key loaded from environment variable")
        return env_api_key

    # 3. Fail if missing
    logger.error("API key not found in parameter or environment variable")
    raise ValueError("Buster API key must be provided via 'buster_api_key' param or 'BUSTER_API_KEY' environment variable.")


def validate_airflow_config(
    config: AirflowReportConfig,
    logger: logging.Logger,
) -> None:
    """
    Validate Airflow configuration.

    Args:
        config: The Airflow configuration to validate
        logger: Logger instance for debug output

    Raises:
        ValueError: If required fields are missing or invalid
    """
    # Currently no validation needed - all fields are optional
    # This function is kept for future validation needs
    pass

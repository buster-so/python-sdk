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
    Validate Airflow configuration to ensure required fields are present
    for the specified deployment type.

    Args:
        config: The Airflow configuration to validate
        logger: Logger instance for debug output

    Raises:
        ValueError: If required fields are missing for the deployment type
    """
    deployment_type = config.get("deployment_type")

    # If deployment_type is not specified, no validation needed
    if not deployment_type:
        return

    # Validate non-local deployments (astronomer, etc.)
    if deployment_type != "local":
        # Check for required base URL
        if not config.get("airflow_base_url"):
            raise ValueError(
                f"airflow_config with deployment_type='{deployment_type}' requires 'airflow_base_url' to be set. "
                f"Example: airflow_base_url='http://localhost:8080'"
            )

        # Check for authentication credentials
        has_token = bool(config.get("api_token"))
        has_basic_auth = bool(config.get("api_username") and config.get("api_password"))

        if not has_token and not has_basic_auth:
            raise ValueError(
                f"airflow_config with deployment_type='{deployment_type}' requires authentication credentials. "
                f"Provide either:\n"
                f"  - 'api_token' for bearer token authentication, or\n"
                f"  - Both 'api_username' and 'api_password' for basic authentication"
            )

        logger.debug(
            f"Airflow config validation passed for deployment_type='{deployment_type}' "
            f"(auth: {'token' if has_token else 'basic'})"
        )

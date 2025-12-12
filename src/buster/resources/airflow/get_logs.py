"""
Airflow log retrieval utilities.

This module provides functions to fetch task logs from Airflow, supporting both
local filesystem access and REST API access for different deployment types.
"""

import codecs
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import requests

from buster.resources.airflow.types import AirflowDeploymentType


def get_all_task_logs(
    dag_id: str,
    task_id: str,
    dag_run_id: Optional[str] = None,
    execution_date: Optional[Union[str, datetime]] = None,
    task_try_number: int = 1,
    deployment_type: AirflowDeploymentType = "astronomer",
    airflow_base_url: Optional[str] = None,
    logs_directory: Optional[str] = None,
    output_file: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    api_token: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> str:
    """
    Fetch ALL logs for a specific Airflow task instance and save to a file.

    This function supports two deployment types:
    1. "local": Reads logs directly from the filesystem (faster, no auth needed)
    2. "astronomer": Uses REST API to fetch logs (requires authentication)

    Args:
        dag_id: The DAG ID
        task_id: The task ID
        dag_run_id: The DAG run ID (required for API access, optional for local if execution_date provided)
        execution_date: The execution date (can be datetime or ISO string, used for local filesystem path)
        task_try_number: The task try number (default: 1). This is the attempt number for retries.
        deployment_type: How Airflow is deployed - "local" or "astronomer" (default: "astronomer")
        airflow_base_url: Base URL of Airflow (required for non-local, e.g., "http://localhost:8080")
        logs_directory: Path to Airflow logs directory (optional for local, defaults to $AIRFLOW_HOME/logs)
        output_file: Optional path to save logs. If None, saves to "{task_id}.log"
        username: Airflow username for basic auth (optional if using api_token)
        password: Airflow password for basic auth (optional if using api_token)
        api_token: Airflow API token for bearer auth (optional if using username/password)
        logger: Optional logger for debug output

    Returns:
        The complete log content as a string

    Raises:
        ValueError: If required parameters are missing for the deployment type
        FileNotFoundError: If log file not found (local deployment)
        requests.exceptions.HTTPError: If the API request fails (non-local deployment)

    Examples:
        >>> # Local filesystem access
        >>> logs = get_all_task_logs(
        ...     dag_id="my_dag",
        ...     task_id="my_task",
        ...     execution_date="2024-01-01T00:00:00+00:00",
        ...     deployment_type="local"
        ... )

        >>> # Astronomer/API access with basic auth
        >>> logs = get_all_task_logs(
        ...     dag_id="my_dag",
        ...     dag_run_id="manual__2024-01-01T00:00:00+00:00",
        ...     task_id="my_task",
        ...     deployment_type="astronomer",
        ...     airflow_base_url="http://localhost:8080",
        ...     username="admin",
        ...     password="admin"
        ... )

        >>> # Astronomer/API access with token
        >>> logs = get_all_task_logs(
        ...     dag_id="my_dag",
        ...     dag_run_id="manual__2024-01-01T00:00:00+00:00",
        ...     task_id="my_task",
        ...     deployment_type="astronomer",
        ...     airflow_base_url="http://localhost:8080",
        ...     api_token="your_token_here"
        ... )

    References:
        - Airflow Logging: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/logging-tasks.html
        - Airflow REST API: https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html
        - GitHub Discussion: https://github.com/apache/airflow/discussions/22415
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    if deployment_type == "local":
        return _get_logs_from_filesystem(
            dag_id=dag_id,
            task_id=task_id,
            execution_date=execution_date,
            dag_run_id=dag_run_id,
            task_try_number=task_try_number,
            logs_directory=logs_directory,
            output_file=output_file,
            logger=logger,
        )
    else:
        # For astronomer and other non-local deployments, use REST API
        return _get_logs_from_api(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            task_try_number=task_try_number,
            airflow_base_url=airflow_base_url,
            username=username,
            password=password,
            api_token=api_token,
            output_file=output_file,
            logger=logger,
        )


def _get_logs_from_filesystem(
    dag_id: str,
    task_id: str,
    execution_date: Optional[Union[str, datetime]],
    dag_run_id: Optional[str],
    task_try_number: int,
    logs_directory: Optional[str],
    output_file: Optional[str],
    logger: logging.Logger,
) -> str:
    """
    Fetch logs directly from the local filesystem.

    Supports both standard and custom Airflow log structures:
    1. Standard: {base_log_folder}/{dag_id}/{task_id}/{execution_date}/{try_number}.log
    2. Custom: {base_log_folder}/dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={try_number}.log
    """
    # Determine logs directory
    if logs_directory is None:
        # Try to get from AIRFLOW_HOME environment variable
        airflow_home = os.environ.get("AIRFLOW_HOME")
        if airflow_home:
            logs_directory = os.path.join(airflow_home, "logs")
        else:
            # Default to ~/airflow/logs
            logs_directory = os.path.expanduser("~/airflow/logs")

        logger.debug(f"Using default logs directory: {logs_directory}")

    # Parse execution date to get the log file path
    if execution_date is None and dag_run_id is None:
        raise ValueError("For local deployment, either execution_date or dag_run_id must be provided")

    # Extract execution date from dag_run_id if not provided directly
    if execution_date is None and dag_run_id:
        # dag_run_id often follows pattern: manual__2024-01-01T00:00:00+00:00
        # or scheduled__2024-01-01T00:00:00+00:00
        parts = dag_run_id.split("__")
        if len(parts) >= 2:
            execution_date = parts[1]
        else:
            raise ValueError(
                f"Could not extract execution_date from dag_run_id: {dag_run_id}. Please provide execution_date explicitly."
            )

    # Convert datetime to string if needed
    if isinstance(execution_date, datetime):
        # Format as ISO string that matches Airflow's log directory naming
        execution_date_str: str = execution_date.isoformat()
    elif isinstance(execution_date, str):
        execution_date_str = execution_date
    else:
        # This shouldn't happen due to earlier validation, but for type safety
        raise ValueError("execution_date must be a string or datetime object")

    # Try custom format first (dag_id=/run_id=/task_id=/attempt=)
    # This is common in Docker Airflow setups
    if dag_run_id:
        custom_log_path = (
            Path(logs_directory)
            / f"dag_id={dag_id}"
            / f"run_id={dag_run_id}"
            / f"task_id={task_id}"
            / f"attempt={task_try_number}.log"
        )

        if custom_log_path.exists():
            logger.info(f"Reading logs from filesystem (custom format): {custom_log_path}")
            return _read_and_save_log(custom_log_path, task_id, output_file, logger)

        logger.debug(f"Custom format log not found at: {custom_log_path}")

    # Try standard Airflow format (dag_id/task_id/execution_date/try_number.log)
    standard_log_path = Path(logs_directory) / dag_id / task_id / execution_date_str / f"{task_try_number}.log"

    if standard_log_path.exists():
        logger.info(f"Reading logs from filesystem (standard format): {standard_log_path}")
        return _read_and_save_log(standard_log_path, task_id, output_file, logger)

    # Neither format found - provide helpful error message
    error_msg = "Log file not found in either format:\n"
    error_msg += f"  Standard: {standard_log_path}\n"
    if dag_run_id:
        error_msg += f"  Custom: {custom_log_path}\n"
    error_msg += "Make sure the logs_directory is correct and the task has executed."

    logger.error(error_msg)
    raise FileNotFoundError(error_msg)


def _read_and_save_log(
    log_file_path: Path,
    task_id: str,
    output_file: Optional[str],
    logger: logging.Logger,
) -> str:
    """
    Helper function to read a log file and optionally save it to an output file.

    Args:
        log_file_path: Path to the log file to read
        task_id: Task ID (used for default output filename)
        output_file: Optional custom output filename
        logger: Logger instance

    Returns:
        The log file content as a string
    """
    try:
        with open(log_file_path, "r", encoding="utf-8") as f:
            log_content = f.read()

        logger.info(f"Successfully read {len(log_content)} characters from log file")

        # Save to output file if specified
        if output_file is None:
            output_file = f"{task_id}.log"

        # Only write if output file is different from source
        if Path(output_file).resolve() != log_file_path.resolve():
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(log_content)
            logger.info(f"Logs saved to: {output_file}")
        else:
            logger.info("Output file same as source, skipping write")

        return log_content

    except IOError as e:
        logger.error(f"Failed to read log file {log_file_path}: {e}")
        raise


def _get_logs_from_api(
    dag_id: str,
    dag_run_id: Optional[str],
    task_id: str,
    task_try_number: int,
    airflow_base_url: Optional[str],
    username: Optional[str],
    password: Optional[str],
    api_token: Optional[str],
    output_file: Optional[str],
    logger: logging.Logger,
) -> str:
    """
    Fetch logs via the Airflow REST API with automatic pagination.

    This function handles pagination automatically using continuation tokens to retrieve
    the complete log content for a task instance, regardless of size.
    """
    # Validate required parameters
    if not airflow_base_url:
        raise ValueError("airflow_base_url is required for non-local deployment types")

    if not dag_run_id:
        raise ValueError("dag_run_id is required for API-based log retrieval")

    # Validate authentication
    if not api_token and not (username and password):
        raise ValueError("Either api_token or both username and password must be provided for API authentication")

    # Construct the API endpoint URL
    # Remove trailing slash from base URL if present
    base_url = airflow_base_url.rstrip("/")
    endpoint = f"{base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number}"

    # Setup authentication headers
    headers = {"Accept": "application/json"}
    auth: Optional[tuple[str, str]] = None

    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    else:
        # Type assertion: validation above ensures username and password are both str
        assert username is not None and password is not None, "username and password must be provided"
        auth = (username, password)

    logger.info(
        f"Fetching logs via API: dag_id={dag_id}, run_id={dag_run_id}, task_id={task_id}, try_number={task_try_number}"
    )

    # Initialize variables for pagination
    continuation_token: Optional[str] = None
    all_logs: list[str] = []
    request_count = 0

    # Fetch logs with pagination
    while True:
        request_count += 1

        # Build query parameters
        params = {}
        if continuation_token is None:
            # First request: get full content
            params["full_content"] = "true"
            logger.debug(f"Request {request_count}: Fetching initial log content (full_content=true)")
        else:
            # Subsequent requests: use continuation token
            params["token"] = continuation_token
            logger.debug(f"Request {request_count}: Fetching next log chunk (token={continuation_token[:20]}...)")

        try:
            # Make the API request
            response = requests.get(endpoint, params=params, headers=headers, auth=auth, timeout=30)
            response.raise_for_status()

            # Parse the JSON response
            data = response.json()
            content = data.get("content", "")
            continuation_token = data.get("continuation_token")

            # Decode the log content (handles escape sequences)
            if content:
                try:
                    # codecs.escape_decode returns tuple[bytes | str, int], we want the bytes part
                    decoded_bytes = codecs.escape_decode(bytes(content, "utf-8"))[0]
                    if isinstance(decoded_bytes, bytes):
                        decoded_content = decoded_bytes.decode("utf-8")
                    else:
                        decoded_content = decoded_bytes
                    all_logs.append(decoded_content)
                    logger.debug(f"Decoded {len(decoded_content)} characters of log content")
                except Exception as e:
                    logger.warning(f"Failed to decode log content, using raw content: {e}")
                    all_logs.append(content)

            # Check if we're done (no more continuation token)
            if not continuation_token:
                logger.info(f"Log retrieval complete after {request_count} requests")
                break

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e}")
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response body: {e.response.text}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

    # Combine all log chunks
    complete_logs = "".join(all_logs)
    logger.info(f"Total log size: {len(complete_logs)} characters")

    # Save to file
    if output_file is None:
        output_file = f"{task_id}.log"

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(complete_logs)
        logger.info(f"Logs saved to: {output_file}")
    except IOError as e:
        logger.error(f"Failed to write logs to file {output_file}: {e}")
        raise

    return complete_logs

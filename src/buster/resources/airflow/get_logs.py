"""
Airflow log retrieval utilities.

This module provides functions to fetch task logs from Airflow using the Airflow SDK.
"""

import logging
import os
from typing import Dict, Optional


def get_all_task_logs(
    dag_id: str,
    task_id: str,
    dag_run_id: str,
    task_try_number: int = 1,
    logger: Optional[logging.Logger] = None,
) -> str:
    """
    Fetch ALL logs for a specific Airflow task instance using the Airflow SDK.

    This function uses Airflow's internal SDK to query the metadata database and retrieve logs.
    It requires Airflow to be installed. If Airflow is not available, it will log a warning
    and return an empty string (non-fatal error).

    Args:
        dag_id: The DAG ID
        task_id: The task ID
        dag_run_id: The DAG run ID (required)
        task_try_number: The task try number (default: 1). This is the attempt number for retries.
        logger: Optional logger for debug output

    Returns:
        The complete log content as a string, or empty string if Airflow SDK is not available

    Examples:
        >>> # SDK access (automatically uses Airflow if installed)
        >>> logs = get_all_task_logs(
        ...     dag_id="my_dag",
        ...     dag_run_id="manual__2024-01-01T00:00:00+00:00",
        ...     task_id="my_task",
        ... )

    References:
        - Airflow Logging: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/logging-tasks.html
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    # Always try to use SDK method
    return _get_logs_from_sdk(
        dag_id=dag_id,
        task_id=task_id,
        dag_run_id=dag_run_id,
        task_try_number=task_try_number,
        logger=logger,
    )


def _get_logs_from_sdk(
    dag_id: str,
    task_id: str,
    dag_run_id: str,
    task_try_number: int,
    logger: logging.Logger,
) -> str:
    """
    Fetch logs using Airflow SDK by querying the metadata database.

    This method requires Airflow to be installed and accessible. It queries the
    TaskInstance from the metadata database and uses Airflow's configuration
    to determine the log file path.

    If Airflow is not installed, logs a warning and returns an empty string.

    Args:
        dag_id: The DAG ID
        task_id: The task ID
        dag_run_id: The DAG run ID (required)
        task_try_number: The task try number
        logger: Logger instance

    Returns:
        The complete log content from all attempts as a string, or empty string if Airflow is not available
    """
    # Import Airflow dependencies - these are optional for the SDK
    try:
        from airflow import settings
        from airflow.configuration import conf
        from airflow.models import TaskInstance
        from jinja2 import Template
    except ImportError as e:
        logger.warning(
            f"Airflow SDK is not available: {e}. Logs cannot be retrieved. Install apache-airflow to enable log retrieval."
        )
        return ""

    logger.info(f"Querying TaskInstance via SDK: dag_id={dag_id}, run_id={dag_run_id}, task_id={task_id}")

    # Create a session to query the Airflow metadata DB
    session = settings.Session()

    try:
        # Query for the TaskInstance using run_id (Airflow 2.2+)
        # We need to query by dag_id, task_id, and run_id
        ti = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.task_id == task_id,
                TaskInstance.run_id == dag_run_id,
            )
            .first()
        )

        if not ti:
            logger.warning(
                f"No TaskInstance found for dag_id={dag_id}, task_id={task_id}, run_id={dag_run_id}. "
                "Task may not have been executed yet. Returning empty string."
            )
            return ""

        logger.info(f"Found TaskInstance with {ti.try_number} attempts")

        # Get the base log folder and filename template from Airflow config
        base_log_folder = conf.get("logging", "base_log_folder")
        filename_template = conf.get(
            "logging",
            "log_filename_template",
            fallback="{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log",
        )

        logger.debug(f"Base log folder: {base_log_folder}")
        logger.debug(f"Log filename template: {filename_template}")

        # Prepare Jinja template for rendering the relative log path
        template = Template(filename_template)

        # Collect logs from all attempts (1 to ti.try_number or task_try_number if specified)
        max_attempt = ti.try_number if task_try_number == 1 else min(task_try_number, ti.try_number)
        all_logs: list[str] = []

        for attempt in range(1, max_attempt + 1):
            # Build context for Jinja template
            # Include common variables that might be used in templates
            context = {
                "ti": ti,
                "dag_id": ti.dag_id,
                "task_id": ti.task_id,
                "execution_date": ti.execution_date if hasattr(ti, "execution_date") else ti.logical_date,
                "logical_date": ti.logical_date if hasattr(ti, "logical_date") else ti.execution_date,
                "try_number": attempt,
                "ts": ti.logical_date.isoformat() if hasattr(ti, "logical_date") else ti.execution_date.isoformat(),
                "run_id": ti.run_id,
            }

            # Render the template to get the relative log path
            relative_path = template.render(**context)
            log_path = os.path.join(base_log_folder, relative_path)

            logger.debug(f"Checking for log file at: {log_path}")

            if os.path.exists(log_path):
                logger.info(f"Reading log file for attempt {attempt}: {log_path}")
                with open(log_path, "r", encoding="utf-8") as f:
                    log_content = f.read()
                    if log_content:
                        all_logs.append(f"\n\n=== Attempt {attempt} Logs ===\n\n")
                        all_logs.append(log_content)
                        logger.info(f"Read {len(log_content)} characters from attempt {attempt}")
            else:
                logger.warning(f"Log file not found for attempt {attempt}: {log_path}")

        if not all_logs:
            logger.warning(
                f"No log files found for dag_id={dag_id}, task_id={task_id}, run_id={dag_run_id}. "
                f"Expected logs in: {base_log_folder}. Returning empty string."
            )
            return ""

        # Combine all log chunks
        complete_logs = "".join(all_logs)
        logger.info(f"Total log size: {len(complete_logs)} characters from {max_attempt} attempts")

        return complete_logs

    finally:
        session.close()


def get_all_dag_logs(
    dag_id: str,
    dag_run_id: str,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, str]:
    """
    Fetch logs for ALL tasks in a DAG run using the Airflow SDK.

    This function queries all TaskInstances for the given DAG run and retrieves
    logs for each task. It requires Airflow to be installed. If Airflow is not
    available, it will log a warning and return an empty dictionary (non-fatal error).

    Args:
        dag_id: The DAG ID
        dag_run_id: The DAG run ID (required)
        logger: Optional logger for debug output

    Returns:
        A dictionary mapping task_id to log content. Returns empty dict if Airflow SDK is not available.
        Format: {"task_id_1": "logs...", "task_id_2": "logs...", ...}

    Examples:
        >>> # Get all logs for a DAG run
        >>> logs = get_all_dag_logs(
        ...     dag_id="my_dag",
        ...     dag_run_id="manual__2024-01-01T00:00:00+00:00",
        ... )
        >>> for task_id, task_logs in logs.items():
        ...     print(f"Task {task_id}: {len(task_logs)} characters")
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    # Import Airflow dependencies - these are optional for the SDK
    try:
        from airflow import settings
        from airflow.models import TaskInstance
    except ImportError as e:
        logger.warning(
            f"Airflow SDK is not available: {e}. Logs cannot be retrieved. Install apache-airflow to enable log retrieval."
        )
        return {}

    logger.info(f"Querying all TaskInstances for DAG run: dag_id={dag_id}, run_id={dag_run_id}")

    # Create a session to query the Airflow metadata DB
    session = settings.Session()

    try:
        # Query for all TaskInstances in this DAG run
        task_instances = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == dag_run_id,
            )
            .all()
        )

        if not task_instances:
            logger.warning(
                f"No TaskInstances found for dag_id={dag_id}, run_id={dag_run_id}. "
                "DAG run may not have been executed yet. Returning empty dictionary."
            )
            return {}

        logger.info(f"Found {len(task_instances)} tasks in DAG run")

        # Collect logs for each task
        all_logs: Dict[str, str] = {}

        for ti in task_instances:
            task_id = ti.task_id
            logger.info(f"Retrieving logs for task: {task_id}")

            # Use get_all_task_logs to get logs for this task
            task_logs = get_all_task_logs(
                dag_id=dag_id,
                task_id=task_id,
                dag_run_id=dag_run_id,
                task_try_number=ti.try_number,
                logger=logger,
            )

            if task_logs:
                all_logs[task_id] = task_logs
                logger.info(f"Retrieved {len(task_logs)} characters for task {task_id}")
            else:
                logger.warning(f"No logs found for task {task_id}")
                all_logs[task_id] = ""

        logger.info(f"Successfully retrieved logs for {len(all_logs)} tasks")
        return all_logs

    finally:
        session.close()

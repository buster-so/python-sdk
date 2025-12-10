from typing import Any, Dict, List, Optional

from buster.types import (
    AirflowCallbackContext,
    ApiVersion,
    DagConfig,
    DataInterval,
    Environment,
    ExceptionLocation,
    RetryConfig,
    TaskDependencies,
    TracebackFrame,
)
from buster.types.api import AirflowFlowVersion
from buster.utils import get_buster_url


def get_airflow_version(flow_version: AirflowFlowVersion = "3.1") -> str:
    """
    Attempts to detect the installed Airflow version using the official pattern.

    This follows the same approach used by Apache Airflow's official provider packages:
    https://airflow.apache.org/docs/apache-airflow-providers/

    Returns:
        The Airflow version string (e.g., "2.5.0" or "3.1")
    """
    try:
        # Try importing __version__ directly (preferred method)
        from airflow import (  # type: ignore[import-not-found] # pyright: ignore[reportMissingImports]
            __version__ as airflow_version,
        )

        return str(airflow_version)
    except ImportError:
        try:
            # Fallback to airflow.version.version (used in some environments)
            from airflow.version import (  # type: ignore[import-not-found] # pyright: ignore[reportMissingImports]
                version as airflow_version,
            )

            return str(airflow_version)
        except (ImportError, AttributeError):
            # Airflow not installed or version not available, use default
            return flow_version


def get_airflow_v3_url(env: Environment, api_version: ApiVersion) -> str:
    """
    Constructs the full API URL based on the environment and API version.
    """
    base_url = get_buster_url(env, api_version)
    return f"{base_url}/public/airflow-events"


def extract_exception_location(exception) -> ExceptionLocation:
    """
    Extract structured exception location data for LLM consumption.

    This provides direct field access to exception details instead of requiring
    the LLM to parse text. Returns file path, line number, function name, and
    exception type as separate fields.

    Args:
        exception: Python exception object with __traceback__ attribute

    Returns:
        Dictionary with keys:
        - type: Exception class name (e.g., "ValueError")
        - message: Exception message string
        - file: Full file path where exception occurred
        - line: Line number where exception occurred
        - function: Function name where exception occurred

        Returns empty dict if exception is None or has no traceback.
    """
    if not exception or isinstance(exception, str):
        return {}

    location: ExceptionLocation = {
        "type": type(exception).__name__,
        "message": str(exception),
    }

    # Extract location from traceback
    if hasattr(exception, "__traceback__") and exception.__traceback__:
        tb = exception.__traceback__
        # Walk to last frame to get actual error location (not wrapper code)
        while tb.tb_next:
            tb = tb.tb_next

        location["file"] = tb.tb_frame.f_code.co_filename
        location["line"] = tb.tb_lineno
        location["function"] = tb.tb_frame.f_code.co_name

    return location


def extract_traceback_frames(exception) -> List[TracebackFrame]:
    """
    Extract structured traceback frames for LLM consumption.

    Instead of plain text traceback, returns an array of frame objects with
    file, line, function, and code for each frame in the call stack.

    Args:
        exception: Python exception object with __traceback__ attribute

    Returns:
        List of dictionaries, each containing:
        - file: File path
        - line: Line number
        - function: Function name
        - code: Line of code (if available)

        Returns empty list if exception is None or has no traceback.
    """
    if not exception or isinstance(exception, str):
        return []

    if not hasattr(exception, "__traceback__") or not exception.__traceback__:
        return []

    import linecache

    frames: List[TracebackFrame] = []
    tb = exception.__traceback__

    while tb:
        frame = tb.tb_frame
        filename = frame.f_code.co_filename
        lineno = tb.tb_lineno
        func_name = frame.f_code.co_name

        # Try to get the actual line of code
        code_line = linecache.getline(filename, lineno).strip()

        frames.append(
            {
                "file": filename,
                "line": lineno,
                "function": func_name,
                "code": code_line if code_line else None,
            }
        )

        tb = tb.tb_next

    return frames


def extract_error_message(context: AirflowCallbackContext) -> str:
    """
    Extracts error context from Airflow task failure callbacks.

    Captures:
    - Exception type and message
    - Task execution details (state, try number, duration, hostname)
    - Airflow UI log URL

    Note: Traceback details are sent separately in the traceback_frames field
    to avoid duplication and provide structured data for LLM consumption.

    Args:
        context: Airflow callback context dictionary containing exception and task info

    Returns:
        Formatted error message string with execution context and log URL
    """
    import logging

    logger = logging.getLogger(__name__)
    logger.debug(f"Extracting error from context with keys: {list(context.keys())}")

    parts = []

    # Get the exception - this is the primary error source for task failures
    exception = context.get("exception")

    if not exception:
        logger.warning("No exception found in context")
        # Fallback: check for alternative error fields (rare, but defensive)
        exception = context.get("reason") or context.get("error")

    logger.debug(f"Exception type: {type(exception)}")

    if exception:
        # Handle both string and exception object formats
        if isinstance(exception, str):
            # Exception was serialized as string (rare in Airflow 3.x)
            parts.append(f"Exception: {exception}")
            logger.debug("Exception provided as string")
        else:
            # Exception is an object - extract type and message only
            # (traceback is sent separately in traceback_frames field)
            exc_type = type(exception).__name__
            exc_message = str(exception)
            parts.append(f"{exc_type}: {exc_message}")
            logger.debug(f"Captured exception: {exc_type}")

    # Extract task instance information for additional context
    ti = context.get("task_instance") or context.get("ti")
    if ti:
        execution_details = []

        # Task state
        if hasattr(ti, "state"):
            execution_details.append(f"State: {ti.state}")

        # Retry attempt information
        if hasattr(ti, "try_number"):
            execution_details.append(f"Try: {ti.try_number}")
        if hasattr(ti, "max_tries"):
            execution_details.append(f"Max Tries: {ti.max_tries}")

        # Execution timing
        if hasattr(ti, "start_date") and ti.start_date:
            execution_details.append(f"Started: {ti.start_date}")
        if hasattr(ti, "duration") and ti.duration:
            execution_details.append(f"Duration: {ti.duration}s")

        # Execution environment
        if hasattr(ti, "hostname") and ti.hostname:
            execution_details.append(f"Host: {ti.hostname}")

        if execution_details:
            parts.append("\nExecution Details:\n" + "\n".join(execution_details))

        # Airflow UI log URL - critical for debugging
        if hasattr(ti, "log_url") and ti.log_url:
            parts.append(f"\nLogs: {ti.log_url}")

    # Return formatted error message
    if not parts:
        logger.warning("No error information could be extracted from context")
        return "Unknown error - no exception or error details found in context"

    result = "\n\n".join(parts)
    logger.debug(f"Extracted error message: {len(result)} characters")
    return result


class ExtractedContext:
    """
    Internal data class holding values extracted from Airflow's Context.

    Uses __slots__ for memory efficiency since we may create many instances.
    All fields are Optional since extraction from context may fail or values
    may not be present.
    """

    __slots__ = (
        "dag_id",
        "run_id",
        "task_id",
        "try_number",
        "max_tries",
        "operator",
        "params",
        "task_dependencies",
        "retry_config",
        "dag_config",
        "data_interval",
    )

    def __init__(
        self,
        dag_id: Optional[str] = None,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        try_number: Optional[int] = None,
        max_tries: Optional[int] = None,
        operator: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,  # User-defined - must remain Any
        task_dependencies: Optional[TaskDependencies] = None,
        retry_config: Optional[RetryConfig] = None,
        dag_config: Optional[DagConfig] = None,
        data_interval: Optional[DataInterval] = None,
    ):
        self.dag_id = dag_id
        self.run_id = run_id
        self.task_id = task_id
        self.try_number = try_number
        self.max_tries = max_tries
        self.operator = operator
        self.params = params
        self.task_dependencies = task_dependencies
        self.retry_config = retry_config
        self.dag_config = dag_config
        self.data_interval = data_interval


def extract_context_values(context: AirflowCallbackContext) -> ExtractedContext:
    """
    Extract relevant values from Airflow's Context dictionary.

    This function supports two context structures:
    1. Real Airflow contexts: Values stored in nested objects (context['dag_run'],
       context['task_instance'] or context['ti'])
    2. Flat dictionaries: Values at top level (for testing/simpler use cases)

    The function tries nested objects first, then falls back to top-level keys.

    Args:
        context: Airflow callback context dictionary

    Returns:
        ExtractedContext with all available values extracted (may contain None values)
    """
    dag_id: Optional[str] = None
    run_id: Optional[str] = None
    task_id: Optional[str] = None
    try_number: Optional[int] = None
    max_tries: Optional[int] = None

    # Extract from dag_run object
    dag_run = context.get("dag_run")
    if dag_run is not None:
        if hasattr(dag_run, "dag_id"):
            dag_id = dag_run.dag_id
        if hasattr(dag_run, "run_id"):
            run_id = dag_run.run_id

    # Fallback: dag_id might be at top level (for testing or simpler contexts)
    if dag_id is None:
        top_dag_id = context.get("dag_id")
        if isinstance(top_dag_id, str):
            dag_id = top_dag_id

    # Fallback: run_id might be at top level
    if run_id is None:
        top_run_id = context.get("run_id")
        if isinstance(top_run_id, str):
            run_id = top_run_id

    # Extract from task_instance (or ti) object
    ti = context.get("task_instance") or context.get("ti")
    if ti is not None:
        if hasattr(ti, "task_id"):
            task_id = ti.task_id
        if hasattr(ti, "try_number"):
            try_number = ti.try_number
        if hasattr(ti, "max_tries"):
            max_tries = ti.max_tries

    # Fallback: task_id might be at top level (for testing or simpler contexts)
    if task_id is None:
        top_task_id = context.get("task_id")
        if isinstance(top_task_id, str):
            task_id = top_task_id

    # Fallback: try_number might be at top level
    if try_number is None:
        top_try = context.get("try_number")
        if isinstance(top_try, int):
            try_number = top_try

    # Fallback: max_tries might be at top level (for testing or simpler contexts)
    if max_tries is None:
        top_max = context.get("max_tries")
        if isinstance(top_max, int):
            max_tries = top_max

    # Extract operator type from task object
    operator: Optional[str] = None
    task = context.get("task")
    if task is not None:
        if hasattr(task, "operator_name"):
            operator = task.operator_name
        elif hasattr(task, "task_type"):
            operator = task.task_type
        elif hasattr(task, "__class__"):
            operator = task.__class__.__name__

    # Extract task parameters
    params: Optional[Dict[str, Any]] = None
    context_params = context.get("params")
    if isinstance(context_params, dict) and context_params:
        # Serialize params to JSON-safe format
        try:
            import json

            json.dumps(context_params)  # Validate serializable
            params = context_params
        except (TypeError, ValueError):
            # Params contain non-serializable objects, convert to strings
            params = {k: str(v) for k, v in context_params.items()}

    # Extract task dependencies (upstream/downstream)
    task_dependencies: Optional[TaskDependencies] = None
    if task is not None:
        deps: TaskDependencies = {}
        if hasattr(task, "upstream_task_ids") and task.upstream_task_ids:
            deps["upstream"] = list(task.upstream_task_ids)
        if hasattr(task, "downstream_task_ids") and task.downstream_task_ids:
            deps["downstream"] = list(task.downstream_task_ids)
        if deps:
            task_dependencies = deps

    # Extract retry configuration
    retry_config: Optional[RetryConfig] = None
    if task is not None:
        retry_cfg: RetryConfig = {}
        if hasattr(task, "retries") and task.retries is not None:
            retry_cfg["retries"] = task.retries
        if hasattr(task, "retry_delay") and task.retry_delay:
            retry_cfg["retry_delay"] = str(task.retry_delay)
        if hasattr(task, "retry_exponential_backoff"):
            retry_cfg["retry_exponential_backoff"] = task.retry_exponential_backoff
        if hasattr(task, "max_retry_delay") and task.max_retry_delay:
            retry_cfg["max_retry_delay"] = str(task.max_retry_delay)
        if retry_cfg:
            retry_config = retry_cfg

    # Extract DAG configuration
    dag_config: Optional[DagConfig] = None
    dag = context.get("dag")
    if dag is not None:
        dag_cfg: DagConfig = {}
        if hasattr(dag, "schedule_interval") and dag.schedule_interval:
            dag_cfg["schedule_interval"] = str(dag.schedule_interval)
        if hasattr(dag, "description") and dag.description:
            dag_cfg["description"] = dag.description
        if hasattr(dag, "catchup"):
            dag_cfg["catchup"] = dag.catchup
        if hasattr(dag, "max_active_runs") and dag.max_active_runs:
            dag_cfg["max_active_runs"] = dag.max_active_runs
        if dag_cfg:
            dag_config = dag_cfg

    # Extract data interval information
    data_interval: Optional[DataInterval] = None
    dag_run = context.get("dag_run")
    if dag_run is not None:
        interval: DataInterval = {}
        if hasattr(dag_run, "data_interval_start") and dag_run.data_interval_start:
            interval["start"] = str(dag_run.data_interval_start)
        if hasattr(dag_run, "data_interval_end") and dag_run.data_interval_end:
            interval["end"] = str(dag_run.data_interval_end)
        if interval:
            data_interval = interval

    return ExtractedContext(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        try_number=try_number,
        max_tries=max_tries,
        operator=operator,
        params=params,
        task_dependencies=task_dependencies,
        retry_config=retry_config,
        dag_config=dag_config,
        data_interval=data_interval,
    )

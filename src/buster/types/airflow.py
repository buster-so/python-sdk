from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Protocol,
    TypedDict,
    runtime_checkable,
)

# Structured types for Airflow event data


class ExceptionLocation(TypedDict, total=False):
    """
    Structured exception location extracted from Python traceback.

    Provides direct field access to exception details for LLM consumption.
    """

    type: str  # Exception class name (e.g., "ValueError")
    message: str  # Exception message string
    file: str  # Full file path where exception occurred
    line: int  # Line number where exception occurred
    function: str  # Function name where exception occurred


class TracebackFrame(TypedDict, total=False):
    """
    Single frame in a Python traceback stack.

    Part of structured traceback for LLM analysis.
    """

    file: str  # File path
    line: int  # Line number
    function: str  # Function name
    code: Optional[str]  # Line of code (if available)


class TaskDependencies(TypedDict, total=False):
    """
    Task dependency relationships in the DAG.

    Shows which tasks must complete before/after this task.
    """

    upstream: List[str]  # Task IDs that must complete before this task
    downstream: List[str]  # Task IDs that run after this task completes


class RetryConfig(TypedDict, total=False):
    """
    Task retry configuration from Airflow.

    Defines how the task behaves when it fails.
    """

    retries: int  # Maximum number of retry attempts
    retry_delay: str  # Delay between retries (e.g., "0:05:00")
    retry_exponential_backoff: bool  # Whether retry delay increases exponentially
    max_retry_delay: str  # Maximum retry delay (e.g., "1:00:00")


class DagConfig(TypedDict, total=False):
    """
    DAG-level configuration from Airflow.

    Provides context about the DAG's scheduling and behavior.
    """

    schedule_interval: str  # Cron expression or timedelta string
    description: str  # Human-readable DAG description
    catchup: bool  # Whether to backfill missed runs
    max_active_runs: int  # Maximum concurrent DAG runs


class DataInterval(TypedDict, total=False):
    """
    Data interval for the DAG run.

    Defines the time window of data this run is processing.
    """

    start: str  # Data interval start time (ISO format)
    end: str  # Data interval end time (ISO format)


# Use Airflow's Context for static type checking only - avoids runtime import issues
# across different Airflow versions while maintaining full type safety
if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context as AirflowCallbackContext
else:
    # At runtime, any dict-like context works - we use Protocol checks for extraction
    AirflowCallbackContext = dict


class AirflowEventType(str, Enum):
    """Enum for Airflow event types sent to Buster API."""

    DAG_RUN_FAILED = "dag_run_failed"
    TASK_INSTANCE_FAILED = "task_instance_failed"


class AirflowEventsPayload(TypedDict, total=False):
    """
    TypedDict for Airflow event payload sent to Buster API.

    This matches the API contract and is used for type checking the request payload.
    Field names match the API expectations (e.g., 'event' not 'event_type').

    Optional fields include structured data for LLM consumption:
    - exception_location: Structured exception details (file, line, function)
    - operator: Task operator type (e.g., "PythonOperator")
    - params: Task parameters
    - Execution context: state, hostname, duration, start_date, log_url
    - traceback_frames: Structured traceback as array of frames
    - task_dependencies: Upstream/downstream task relationships
    - retry_config: Retry configuration details
    - dag_config: DAG-level configuration (schedule, description, etc.)
    - data_interval: Data interval start/end times
    """

    # Required fields
    dag_id: str
    run_id: str
    event: str  # Event type as string value (from AirflowEventType enum)

    # Optional core fields
    task_id: Optional[str]
    try_number: Optional[int]
    error_message: Optional[str]
    airflow_version: Optional[str]

    # Structured exception details
    exception_location: Optional[ExceptionLocation]

    # Task metadata
    operator: Optional[str]
    params: Optional[Dict[str, Any]]  # User-defined parameters - must remain Any

    # Execution context
    state: Optional[str]
    hostname: Optional[str]
    duration: Optional[float]
    start_date: Optional[str]
    log_url: Optional[str]

    # Structured traceback
    traceback_frames: Optional[List[TracebackFrame]]

    # Task relationships and configuration
    task_dependencies: Optional[TaskDependencies]
    retry_config: Optional[RetryConfig]

    # DAG configuration
    dag_config: Optional[DagConfig]
    data_interval: Optional[DataInterval]


@runtime_checkable
class DagRunProtocol(Protocol):
    """
    Protocol for runtime checking of Airflow DagRun objects.

    Used to safely extract dag_id and run_id from context["dag_run"].
    Compatible with Airflow 2.x DagRun and 3.x DagRunProtocol.

    Note: Currently not used in favor of hasattr() checks for better
    compatibility with flat dictionary contexts in tests.
    """

    dag_id: str
    run_id: str


@runtime_checkable
class TaskInstanceProtocol(Protocol):
    """
    Protocol for runtime checking of Airflow TaskInstance objects.

    Used to safely extract task_id, try_number, max_tries from
    context["task_instance"] or context["ti"].
    Compatible with Airflow 2.x TaskInstance and 3.x RuntimeTaskInstanceProtocol.

    Note: Currently not used in favor of hasattr() checks for better
    compatibility with flat dictionary contexts in tests.
    """

    task_id: str
    try_number: int
    max_tries: int


class AirflowReportConfig(TypedDict, total=False):
    """
    Configuration options for Airflow error reporting.

    All fields are optional (total=False). Used when initializing the Client
    with airflow_config parameter.
    """

    airflow_version: Optional[str]
    send_when_retries_exhausted: bool

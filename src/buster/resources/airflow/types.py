from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from typing_extensions import TypedDict

# Airflow types - use static type checking only to avoid runtime import issues


if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from airflow.sdk.definitions.context import Context as AirflowCallbackContext
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
    from airflow.utils.state import TaskInstanceState
else:
    # At runtime, any dict-like context works
    AirflowCallbackContext = dict
    RuntimeTaskInstance = object  # type: ignore[misc, assignment]
    TaskInstanceState = object  # type: ignore[misc, assignment]
    DagRun = object  # type: ignore[misc, assignment]


class AirflowEventType(str, Enum):
    """Enum for Airflow event types sent to Buster API."""

    TASK_ON_FAILURE = "task_on_failure"
    DAG_ON_FAILURE = "dag_on_failure"


class AirflowEventTriggerType(str, Enum):
    DAG = "dag"
    PLUGIN = "plugin"


class AirflowTaskFailureCallback(TypedDict, total=False):
    """
    TypedDict representing the Airflow 3 task failure callback context.

    This matches the context dictionary passed to on_failure_callback for tasks.
    All fields are optional as Airflow may not provide all of them.
    """

    # Core identifiers
    dag_id: str
    run_id: str
    task_id: str

    # Exception and error information
    exception: Any  # BaseException or str
    reason: Optional[str]

    # Task instance and execution details
    task_instance: Any  # RuntimeTaskInstance or similar
    ti: Any  # Alternative reference to task_instance
    try_number: Optional[int]
    max_tries: Optional[int]

    # DAG and task objects
    dag: Any  # DAG object
    dag_run: Any  # DagRun object
    task: Any  # Task/BaseOperator object

    # Task configuration
    params: Optional[Dict[str, Any]]
    conf: Optional[Dict[str, Any]]

    # Execution context
    execution_date: Optional[Any]  # datetime
    logical_date: Optional[Any]  # datetime
    data_interval_start: Optional[Any]  # datetime
    data_interval_end: Optional[Any]  # datetime

    # Other context fields that may be present
    ds: Optional[str]  # Execution date as string
    ds_nodash: Optional[str]
    ts: Optional[str]  # Timestamp
    ts_nodash: Optional[str]
    prev_ds: Optional[str]
    prev_ds_nodash: Optional[str]
    next_ds: Optional[str]
    next_ds_nodash: Optional[str]
    yesterday_ds: Optional[str]
    yesterday_ds_nodash: Optional[str]
    tomorrow_ds: Optional[str]
    tomorrow_ds_nodash: Optional[str]

    # Additional context
    macros: Optional[Any]
    var: Optional[Dict[str, Any]]
    conn: Optional[Any]
    test_mode: Optional[bool]


class AirflowDagFailureCallback(TypedDict, total=False):
    """
    TypedDict representing the Airflow 3 DAG failure callback context.

    This matches the context dictionary passed to on_failure_callback for DAGs.
    All fields are optional as Airflow may not provide all of them.
    """

    # Core identifiers
    dag_id: str
    run_id: str

    # Exception and error information
    exception: Any  # BaseException or str
    reason: Optional[str]

    # DAG run details
    dag_run: Any  # DagRun object
    dag: Any  # DAG object

    # Execution context
    execution_date: Optional[Any]  # datetime
    logical_date: Optional[Any]  # datetime
    data_interval_start: Optional[Any]  # datetime
    data_interval_end: Optional[Any]  # datetime

    # Configuration
    conf: Optional[Dict[str, Any]]

    # Other context fields
    ds: Optional[str]
    ts: Optional[str]
    macros: Optional[Any]
    var: Optional[Dict[str, Any]]
    conn: Optional[Any]
    test_mode: Optional[bool]


class AirflowPluginTaskFailureCallback(TypedDict, total=False):
    """
    TypedDict representing the Airflow 3 plugin task failure hook context.

    This matches the parameters passed to @hookimpl on_task_instance_failed.
    Note: This is NOT a dictionary in the actual hook - these are separate parameters.
    We structure it as a TypedDict for consistency in our payload.
    """

    # Previous state of the task instance
    previous_state: str  # TaskInstanceState as string

    # Task instance object
    task_instance: Any  # RuntimeTaskInstance

    # Error information
    error: Any  # str | BaseException | None
    msg: Optional[str]  # Error message if error is BaseException


class AirflowPluginDagFailureCallback(TypedDict, total=False):
    """
    TypedDict representing the Airflow 3 plugin DAG failure hook context.

    This matches the parameters passed to @hookimpl on_dag_run_failed.
    Note: This is NOT a dictionary in the actual hook - these are separate parameters.
    We structure it as a TypedDict for consistency in our payload.
    """

    # DAG run object
    dag_run: Any  # DagRun

    # Error message
    msg: str


class AirflowEventsPayload(TypedDict):
    """
    TypedDict for Airflow event payload sent to Buster API.

    This matches the new API contract where we send the complete Airflow context
    to the server instead of extracting individual fields.

    The payload structure is:
    {
        event_type: 'task_on_failure' | 'dag_on_failure',
        event_trigger_type: 'dag' | 'plugin',
        airflow_version: string,
        context: AirflowTaskFailureCallback | AirflowDagFailureCallback | AirflowPluginTaskFailureCallback | AirflowPluginDagFailureCallback
    }
    """

    # Event type (required) - literal string value from AirflowEventType enum
    event_type: AirflowEventType

    event_trigger_type: AirflowEventTriggerType

    # Airflow version (required) - version string like "2.5.0" or "3.1"
    airflow_version: str

    # Full callback context (required) - the type depends on event_type
    # After serialization, this becomes a Dict[str, Any], but the input structure
    # matches one of the four callback types below
    context: Union[
        AirflowTaskFailureCallback,
        AirflowDagFailureCallback,
        AirflowPluginTaskFailureCallback,
        AirflowPluginDagFailureCallback,
    ]


class AirflowReportConfig(TypedDict, total=False):
    """
    Configuration options for Airflow error reporting.

    All fields are optional (total=False). Used when initializing the Client
    with airflow_config parameter.
    """

    send_when_retries_exhausted: bool

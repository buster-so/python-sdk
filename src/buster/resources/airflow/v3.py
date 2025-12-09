from typing import Any, Dict, List, Optional, cast

from pydantic import BaseModel, Field, ValidationError

from buster.types import (
    AirflowCallbackContext,
    AirflowEventsPayload,
    AirflowEventType,
    AirflowReportConfig,
    ApiVersion,
    DagConfig,
    DataInterval,
    Environment,
    ExceptionLocation,
    RetryConfig,
    TaskDependencies,
    TracebackFrame,
)
from buster.utils import send_request

from .utils import (
    ExtractedContext,
    extract_context_values,
    extract_error_message,
    get_airflow_v3_url,
    get_airflow_version,
)


class AirflowErrorEvent(BaseModel):
    """
    Pydantic model for validating Airflow error events before sending to API.

    All required fields (dag_id, run_id, event_type, api_version, env) must be
    provided. Optional fields are used when available from the Airflow context.

    Optional fields include:
    - exception_location: Structured exception details for LLM consumption
    - operator: Task operator type
    - params: Task parameters
    - traceback_frames: Structured traceback as array of frames
    - task_dependencies: Upstream/downstream task relationships
    - retry_config: Retry configuration details
    - dag_config: DAG-level configuration
    - data_interval: Data interval start/end times
    - Execution context: state, hostname, duration, start_date, log_url
    """

    event_type: AirflowEventType
    dag_id: str = Field(..., min_length=1)
    run_id: str = Field(..., min_length=1)
    task_id: Optional[str] = None
    try_number: Optional[int] = None
    error_message: Optional[str] = None
    airflow_version: Optional[str] = None
    api_version: ApiVersion
    env: Environment

    # Structured exception details
    exception_location: Optional[ExceptionLocation] = None

    # Task metadata
    operator: Optional[str] = None
    params: Optional[Dict[str, Any]] = None  # User-defined parameters - must remain Any

    # Execution context
    state: Optional[str] = None
    hostname: Optional[str] = None
    duration: Optional[float] = None
    start_date: Optional[str] = None
    log_url: Optional[str] = None

    # Structured traceback
    traceback_frames: Optional[List[TracebackFrame]] = None

    # Task relationships and configuration
    task_dependencies: Optional[TaskDependencies] = None
    retry_config: Optional[RetryConfig] = None

    # DAG configuration
    dag_config: Optional[DagConfig] = None
    data_interval: Optional[DataInterval] = None

    def to_payload(self) -> AirflowEventsPayload:
        """
        Convert the validated event to the API payload format.

        This method ensures consistency between validation and payload structure,
        mapping event_type enum to the 'event' string field expected by the API.

        Returns:
            AirflowEventsPayload ready to send to the API. None values are
            included in the payload.
        """
        return cast(
            AirflowEventsPayload,
            {
                "dag_id": self.dag_id,
                "run_id": self.run_id,
                "task_id": self.task_id,
                "try_number": self.try_number,
                "event": self.event_type.value,
                "error_message": self.error_message,
                "airflow_version": self.airflow_version,
                "exception_location": self.exception_location,
                "operator": self.operator,
                "params": self.params,
                "state": self.state,
                "hostname": self.hostname,
                "duration": self.duration,
                "start_date": self.start_date,
                "log_url": self.log_url,
                "traceback_frames": self.traceback_frames,
                "task_dependencies": self.task_dependencies,
                "retry_config": self.retry_config,
                "dag_config": self.dag_config,
                "data_interval": self.data_interval,
            },
        )


class AirflowV3:
    def __init__(self, client, config: Optional[AirflowReportConfig] = None):
        self.client = client
        self._config = config or {}
        client.logger.debug("AirflowV3 handler initialized")

    def _report_error(
        self,
        extracted: ExtractedContext,
        error_message: Optional[str] = None,
        event_type: AirflowEventType = AirflowEventType.TASK_INSTANCE_FAILED,
        exception_location: Optional[ExceptionLocation] = None,
        execution_context: Optional[Dict[str, Any]] = None,
        traceback_frames: Optional[List[TracebackFrame]] = None,
    ) -> None:
        """
        Internal method to report an Airflow error event to Buster API.

        This method:
        1. Validates required fields (dag_id, run_id) using Pydantic
        2. Checks if retries are exhausted (if send_when_retries_exhausted is True)
        3. Constructs and sends the API request

        Args:
            extracted: Context values extracted from Airflow callback
            error_message: Human-readable error message
            event_type: Type of event (DAG_RUN_FAILED or TASK_INSTANCE_FAILED)
            exception_location: Structured exception location (file, line, function)
            execution_context: Execution details (state, hostname, duration, etc.)
            traceback_frames: Structured traceback as array of frames

        Raises:
            ValueError: If required fields are missing or invalid

        Returns:
            None
        """
        dag_id = extracted.dag_id
        run_id = extracted.run_id
        task_id = extracted.task_id
        try_number = extracted.try_number
        max_tries = extracted.max_tries or 0

        self.client.logger.info(
            f"📋 Reporting {event_type.value}: dag_id={dag_id}, run_id={run_id}"
            + (f", task_id={task_id}" if task_id else "")
        )
        self.client.logger.debug(
            f"Event details: try_number={try_number}, max_tries={max_tries}"
        )

        # Extract values from config with defaults
        config = self._config
        # Try to get airflow_version from config, otherwise detect it automatically
        airflow_version = config.get("airflow_version") or get_airflow_version()
        send_when_retries_exhausted = config.get("send_when_retries_exhausted", True)

        # Use env and api_version from client (set at client level)
        env = self.client.env
        api_version = self.client.api_version

        # Logic to check if we should send the event based on retries
        if send_when_retries_exhausted and try_number is not None:
            if try_number < max_tries:
                self.client.logger.info(
                    f"⏭️  Skipping report (retries not exhausted): "
                    f"try {try_number}/{max_tries}"
                )
                return

        try:
            # Validate inputs by creating the model
            self.client.logger.debug("Validating event data...")

            # Extract execution context fields
            exec_ctx = execution_context or {}

            event = AirflowErrorEvent(
                event_type=event_type,
                dag_id=dag_id or "",
                run_id=run_id or "",
                task_id=task_id,
                try_number=try_number,
                error_message=error_message,
                airflow_version=airflow_version,
                api_version=api_version,
                env=env,
                exception_location=exception_location,
                operator=extracted.operator,
                params=extracted.params,
                state=exec_ctx.get("state"),
                hostname=exec_ctx.get("hostname"),
                duration=exec_ctx.get("duration"),
                start_date=exec_ctx.get("start_date"),
                log_url=exec_ctx.get("log_url"),
                traceback_frames=traceback_frames,
                task_dependencies=extracted.task_dependencies,
                retry_config=extracted.retry_config,
                dag_config=extracted.dag_config,
                data_interval=extracted.data_interval,
            )
            self.client.logger.debug("Event validation successful")

            # Convert validated event to API payload format
            request_payload = event.to_payload()

            # Construct the URL
            url = get_airflow_v3_url(env, api_version)
            self.client.logger.debug(
                f"Sending request to: {url} (env={env}, api_version={api_version})"
            )

            # Log the full payload for debugging
            self.client.logger.debug("=" * 80)
            self.client.logger.debug("FULL PAYLOAD BEING SENT:")
            self.client.logger.debug("=" * 80)
            for key, value in request_payload.items():
                if key == "error_message" and value:
                    self.client.logger.debug(f"{key}:")
                    self.client.logger.debug(f"{value}")
                    self.client.logger.debug(
                        f"(error_message length: {len(str(value))} characters)"
                    )
                else:
                    self.client.logger.debug(f"{key}: {value}")
            self.client.logger.debug("=" * 80)

            # Send the request
            send_request(
                url,
                cast(Dict[str, Any], request_payload),
                self.client._buster_api_key,
                self.client.logger,
            )

            self.client.logger.info("✓ Event reported successfully")

        except ValidationError as e:
            # Create a friendly error message
            issues = []
            for err in e.errors():
                # Get the field name (it's a tuple, we want the first element)
                # If loc is empty (root validator), use "root"
                field = str(err["loc"][0]) if err["loc"] else "root"
                msg = err["msg"]
                issues.append(f"- {field}: {msg}")

            error_msg = "Invalid arguments provided to report_error:\n" + "\n".join(
                issues
            )
            self.client.logger.error(f"❌ Validation error: {error_msg}")
            raise ValueError(error_msg) from e

    def dag_on_failure(self, context: AirflowCallbackContext) -> None:
        """
        Airflow callback for DAG failures.

        Usage:
            dag = DAG(..., on_failure_callback=client.airflow.v3.dag_on_failure)

        Args:
            context: The Airflow context dictionary
                (airflow.sdk.definitions.context.Context).
        """
        self.client.logger.debug("DAG failure callback triggered")
        self.client.logger.debug(f"Context keys: {list(context.keys())}")

        extracted = extract_context_values(context)
        self.client.logger.debug(
            f"Extracted: dag_id={extracted.dag_id}, run_id={extracted.run_id}"
        )

        error_message = extract_error_message(context)
        if error_message:
            self.client.logger.debug(
                f"Error message length: {len(error_message)} chars"
            )
            self.client.logger.debug(f"Error message preview: {error_message[:200]}...")
        else:
            self.client.logger.warning("No error message extracted from context")

        # Extract structured exception data for LLM consumption
        exception = context.get("exception")
        from .utils import extract_exception_location, extract_traceback_frames

        exception_location = extract_exception_location(exception)
        traceback_frames = extract_traceback_frames(exception)

        # Extract execution context (may be limited for DAG failures)
        execution_context = {}
        ti = context.get("task_instance") or context.get("ti")
        if ti:
            if hasattr(ti, "state"):
                execution_context["state"] = ti.state
            if hasattr(ti, "hostname"):
                execution_context["hostname"] = ti.hostname
            if hasattr(ti, "duration"):
                execution_context["duration"] = ti.duration
            if hasattr(ti, "start_date"):
                execution_context["start_date"] = str(ti.start_date)
            if hasattr(ti, "log_url"):
                execution_context["log_url"] = ti.log_url

        self._report_error(
            extracted,
            error_message,
            AirflowEventType.DAG_RUN_FAILED,
            exception_location=exception_location,
            execution_context=execution_context,
            traceback_frames=traceback_frames,
        )

    def task_on_failure(self, context: AirflowCallbackContext) -> None:
        """
        Airflow callback for Task failures.

        Usage:
            task = PythonOperator(
                ..., on_failure_callback=client.airflow.v3.task_on_failure
            )

        Args:
            context: The Airflow context dictionary
                (airflow.sdk.definitions.context.Context).
        """
        self.client.logger.debug("Task failure callback triggered")
        self.client.logger.debug(f"Context keys: {list(context.keys())}")

        extracted = extract_context_values(context)
        error_message = extract_error_message(context)
        if error_message:
            self.client.logger.debug(
                f"Error message length: {len(error_message)} chars"
            )
            self.client.logger.debug(f"Error message preview: {error_message[:200]}...")
        else:
            self.client.logger.warning("No error message extracted from context")

        # Extract structured exception data for LLM consumption
        exception = context.get("exception")
        from .utils import extract_exception_location, extract_traceback_frames

        exception_location = extract_exception_location(exception)
        traceback_frames = extract_traceback_frames(exception)

        # Extract execution context from task_instance
        execution_context = {}
        ti = context.get("task_instance") or context.get("ti")
        if ti:
            if hasattr(ti, "state"):
                execution_context["state"] = ti.state
            if hasattr(ti, "hostname"):
                execution_context["hostname"] = ti.hostname
            if hasattr(ti, "duration"):
                execution_context["duration"] = ti.duration
            if hasattr(ti, "start_date"):
                execution_context["start_date"] = str(ti.start_date)
            if hasattr(ti, "log_url"):
                execution_context["log_url"] = ti.log_url

        self._report_error(
            extracted,
            error_message,
            AirflowEventType.TASK_INSTANCE_FAILED,
            exception_location=exception_location,
            execution_context=execution_context,
            traceback_frames=traceback_frames,
        )

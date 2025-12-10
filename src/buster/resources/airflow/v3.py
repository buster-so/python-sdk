from typing import Any, Dict, List, Optional, Union, cast

from pydantic import BaseModel, Field, ValidationError

from buster.types import (
    AirflowCallbackContext,
    AirflowEventsPayload,
    AirflowEventType,
    AirflowReportConfig,
    ApiVersion,
    DagConfig,
    DagRun,
    DataInterval,
    Environment,
    ExceptionLocation,
    RetryConfig,
    RuntimeTaskInstance,
    TaskDependencies,
    TaskInstanceState,
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
    end_date: Optional[str] = None
    log_url: Optional[str] = None

    # DAG run specific fields (from plugin_dag_on_failure)
    run_type: Optional[str] = None
    logical_date: Optional[str] = None
    queued_at: Optional[str] = None
    conf: Optional[Dict[str, Any]] = None

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
                "end_date": self.end_date,
                "log_url": self.log_url,
                "run_type": self.run_type,
                "logical_date": self.logical_date,
                "queued_at": self.queued_at,
                "conf": self.conf,
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
        self.client.logger.debug(f"Event details: try_number={try_number}, max_tries={max_tries}")

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
                self.client.logger.info(f"⏭️  Skipping report (retries not exhausted): try {try_number}/{max_tries}")
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
                end_date=exec_ctx.get("end_date"),
                log_url=exec_ctx.get("log_url"),
                run_type=exec_ctx.get("run_type"),
                logical_date=exec_ctx.get("logical_date"),
                queued_at=exec_ctx.get("queued_at"),
                conf=exec_ctx.get("conf"),
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
            self.client.logger.debug(f"Sending request to: {url} (env={env}, api_version={api_version})")

            # Log the full payload for debugging
            self.client.logger.debug("=" * 80)
            self.client.logger.debug("FULL PAYLOAD BEING SENT:")
            self.client.logger.debug("=" * 80)
            for key, value in request_payload.items():
                if key == "error_message" and value:
                    self.client.logger.debug(f"{key}:")
                    self.client.logger.debug(f"{value}")
                    self.client.logger.debug(f"(error_message length: {len(str(value))} characters)")
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

            error_msg = "Invalid arguments provided to report_error:\n" + "\n".join(issues)
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
        self.client.logger.debug(f"Extracted: dag_id={extracted.dag_id}, run_id={extracted.run_id}")

        error_message = extract_error_message(context)
        if error_message:
            self.client.logger.debug(f"Error message length: {len(error_message)} chars")
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
            self.client.logger.debug(f"Error message length: {len(error_message)} chars")
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

    def plugin_task_on_failure(
        self,
        previous_state: TaskInstanceState,
        task_instance: RuntimeTaskInstance,
        error: Optional[Union[str, BaseException]],
    ) -> None:
        """
        Airflow plugin hook for task failures.

        This function is designed to be called from Airflow plugin listeners
        (specifically @hookimpl on_task_instance_failed). Unlike the standard
        task_on_failure callback which receives a full context dictionary, this
        function receives structured parameters from the plugin hook.

        Usage:
            # In your Airflow plugin:
            from buster import Client

            client = Client(env="development")

            @hookimpl
            def on_task_instance_failed(previous_state, task_instance, error):
                client.airflow.v3.plugin_task_on_failure(
                    previous_state=previous_state,
                    task_instance=task_instance,
                    error=error,
                )

        Args:
            previous_state: TaskInstanceState - The state the task was in before failing
            task_instance: RuntimeTaskInstance - The task instance object
            error: str | BaseException | None - The error that caused the failure

        Note:
            This function has access to limited context compared to task_on_failure:
            - Available: dag_id, run_id, task_id, try_number, max_tries, error details,
              execution context (state, hostname, duration, start_date, log_url)
            - Not available: operator, params, task_dependencies, dag_config, data_interval
            - Unique to plugin: previous_state field
        """
        self.client.logger.debug("Plugin task failure hook triggered")
        self.client.logger.debug(
            f"Task: {getattr(task_instance, 'dag_id', 'unknown')}."
            f"{getattr(task_instance, 'task_id', 'unknown')}, "
            f"run: {getattr(task_instance, 'run_id', 'unknown')}, "
            f"previous_state: {previous_state}"
        )

        # Manually construct ExtractedContext from task_instance attributes
        extracted = ExtractedContext(
            dag_id=getattr(task_instance, "dag_id", None),
            run_id=getattr(task_instance, "run_id", None),
            task_id=getattr(task_instance, "task_id", None),
            try_number=getattr(task_instance, "try_number", None),
            max_tries=getattr(task_instance, "max_tries", None),
            # Plugin hooks don't have access to these fields:
            operator=None,
            params=None,
            task_dependencies=None,
            retry_config=None,
            dag_config=None,
            data_interval=None,
        )

        # Extract error message and exception data from error parameter
        error_message: Optional[str] = None
        exception_location: Optional[ExceptionLocation] = None
        traceback_frames: Optional[List[TracebackFrame]] = None

        if error:
            if isinstance(error, str):
                # Error provided as string
                error_message = error
                self.client.logger.debug("Error provided as string")
            else:
                # Error is a BaseException - extract structured data
                exc_type = type(error).__name__
                exc_message = str(error)
                error_message = f"{exc_type}: {exc_message}"
                self.client.logger.debug(f"Error extracted from exception: {exc_type}")

                # Extract structured exception location and traceback
                from .utils import extract_exception_location, extract_traceback_frames

                exception_location = extract_exception_location(error)
                traceback_frames = extract_traceback_frames(error)
        else:
            self.client.logger.warning("No error provided to plugin hook")

        # Build execution context with previous_state and task_instance attributes
        execution_context = {
            "previous_state": str(previous_state),
        }

        # Extract additional execution details from task_instance if available
        if hasattr(task_instance, "state"):
            execution_context["state"] = str(task_instance.state)
        if hasattr(task_instance, "hostname"):
            execution_context["hostname"] = task_instance.hostname
        if hasattr(task_instance, "duration"):
            execution_context["duration"] = task_instance.duration
        if hasattr(task_instance, "start_date") and task_instance.start_date:
            execution_context["start_date"] = str(task_instance.start_date)
        if hasattr(task_instance, "log_url"):
            execution_context["log_url"] = task_instance.log_url

        # Call _report_error with extracted data
        self._report_error(
            extracted=extracted,
            error_message=error_message,
            event_type=AirflowEventType.TASK_INSTANCE_FAILED,
            exception_location=exception_location,
            execution_context=execution_context,
            traceback_frames=traceback_frames,
        )

    def plugin_dag_on_failure(
        self,
        dag_run: DagRun,
        msg: str,
    ) -> None:
        """
        Airflow plugin hook for DAG run failures.

        This function is designed to be called from Airflow plugin listeners
        (specifically @hookimpl on_dag_run_failed). Unlike the standard
        dag_on_failure callback which receives a full context dictionary, this
        function receives structured parameters from the plugin hook.

        Usage:
            # In your Airflow plugin:
            from buster import Client

            client = Client(env="development")

            @hookimpl
            def on_dag_run_failed(dag_run, msg):
                client.airflow.v3.plugin_dag_on_failure(
                    dag_run=dag_run,
                    msg=msg,
                )

        Args:
            dag_run: DagRun - The DAG run object that failed
            msg: str - Error message describing the failure

        Note:
            This function has access to limited context compared to dag_on_failure:
            - Available: dag_id, run_id, error_message (from msg), data_interval,
              execution context (state, start_date, end_date, duration, run_type,
              logical_date, queued_at, conf)
            - Not available: exception_location, traceback_frames (no exception object),
              task_id, operator, params, task_dependencies, dag_config (no DAG object)
            - Unique to plugin: run_type, logical_date, queued_at, conf fields
        """
        self.client.logger.debug("Plugin DAG failure hook triggered")
        self.client.logger.debug(
            f"DAG: {getattr(dag_run, 'dag_id', 'unknown')}, "
            f"run: {getattr(dag_run, 'run_id', 'unknown')}, "
            f"msg: {msg[:100] if msg else 'None'}"
        )

        # Extract data_interval from dag_run
        data_interval: Optional[DataInterval] = None
        if hasattr(dag_run, "data_interval_start") and dag_run.data_interval_start:
            interval: DataInterval = {}
            interval["start"] = str(dag_run.data_interval_start)
            if hasattr(dag_run, "data_interval_end") and dag_run.data_interval_end:
                interval["end"] = str(dag_run.data_interval_end)
            if interval:
                data_interval = interval

        # Manually construct ExtractedContext from dag_run attributes
        # All task-specific fields are None for DAG-level failures
        extracted = ExtractedContext(
            dag_id=getattr(dag_run, "dag_id", None),
            run_id=getattr(dag_run, "run_id", None),
            task_id=None,  # DAG-level failure, no specific task
            try_number=None,
            max_tries=None,
            operator=None,
            params=None,
            task_dependencies=None,
            retry_config=None,
            dag_config=None,  # No access to DAG object in plugin hook
            data_interval=data_interval,
        )

        # Extract error message from msg parameter
        error_message = msg if msg else "DAG run failed with no error message provided"

        # No exception object available from plugin hook
        exception_location: Optional[ExceptionLocation] = None
        traceback_frames: Optional[List[TracebackFrame]] = None

        # Build execution context with dag_run attributes
        execution_context: Dict[str, Any] = {}

        # State and timing
        if hasattr(dag_run, "state"):
            execution_context["state"] = str(dag_run.state)
        if hasattr(dag_run, "start_date") and dag_run.start_date:
            execution_context["start_date"] = str(dag_run.start_date)
        if hasattr(dag_run, "end_date") and dag_run.end_date:
            execution_context["end_date"] = str(dag_run.end_date)

        # Calculate duration
        if hasattr(dag_run, "start_date") and hasattr(dag_run, "end_date"):
            if dag_run.start_date and dag_run.end_date:
                duration = (dag_run.end_date - dag_run.start_date).total_seconds()
                execution_context["duration"] = duration

        # Run metadata (unique to DAG run plugin hook)
        if hasattr(dag_run, "run_type"):
            execution_context["run_type"] = str(dag_run.run_type)
        if hasattr(dag_run, "logical_date") and dag_run.logical_date:
            execution_context["logical_date"] = str(dag_run.logical_date)
        if hasattr(dag_run, "queued_at") and dag_run.queued_at:
            execution_context["queued_at"] = str(dag_run.queued_at)

        # DAG run configuration
        if hasattr(dag_run, "conf") and dag_run.conf:
            try:
                import json

                json.dumps(dag_run.conf)  # Validate serializable
                execution_context["conf"] = dag_run.conf
            except (TypeError, ValueError):
                execution_context["conf"] = str(dag_run.conf)

        self.client.logger.debug(f"Execution context fields: {list(execution_context.keys())}")

        # Call _report_error with extracted data
        self._report_error(
            extracted=extracted,
            error_message=error_message,
            event_type=AirflowEventType.DAG_RUN_FAILED,
            exception_location=exception_location,
            execution_context=execution_context,
            traceback_frames=traceback_frames,
        )

"""
Base implementation for Airflow callback handlers.

This module contains the shared implementation used by both Airflow 2.11
and Airflow 3.x handlers.
"""

from typing import Any, Dict, Optional, Union, cast

from pydantic import ValidationError

from buster.types import (
    AirflowCallbackContext,
    AirflowEventTriggerType,
    AirflowEventType,
    AirflowReportConfig,
    DagRun,
    TaskInstanceState,
)
from buster.utils import send_request

from .models import AirflowErrorEvent
from .utils import (
    get_airflow_events_url,
    serialize_airflow_context,
)


class AirflowBase:
    """
    Base class for Airflow callback handlers.

    Contains shared implementation logic used by both Airflow 2.11 and 3.x handlers.
    This class should not be instantiated directly - use AirflowV2_11 or AirflowV3.
    """

    def __init__(self, client, config: Optional[AirflowReportConfig] = None, version_label: str = "Airflow"):
        self.client = client
        self._config = config or {}
        self._version_label = version_label
        client.logger.debug(f"{version_label} handler initialized")

    def _report_error(
        self,
        context: Dict[str, Any],
        event_type: AirflowEventType,
        event_trigger_type: AirflowEventTriggerType,
    ) -> None:
        """
        Internal method to report an Airflow error event to Buster API.

        This method:
        1. Serializes the complete Airflow context
        2. Checks if retries are exhausted (if send_when_retries_exhausted is True)
        3. Validates and sends the API request

        Args:
            context: The complete Airflow callback context dictionary
            event_type: Type of event (TASK_ON_FAILURE, DAG_ON_FAILURE)
            event_trigger_type: Trigger type (DAG or PLUGIN)

        Raises:
            ValueError: If required fields are missing or invalid

        Returns:
            None
        """
        # Extract basic info for logging and retry logic
        dag_id = context.get("dag_id")
        run_id = context.get("run_id")
        task_id = context.get("task_id")

        # Extract from nested objects if not at top level
        if not dag_id or not run_id:
            dag_run = context.get("dag_run")
            if dag_run:
                dag_id = dag_id or getattr(dag_run, "dag_id", None)
                run_id = run_id or getattr(dag_run, "run_id", None)

        if not task_id:
            ti = context.get("task_instance") or context.get("ti")
            if ti:
                task_id = getattr(ti, "task_id", None)

        self.client.logger.info(
            f"📋 Reporting {event_type.value}: dag_id={dag_id}, run_id={run_id}"
            + (f", task_id={task_id}" if task_id else "")
        )

        # Extract retry information for filtering
        try_number: Optional[int] = context.get("try_number")
        max_tries: Optional[int] = context.get("max_tries")

        # Extract from task_instance if not at top level
        if try_number is None or max_tries is None:
            ti = context.get("task_instance") or context.get("ti")
            if ti:
                try_number = try_number or getattr(ti, "try_number", None)
                max_tries = max_tries or getattr(ti, "max_tries", None)

        self.client.logger.debug(f"Event details: try_number={try_number}, max_tries={max_tries}")

        # Extract values from config with defaults
        config = self._config
        send_when_retries_exhausted = config.get("send_when_retries_exhausted", True)

        # Use env and api_version from client (set at client level)
        env = self.client.env
        api_version = self.client.api_version

        # Logic to check if we should send the event based on retries
        if send_when_retries_exhausted and try_number is not None and max_tries is not None:
            if try_number < max_tries:
                self.client.logger.info(f"⏭️  Skipping report (retries not exhausted): try {try_number}/{max_tries}")
                return

        try:
            # Log the raw context (keys only, plus important error details)
            self.client.logger.debug("=" * 80)
            self.client.logger.debug("RAW CONTEXT (keys and types):")
            self.client.logger.debug("=" * 80)
            for key, value in context.items():
                if key == "error" and value:
                    # Log error details
                    if isinstance(value, BaseException):
                        self.client.logger.debug(f"  {key} (BaseException):")
                        self.client.logger.debug(f"    - Type: {type(value).__name__}")
                        self.client.logger.debug(f"    - Message: {str(value)}")
                        self.client.logger.debug(f"    - Has traceback: {value.__traceback__ is not None}")
                    else:
                        self.client.logger.debug(f"  {key}: {value}")
                elif key == "msg":
                    self.client.logger.debug(f"  {key}: {value}")
                elif key == "exception":
                    # Log exception if present
                    if isinstance(value, BaseException):
                        self.client.logger.debug(f"  {key} (BaseException): {type(value).__name__} - {str(value)}")
                    else:
                        self.client.logger.debug(f"  {key}: {value}")
                else:
                    self.client.logger.debug(f"  {key}: <{type(value).__name__}>")
            self.client.logger.debug("=" * 80)

            # Serialize the context to JSON-safe format
            self.client.logger.debug("Serializing context...")
            serialized_context = serialize_airflow_context(context)
            self.client.logger.debug(f"Serialized context with {len(serialized_context)} keys")

            # Log the serialized context details (abbreviated to avoid timeouts)
            self.client.logger.debug("=" * 80)
            self.client.logger.debug("SERIALIZED CONTEXT (keys only):")
            self.client.logger.debug("=" * 80)
            import json

            for key, value in serialized_context.items():
                if key == "error" and value:
                    # Log serialized error in full detail
                    self.client.logger.debug(f"  {key}:")
                    self.client.logger.debug(f"    {json.dumps(value, indent=6)}")
                elif key == "msg":
                    self.client.logger.debug(f"  {key}: {value}")
                elif key == "exception" and value:
                    # Log exception details
                    self.client.logger.debug(f"  {key}:")
                    self.client.logger.debug(f"    {json.dumps(value, indent=6)}")
                else:
                    # Just log the key and type, not the full value (to avoid timeout on large objects)
                    value_type = type(value).__name__ if hasattr(value, "__class__") else type(value)
                    self.client.logger.debug(f"  {key}: <{value_type}>")
            self.client.logger.debug("=" * 80)

            # Validate inputs by creating the model
            self.client.logger.debug("Validating event data...")
            event = AirflowErrorEvent(
                event_type=event_type,
                event_trigger_type=event_trigger_type,
                context=serialized_context,
                api_version=api_version,
                env=env,
            )
            self.client.logger.debug("Event validation successful")

            # Convert validated event to API payload format
            request_payload = event.to_payload()

            # Construct the URL
            url = get_airflow_events_url(env, api_version)
            self.client.logger.debug(f"Sending request to: {url} (env={env}, api_version={api_version})")

            # Log the payload metadata (not full payload to avoid timeout)
            self.client.logger.debug("=" * 80)
            self.client.logger.debug("PAYLOAD BEING SENT TO API:")
            self.client.logger.debug("=" * 80)
            self.client.logger.debug(f"event_type: {request_payload.get('event_type')}")
            self.client.logger.debug(f"event_trigger_type: {request_payload.get('event_trigger_type')}")
            self.client.logger.debug(f"airflow_version: {request_payload.get('airflow_version')}")
            self.client.logger.debug(f"context keys: {list(request_payload.get('context', {}).keys())}")

            # Calculate payload size
            payload_json = json.dumps(request_payload, default=str)
            payload_size_kb = len(payload_json) / 1024
            self.client.logger.debug(f"Payload size: {payload_size_kb:.2f} KB")
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
                field = str(err["loc"][0]) if err["loc"] else "root"
                msg = err["msg"]
                issues.append(f"- {field}: {msg}")

            error_msg = "Invalid arguments provided to report_error:\n" + "\n".join(issues)
            self.client.logger.error(f"❌ Validation error: {error_msg}")
            raise ValueError(error_msg) from e

    def dag_on_failure(self, context: AirflowCallbackContext) -> None:
        """
        Airflow callback for DAG failures.

        Args:
            context: The Airflow context dictionary.
        """
        self.client.logger.debug(f"DAG failure callback triggered ({self._version_label})")
        self.client.logger.debug(f"Context keys: {list(context.keys())}")

        # Send the entire context to the server
        self._report_error(context, AirflowEventType.DAG_ON_FAILURE, AirflowEventTriggerType.DAG)

    def task_on_failure(self, context: AirflowCallbackContext) -> None:
        """
        Airflow callback for Task failures.

        Args:
            context: The Airflow context dictionary.
        """
        self.client.logger.debug(f"Task failure callback triggered ({self._version_label})")
        self.client.logger.debug(f"Context keys: {list(context.keys())}")

        # Send the entire context to the server
        self._report_error(context, AirflowEventType.TASK_ON_FAILURE, AirflowEventTriggerType.DAG)

    def plugin_task_on_failure(
        self,
        previous_state: TaskInstanceState,
        task_instance: Any,
        error: Optional[Union[str, BaseException]],
    ) -> None:
        """
        Airflow plugin hook for task failures.

        Args:
            previous_state: TaskInstanceState - The state the task was in before failing
            task_instance: TaskInstance - The task instance object
            error: str | BaseException | None - The error that caused the failure
        """
        self.client.logger.debug(f"Plugin task failure hook triggered ({self._version_label})")
        self.client.logger.debug(
            f"Task: {getattr(task_instance, 'dag_id', 'unknown')}."
            f"{getattr(task_instance, 'task_id', 'unknown')}, "
            f"run: {getattr(task_instance, 'run_id', 'unknown')}, "
            f"previous_state: {previous_state}"
        )

        # Construct a context dictionary from the plugin hook parameters
        context: Dict[str, Any] = {
            "previous_state": str(previous_state),
            "task_instance": task_instance,
            "error": error,
        }

        # If error is an exception, also add a msg field with the error message
        if error and not isinstance(error, str):
            context["msg"] = f"{type(error).__name__}: {str(error)}"

        # Send the entire context to the server
        self._report_error(context, AirflowEventType.TASK_ON_FAILURE, AirflowEventTriggerType.PLUGIN)

    def plugin_dag_on_failure(
        self,
        dag_run: DagRun,
        msg: str,
    ) -> None:
        """
        Airflow plugin hook for DAG run failures.

        Args:
            dag_run: DagRun - The DAG run object that failed
            msg: str - Error message describing the failure
        """
        self.client.logger.debug(f"Plugin DAG failure hook triggered ({self._version_label})")
        self.client.logger.debug(
            f"DAG: {getattr(dag_run, 'dag_id', 'unknown')}, "
            f"run: {getattr(dag_run, 'run_id', 'unknown')}, "
            f"msg: {msg[:100] if msg else 'None'}"
        )

        # Construct a context dictionary from the plugin hook parameters
        context: Dict[str, Any] = {
            "dag_run": dag_run,
            "msg": msg,
        }

        # Send the entire context to the server
        self._report_error(context, AirflowEventType.DAG_ON_FAILURE, AirflowEventTriggerType.PLUGIN)

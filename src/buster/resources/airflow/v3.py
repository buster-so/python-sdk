from typing import Optional

from pydantic import BaseModel, ValidationError

from buster.types import (
    AirflowContext,
    AirflowEventsPayload,
    AirflowEventType,
    AirflowReportConfig,
    ApiVersion,
    Environment,
)
from buster.utils import send_request

from .utils import extract_error_message, get_airflow_v3_url


class AirflowErrorEvent(BaseModel):
    event_type: AirflowEventType = AirflowEventType.TASK_INSTANCE_FAILED
    dag_id: str
    run_id: str
    task_id: Optional[str] = None
    try_number: Optional[int] = None
    error_message: Optional[str] = None
    airflow_version: Optional[str] = None
    api_version: ApiVersion = ApiVersion.V2
    env: Environment = Environment.PRODUCTION


class AirflowV3:
    def __init__(self, client, config: Optional[AirflowReportConfig] = None):
        self.client = client
        self._config = config or {}
        client.logger.debug("AirflowV3 handler initialized")

    def _report_error(
        self,
        context: AirflowContext,
        error_message: Optional[str] = None,
        event_type: AirflowEventType = AirflowEventType.TASK_INSTANCE_FAILED,
    ):
        """
        Reports an Airflow error event to Buster.
        Validates inputs using Pydantic.
        """
        # Extract values from context
        dag_id = context.get("dag_id")
        run_id = context.get("run_id")
        task_id = context.get("task_id")
        try_number = context.get("try_number")
        max_tries = context.get("max_tries") or 0

        self.client.logger.info(
            f"📋 Reporting {event_type.value}: dag_id={dag_id}, run_id={run_id}"
            + (f", task_id={task_id}" if task_id else "")
        )
        self.client.logger.debug(
            f"Event details: try_number={try_number}, max_tries={max_tries}"
        )

        # Extract values from config with defaults
        config = self._config
        airflow_version = config.get("airflow_version")
        api_version = config.get("api_version", ApiVersion.V2)
        env = config.get("env", Environment.PRODUCTION)
        send_when_retries_exhausted = config.get("send_when_retries_exhausted", True)

        # Logic to check if we should send the event based on retries
        if send_when_retries_exhausted and try_number is not None:
            if try_number < max_tries:
                self.client.logger.info(
                    f"⏭️  Skipping report (retries not exhausted): "
                    f"try {try_number}/{max_tries}"
                )
                return None

        try:
            # Validate inputs by creating the model
            self.client.logger.debug("Validating event data...")
            event = AirflowErrorEvent(
                event_type=event_type,
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                try_number=try_number,
                error_message=error_message,
                airflow_version=airflow_version,
                api_version=api_version,
                env=env,
            )
            self.client.logger.debug("Event validation successful")

            # Construct the payload
            request_payload: AirflowEventsPayload = {
                "dag_id": event.dag_id,
                "run_id": event.run_id,
                "task_id": event.task_id,
                "try_number": event.try_number,
                "event": event.event_type.value,
                "error_message": event.error_message,
                "airflow_version": event.airflow_version,
            }

            # Construct the URL
            url = get_airflow_v3_url(env, api_version)
            self.client.logger.debug(
                f"Sending request to: {url} "
                f"(env={env.value}, api_version={api_version.value})"
            )

            # Send the request
            from typing import Any, Dict, cast

            response = send_request(
                url,
                cast(Dict[str, Any], request_payload),
                self.client._buster_api_key,
                self.client.logger,
            )

            self.client.logger.info("✓ Event reported successfully")
            return response

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

    def dag_on_failure(self, context: AirflowContext):
        """
        Airflow callback for DAG failures.

        Usage:
            dag = DAG(..., on_failure_callback=client.airflow.v3.dag_on_failure)

        Args:
            context: The Airflow context dictionary.
        """
        self.client.logger.debug("DAG failure callback triggered")
        error_message = extract_error_message(context)
        if error_message:
            self.client.logger.debug(f"Error message: {error_message}")

        return self._report_error(
            context, error_message, AirflowEventType.DAG_RUN_FAILED
        )

    def task_on_failure(self, context: AirflowContext):
        """
        Airflow callback for Task failures.

        Usage:
            task = PythonOperator(
                ..., on_failure_callback=client.airflow.v3.task_on_failure
            )

        Args:
            context: The Airflow context dictionary.
        """
        self.client.logger.debug("Task failure callback triggered")
        error_message = extract_error_message(context)
        if error_message:
            self.client.logger.debug(f"Error message: {error_message}")

        return self._report_error(
            context, error_message, AirflowEventType.TASK_INSTANCE_FAILED
        )

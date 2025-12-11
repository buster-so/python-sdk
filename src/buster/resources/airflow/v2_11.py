"""
Airflow 2.11 callback handler implementation.
"""

from typing import Any, Dict, Optional

from buster.types import AirflowReportConfig

from .base import AirflowBase


class AirflowV2_11(AirflowBase):
    """
    Airflow 2.11 callback handler for the Buster SDK.

    This class provides callback functions for Airflow 2.11 DAG and task failures.
    It supports both standard callbacks (attached to DAGs/tasks) and plugin hooks.

    Note: This implementation is compatible with Airflow 2.11.x. For Airflow 3.x,
    use AirflowV3 instead.

    Usage:
        from airflow import DAG
        from airflow.operators.python import PythonOperator
        from buster import Client

        client = Client()

        # DAG-level callback
        dag = DAG(
            dag_id="my_pipeline",
            ...,
            on_failure_callback=client.airflow.v2_11.dag_on_failure
        )

        # Task-level callback
        task = PythonOperator(
            task_id="my_task",
            python_callable=my_function,
            on_failure_callback=client.airflow.v2_11.task_on_failure
        )

        # Plugin integration (in Airflow plugin file)
        from airflow.plugins_manager import AirflowPlugin
        from airflow.listeners import hookimpl
        from airflow.utils.state import TaskInstanceState
        from airflow.models.taskinstance import TaskInstance
        from airflow.models.dagrun import DagRun

        @hookimpl
        def on_task_instance_failed(
            previous_state: TaskInstanceState,
            task_instance: TaskInstance,
            error: str | BaseException | None,
        ):
            client.airflow.v2_11.plugin_task_on_failure(
                previous_state=previous_state,
                task_instance=task_instance,
                error=error,
            )

        @hookimpl
        def on_dag_run_failed(dag_run: DagRun, msg: str):
            client.airflow.v2_11.plugin_dag_on_failure(
                dag_run=dag_run,
                msg=msg,
            )
    """

    # Keys to exclude from context due to size (Airflow 2.11 includes massive objects)
    EXCLUDED_CONTEXT_KEYS = {
        # Large objects (100MB+ in some cases)
        "conf",  # AirflowConfigParser (~100MB)
        "dag",  # DAG object (large, redundant with dag_run)
        "task",  # Task object (large, redundant with task_instance)
        "ti",  # TaskInstance object (large, redundant with task_instance)
        # Accessor objects (not needed, can be large)
        "macros",  # Python module (not needed)
        "var",  # Variables accessor (not needed, can be large)
        "conn",  # Connections accessor (not needed, can be large)
        # Event accessors (not needed)
        "outlet_events",
        "inlet_events",
        "triggering_dataset_events",
        # Mapping/expansion fields (not needed for debugging)
        "expanded_ti_count",
        "inlets",
        "outlets",
        "map_index_template",
        "params",
        # Date/time template variables (redundant with execution_date/logical_date)
        "ds",  # execution_date as YYYY-MM-DD
        "ds_nodash",  # execution_date as YYYYMMDD
        "ts",  # execution_date as ISO format
        "ts_nodash",  # execution_date as compact format
        "ts_nodash_with_tz",  # execution_date as compact format with timezone
        "tomorrow_ds",
        "tomorrow_ds_nodash",
        "yesterday_ds",
        "yesterday_ds_nodash",
        "next_ds",
        "next_ds_nodash",
        "prev_ds",
        "prev_ds_nodash",
        # Additional date helpers (redundant)
        "next_execution_date",
        "prev_execution_date",
        "prev_data_interval_start_success",
        "prev_data_interval_end_success",
        "prev_start_date_success",
        # Other template helpers (not needed)
        "test_mode",
        "templates_dict",
        "run_id_str",  # redundant with run_id
    }

    def __init__(self, client, config: Optional[AirflowReportConfig] = None):
        super().__init__(client, config, version_label="Airflow 2.11")

    def _filter_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter out large/unnecessary objects from Airflow 2.11 context.

        Airflow 2.11 includes massive objects like AirflowConfigParser that can
        result in 100MB+ payloads. We filter these out to keep payloads manageable.

        Nested object deduplication (e.g., task_instance.dag_run duplicating top-level
        dag_run) is handled automatically by the serialization layer in utils.py.
        """
        filtered = {}

        for key, value in context.items():
            if key not in self.EXCLUDED_CONTEXT_KEYS:
                filtered[key] = value
            else:
                self.client.logger.debug(f"Excluding large context key: {key}")

        return filtered

    def task_on_failure(self, context: Dict[str, Any]) -> None:
        """
        Task failure callback for Airflow 2.11 (with context filtering).

        This is the standard Airflow on_failure_callback that can be attached to tasks
        or specified in default_args.
        """
        from .types import AirflowEventTriggerType, AirflowEventType

        self.client.logger.debug("Task failure callback triggered (Airflow 2.11)")
        self.client.logger.debug(f"Context keys: {list(context.keys())}")

        # Filter out large objects before reporting
        filtered_context = self._filter_context(context)

        # Send error report
        self._report_error(filtered_context, AirflowEventType.TASK_ON_FAILURE, AirflowEventTriggerType.DAG)

    def dag_on_failure(self, context: Dict[str, Any]) -> None:
        """
        DAG failure callback for Airflow 2.11 (with context filtering).

        This is the standard Airflow on_failure_callback that can be attached to DAGs.
        """
        from .types import AirflowEventTriggerType, AirflowEventType

        self.client.logger.debug("DAG failure callback triggered (Airflow 2.11)")
        self.client.logger.debug(f"Context keys: {list(context.keys())}")

        # Filter out large objects before reporting
        filtered_context = self._filter_context(context)

        # Send error report
        self._report_error(filtered_context, AirflowEventType.DAG_ON_FAILURE, AirflowEventTriggerType.DAG)

"""
Airflow 3.x callback handler implementation.
"""

from typing import Optional

from buster.types import AirflowReportConfig

from .base import AirflowBase


class AirflowV3(AirflowBase):
    """
    Airflow 3.x callback handler for the Buster SDK.

    This class provides callback functions for Airflow 3.x DAG and task failures.
    It supports both standard callbacks (attached to DAGs/tasks) and plugin hooks.

    Note: This implementation is compatible with Airflow 3.x. For Airflow 2.11,
    use AirflowV2_11 instead.

    Usage:
        from airflow.sdk import DAG, task
        from buster import Client

        client = Client()

        # DAG-level callback
        dag = DAG(
            ...,
            on_failure_callback=client.airflow.v3.dag_on_failure
        )

        # Task-level callback (with decorator)
        @task(on_failure_callback=client.airflow.v3.task_on_failure)
        def my_task():
            pass

        # Plugin integration (in Airflow plugin file)
        from airflow.sdk.definitions.context import Context
        from airflow.listeners import hookimpl

        @hookimpl
        def on_task_instance_failed(previous_state, task_instance, error):
            client.airflow.v3.plugin_task_on_failure(
                previous_state=previous_state,
                task_instance=task_instance,
                error=error,
            )

        @hookimpl
        def on_dag_run_failed(dag_run, msg):
            client.airflow.v3.plugin_dag_on_failure(
                dag_run=dag_run,
                msg=msg,
            )
    """

    def __init__(self, client, config: Optional[AirflowReportConfig] = None):
        super().__init__(client, config, version_label="Airflow 3")

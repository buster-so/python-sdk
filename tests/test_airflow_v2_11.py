"""
Tests for Airflow 2.11 integration (v2_11.py).

These tests verify that the AirflowV2_11 class correctly handles
Airflow 2.11 callbacks and plugin hooks.
"""

from buster import Client


def test_airflow_v2_11_resource_initialization():
    """
    Verifies that the Airflow 2.11 resource is correctly initialized on the Client.
    """
    # Test without config
    client = Client(buster_api_key="test-key")
    assert client.airflow is not None
    assert client.airflow.v2_11 is not None
    assert client.airflow.v2_11.client == client
    assert client.airflow.v2_11._config == {}

    # Test with config
    from buster.types import AirflowReportConfig

    config: AirflowReportConfig = {"send_when_retries_exhausted": False}
    client_with_config = Client(buster_api_key="test-key", airflow_config=config)
    assert client_with_config.airflow.v2_11._config == config


def test_airflow_v2_11_report_error(capsys, monkeypatch):
    """
    Verifies that report_error accepts arguments and calls send_request with
    expected data for Airflow 2.11.
    """
    client = Client(buster_api_key="test-key")

    # Mock send_request
    mock_response = {"success": True}

    def mock_send_request(url, payload, api_key, logger=None):
        assert "api2.buster.so/api/v2/public/airflow-events" in url
        assert payload["event_type"] == "dag_on_failure"
        assert payload["event_trigger_type"] == "dag"
        assert api_key == "test-key"
        # Verify context contains the data
        assert "context" in payload
        assert payload["context"]["dag_id"] == "test_dag"
        assert payload["context"]["run_id"] == "run_123"
        return mock_response

    import buster.resources.airflow.base as base_module

    monkeypatch.setattr(base_module, "send_request", mock_send_request)

    # Use dag_on_failure which calls _report_error internally
    client.airflow.v2_11.dag_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "exception": Exception("Something went wrong"),
        }
    )


def test_airflow_v2_11_with_execution_date(monkeypatch):
    """
    Verifies that execution_date is properly serialized (2.11 specific field).
    """
    from datetime import datetime

    import buster.resources.airflow.base as base_module

    client = Client(buster_api_key="test-key")

    def mock_send_request(url, payload, api_key, logger=None):
        # Both execution_date and logical_date should be in context
        assert "execution_date" in payload["context"]
        assert "logical_date" in payload["context"]
        # datetime objects are serialized to ISO format
        assert "2024-01-01" in payload["context"]["execution_date"]
        assert "2024-01-01" in payload["context"]["logical_date"]
        return {"success": True}

    monkeypatch.setattr(base_module, "send_request", mock_send_request)

    client.airflow.v2_11.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "execution_date": datetime(2024, 1, 1, 0, 0, 0),
            "logical_date": datetime(2024, 1, 1, 0, 0, 0),
        }
    )


def test_airflow_v2_11_coexistence_with_v3():
    """
    Verifies that both v3 and v2_11 can coexist on the same client.
    """
    client = Client(buster_api_key="test-key")

    # Both should be accessible
    assert client.airflow.v3 is not None
    assert client.airflow.v2_11 is not None

    # They should be different objects
    assert client.airflow.v3 is not client.airflow.v2_11

    # Both should share the same client
    assert client.airflow.v3.client is client
    assert client.airflow.v2_11.client is client


def test_airflow_v2_11_task_on_failure(monkeypatch):
    """
    Verifies that task_on_failure works correctly for Airflow 2.11.
    """
    import buster.resources.airflow.base as base_module
    from buster.types import AirflowEventType

    client = Client(buster_api_key="test-key")

    def mock_send_request(url, payload, api_key, logger=None):
        assert payload["event_type"] == AirflowEventType.TASK_ON_FAILURE.value
        assert payload["event_type"] == "task_on_failure"
        assert payload["event_trigger_type"] == "dag"
        return {"success": True}

    monkeypatch.setattr(base_module, "send_request", mock_send_request)

    client.airflow.v2_11.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
        }
    )


def test_airflow_v2_11_dag_on_failure(monkeypatch):
    """
    Verifies that dag_on_failure works correctly for Airflow 2.11.
    """
    import buster.resources.airflow.base as base_module
    from buster.types import AirflowEventType

    client = Client(buster_api_key="test-key")

    def mock_send_request(url, payload, api_key, logger=None):
        assert payload["event_type"] == AirflowEventType.DAG_ON_FAILURE.value
        assert payload["event_type"] == "dag_on_failure"
        assert payload["event_trigger_type"] == "dag"
        return {"success": True}

    monkeypatch.setattr(base_module, "send_request", mock_send_request)

    client.airflow.v2_11.dag_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
        }
    )


def test_airflow_v2_11_plugin_task_on_failure(monkeypatch):
    """
    Verifies plugin_task_on_failure works correctly for Airflow 2.11.
    """
    import buster.resources.airflow.base as base_module

    client = Client(buster_api_key="test-key")

    class MockTaskInstance:
        dag_id = "test_dag"
        run_id = "run_123"
        task_id = "test_task"
        try_number = 1
        max_tries = 3

    class MockState:
        def __str__(self):
            return "running"

    def mock_send_request(url, payload, api_key, logger=None):
        assert payload["event_type"] == "task_on_failure"
        assert payload["event_trigger_type"] == "plugin"
        # Check context contains serialized data
        assert "context" in payload
        assert payload["context"]["previous_state"] == "running"
        # task_instance should be serialized
        assert "task_instance" in payload["context"]
        ti_data = payload["context"]["task_instance"]
        assert ti_data["dag_id"] == "test_dag"
        assert ti_data["run_id"] == "run_123"
        assert ti_data["task_id"] == "test_task"
        return {"success": True}

    monkeypatch.setattr(base_module, "send_request", mock_send_request)

    client.airflow.v2_11.plugin_task_on_failure(
        previous_state=MockState(),
        task_instance=MockTaskInstance(),
        error=ValueError("Test error"),
    )


def test_airflow_v2_11_plugin_dag_on_failure(monkeypatch):
    """
    Verifies plugin_dag_on_failure works correctly for Airflow 2.11.
    """
    import buster.resources.airflow.base as base_module

    client = Client(buster_api_key="test-key")

    class MockDagRun:
        def __init__(self):
            self.dag_id = "test_dag"
            self.run_id = "run_123"
            self.state = "failed"

    def mock_send_request(url, payload, api_key, logger=None):
        assert payload["event_type"] == "dag_on_failure"
        assert payload["event_trigger_type"] == "plugin"
        # Check context contains serialized data
        assert "context" in payload
        assert "dag_run" in payload["context"]
        dag_run_data = payload["context"]["dag_run"]
        assert dag_run_data["dag_id"] == "test_dag"
        assert dag_run_data["run_id"] == "run_123"
        assert dag_run_data["state"] == "failed"
        # msg should be in context
        assert payload["context"]["msg"] == "Test DAG failure message"
        return {"success": True}

    monkeypatch.setattr(base_module, "send_request", mock_send_request)

    client.airflow.v2_11.plugin_dag_on_failure(
        dag_run=MockDagRun(),
        msg="Test DAG failure message",
    )


def test_airflow_v2_11_retry_logic(monkeypatch):
    """
    Verifies that retry logic works correctly for Airflow 2.11.
    """
    import buster.resources.airflow.base as base_module

    # Config with send_when_retries_exhausted=True
    client = Client(
        buster_api_key="test-key",
        airflow_config={"send_when_retries_exhausted": True},
    )

    send_request_called = []

    def mock_send_request(url, payload, api_key, logger=None):
        send_request_called.append(True)
        return {"success": True}

    monkeypatch.setattr(base_module, "send_request", mock_send_request)

    # max_tries=3, try_number=1 -> should skip (retries not exhausted)
    client.airflow.v2_11.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "try_number": 1,
            "max_tries": 3,
        }
    )

    # Should NOT have called send_request
    assert len(send_request_called) == 0

    # max_tries=3, try_number=3 -> should send (retries exhausted)
    client.airflow.v2_11.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "try_number": 3,
            "max_tries": 3,
        }
    )

    # Should have called send_request once
    assert len(send_request_called) == 1


def test_airflow_v2_11_exception_serialization(monkeypatch):
    """
    Verifies that exceptions are properly serialized for Airflow 2.11.
    """
    import buster.resources.airflow.base as base_module

    client = Client(buster_api_key="test-key")

    mock_exception_msg = "Test exception for Airflow 2.11"

    def mock_send_request(url, payload, api_key, logger=None):
        # Exception should be serialized in context
        assert "context" in payload
        assert "exception" in payload["context"]
        exception_data = payload["context"]["exception"]
        assert exception_data["_type"] == "exception"
        assert exception_data["exception_type"] == "Exception"
        assert exception_data["exception_message"] == mock_exception_msg
        assert "traceback" in exception_data
        return {"success": True}

    monkeypatch.setattr(base_module, "send_request", mock_send_request)

    client.airflow.v2_11.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "exception": Exception(mock_exception_msg),
        }
    )


def test_airflow_v2_11_type_aliases():
    """
    Verifies that Airflow 2.11 type aliases are exported correctly.
    """
    from buster.types import (
        Airflow2_11DagFailureCallback,
        Airflow2_11PluginDagFailureCallback,
        Airflow2_11PluginTaskFailureCallback,
        Airflow2_11TaskFailureCallback,
        AirflowDagFailureCallback,
        AirflowPluginDagFailureCallback,
        AirflowPluginTaskFailureCallback,
        AirflowTaskFailureCallback,
    )

    # Verify that the aliases point to the same types
    assert Airflow2_11TaskFailureCallback is AirflowTaskFailureCallback
    assert Airflow2_11DagFailureCallback is AirflowDagFailureCallback
    assert Airflow2_11PluginTaskFailureCallback is AirflowPluginTaskFailureCallback
    assert Airflow2_11PluginDagFailureCallback is AirflowPluginDagFailureCallback


def test_airflow_v2_11_nested_object_deduplication(monkeypatch):
    """
    Verifies that nested duplicate objects are replaced with references to reduce payload size.
    """
    import buster.resources.airflow.base as base_module

    client = Client(buster_api_key="test-key")

    # Create mock objects that would normally be duplicated
    class MockDagRun:
        def __init__(self):
            self.dag_id = "test_dag"
            self.run_id = "run_123"
            self.state = "failed"

    class MockTaskInstance:
        def __init__(self, dag_run):
            self.dag_id = "test_dag"
            self.run_id = "run_123"
            self.task_id = "test_task"
            self.dag_run = dag_run  # This would normally duplicate the entire dag_run

    dag_run = MockDagRun()
    task_instance = MockTaskInstance(dag_run)

    def mock_send_request(url, payload, api_key, logger=None):
        # Verify that the nested dag_run in task_instance is replaced with a reference
        assert "context" in payload
        assert "dag_run" in payload["context"]
        assert "task_instance" in payload["context"]

        # The top-level dag_run should be fully serialized
        dag_run_data = payload["context"]["dag_run"]
        assert dag_run_data["_type"] == "object"
        assert dag_run_data["dag_id"] == "test_dag"
        assert dag_run_data["run_id"] == "run_123"

        # The nested dag_run inside task_instance should be a reference
        task_instance_data = payload["context"]["task_instance"]
        assert task_instance_data["_type"] == "object"
        assert task_instance_data["dag_id"] == "test_dag"
        # The nested dag_run should be replaced with a reference string
        assert isinstance(task_instance_data["dag_run"], str)
        assert "reference to top-level" in task_instance_data["dag_run"]

        return {"success": True}

    monkeypatch.setattr(base_module, "send_request", mock_send_request)

    client.airflow.v2_11.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "dag_run": dag_run,
            "task_instance": task_instance,
        }
    )


def test_airflow_v2_11_excluded_template_variables(monkeypatch):
    """
    Verifies that template variables are excluded from the context.
    """
    import buster.resources.airflow.base as base_module

    client = Client(buster_api_key="test-key")

    def mock_send_request(url, payload, api_key, logger=None):
        # Verify that template variables are not in the context
        context = payload["context"]
        excluded_keys = [
            "test_mode",
            "tomorrow_ds",
            "tomorrow_ds_nodash",
            "ts",
            "ts_nodash",
            "ts_nodash_with_tz",
            "yesterday_ds",
            "yesterday_ds_nodash",
            "templates_dict",
        ]
        for key in excluded_keys:
            assert key not in context, f"Key '{key}' should have been excluded but was present"

        # Verify that important keys are still present
        assert "dag_id" in context
        assert "run_id" in context
        assert "task_id" in context

        return {"success": True}

    monkeypatch.setattr(base_module, "send_request", mock_send_request)

    client.airflow.v2_11.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            # Include all the template variables that should be filtered out
            "test_mode": False,
            "tomorrow_ds": "2025-12-12",
            "tomorrow_ds_nodash": "20251212",
            "ts": "2025-12-11T22:55:42.212691+00:00",
            "ts_nodash": "20251211T225542",
            "ts_nodash_with_tz": "20251211T225542.212691+0000",
            "yesterday_ds": "2025-12-10",
            "yesterday_ds_nodash": "20251210",
            "templates_dict": None,
        }
    )

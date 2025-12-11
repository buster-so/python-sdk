import pytest

from buster import Client


def test_airflow_resource_initialization():
    """
    Verifies that the Airflow resource is correctly initialized on the Client.
    """
    # Test without config
    client = Client(buster_api_key="test-key")
    assert client.airflow is not None
    assert client.airflow.v3 is not None
    assert client.airflow.v3.client == client
    assert client.airflow.v3._config == {}

    # Test with config
    from buster.types import AirflowReportConfig

    config: AirflowReportConfig = {"airflow_version": "2.5.0"}
    client_with_config = Client(buster_api_key="test-key", airflow_config=config)
    assert client_with_config.airflow.v3._config == config


def test_airflow_report_error(capsys, monkeypatch):
    """
    Verifies that report_error accepts arguments and calls send_request with
    expected data.
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

    import buster.resources.airflow.v3 as v3_module

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    # Use dag_on_failure which calls _report_error internally
    client.airflow.v3.dag_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "exception": Exception("Something went wrong"),
        }
    )


def test_airflow_validation_error():
    """
    Verifies that the context can be any dict - no field-level validation.
    The new structure just serializes whatever context is provided.
    """
    client = Client(buster_api_key="test-key")

    import buster.resources.airflow.v3 as v3_module

    def mock_send_request(url, payload, api_key, logger=None):
        # Should successfully send even with minimal context
        assert payload["event_type"] == "dag_on_failure"
        assert "context" in payload
        assert payload["context"]["run_id"] == "run_123"
        return {"success": True}

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    # Should work fine with minimal context - we serialize everything
    client.airflow.v3.dag_on_failure(context={"run_id": "run_123"})

    monkeypatch.undo()


def test_airflow_report_error_with_api_version(monkeypatch):
    """
    Verifies that report_error accepts api_version argument via client parameter.
    """
    import buster.resources.airflow.v3 as v3_module

    # Pass api_version as client parameter
    client = Client(buster_api_key="test-key", api_version="v2")

    # Mock
    def mock_send_request(url, payload, api_key, logger=None):
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    # Use dag_on_failure
    client.airflow.v3.dag_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
        }
    )
    # If no exception, it passed validation and mock call


def test_airflow_report_error_with_env(monkeypatch):
    """
    Verifies that report_error accepts env argument via client parameter.
    """
    import buster.resources.airflow.v3 as v3_module

    # Pass env as client parameter
    client = Client(buster_api_key="test-key", env="staging")

    # Mock
    def mock_send_request(url, payload, api_key, logger=None):
        assert "staging" in url
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.dag_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
        }
    )


def test_airflow_version_auto_detection(monkeypatch):
    """
    Verifies that airflow_version is auto-detected when not provided in config.
    If Airflow is not installed, it defaults to "3.1".
    """
    import buster.resources.airflow.v3 as v3_module
    from buster.resources.airflow.utils import get_airflow_version

    # Test the auto-detection function directly
    detected_version = get_airflow_version()
    # Should return either the actual Airflow version or "3.1" as default
    assert isinstance(detected_version, str)
    assert len(detected_version) > 0

    # Test that client uses auto-detected version when config doesn't specify one
    client = Client(buster_api_key="test-key")

    # Mock
    def mock_send_request(url, payload, api_key, logger=None):
        # Should have an airflow_version (either detected or default "3.1")
        assert "airflow_version" in payload, "airflow_version must be in payload"
        assert payload["airflow_version"] is not None, "airflow_version must not be None"
        assert isinstance(payload["airflow_version"], str), "airflow_version must be a string"
        assert len(payload["airflow_version"]) > 0, "airflow_version must not be empty"
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.dag_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
        }
    )


def test_airflow_payload_includes_none_values(monkeypatch):
    """
    Verifies that None values in context are serialized properly.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Mock
    def mock_send_request(url, payload, api_key, logger=None):
        # airflow_version should always be present (auto-detected to "3.1")
        assert "airflow_version" in payload
        assert payload["airflow_version"] == "3.1"

        # Context should contain the serialized fields
        assert "context" in payload
        assert payload["context"]["params"] is None, "params should be None"
        assert payload["context"]["duration"] is None, "duration should be None"
        assert payload["context"]["hostname"] is None, "hostname should be None"

        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    # Call with context that has None values
    client.airflow.v3.dag_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "params": None,
            "duration": None,
            "hostname": None,
        }
    )


def test_airflow_report_error_skips_on_retries():
    """
    Verifies that report_error returns None when max_tries is not met.
    """
    # Config is on client now
    client = Client(buster_api_key="test-key", airflow_config={"send_when_retries_exhausted": True})

    # max_tries=3, try_number=1 -> should skip
    # This should not raise an error and should skip reporting
    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "try_number": 1,
            "max_tries": 3,
        }
    )


def test_airflow_report_error_sends_on_exhaustion(monkeypatch):
    """
    Verifies that report_error sends when max_tries is met.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key", airflow_config={"send_when_retries_exhausted": True})

    # Mock
    called = False

    def mock_send_request(url, payload, api_key, logger=None):
        nonlocal called
        called = True
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    # max_tries=3, try_number=3 -> should send
    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "try_number": 3,
            "max_tries": 3,
        }
    )
    assert called, "send_request should have been called"


def test_airflow_report_error_default_event_type(monkeypatch):
    """
    Verifies that task_on_failure uses TASK_ON_FAILURE event type.
    """
    import buster.resources.airflow.v3 as v3_module
    from buster.types import AirflowEventType

    client = Client(buster_api_key="test-key")

    # Mock
    def mock_send_request(url, payload, api_key, logger=None):
        assert payload["event_type"] == AirflowEventType.TASK_ON_FAILURE.value
        assert payload["event_type"] == "task_on_failure"
        assert payload["event_trigger_type"] == "dag"
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    # task_on_failure should use TASK_ON_FAILURE
    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
        }
    )


def test_dag_on_failure_serializes_exception(monkeypatch):
    """
    Verifies that dag_on_failure serializes exception in context.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    mock_exception_msg = "Test exception message"

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

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.dag_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "exception": Exception(mock_exception_msg),
        }
    )


def test_task_on_failure_serializes_exception(monkeypatch):
    """
    Verifies that task_on_failure serializes exception in context.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    mock_exception_msg = "Task failure exception"

    def mock_send_request(url, payload, api_key, logger=None):
        # Exception should be serialized in context
        assert "context" in payload
        assert "exception" in payload["context"]
        exception_data = payload["context"]["exception"]
        assert exception_data["_type"] == "exception"
        assert exception_data["exception_type"] == "Exception"
        assert exception_data["exception_message"] == mock_exception_msg
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "exception": Exception(mock_exception_msg),
        }
    )


def test_serializes_exception_with_traceback(monkeypatch):
    """
    Verifies that exceptions with tracebacks are properly serialized.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Create an exception with a traceback
    try:
        raise ValueError("Something went wrong in the task")
    except ValueError as e:
        exception_with_traceback = e

    # Mock task instance with log_url
    class MockTaskInstance:
        def __init__(self):
            self.log_url = "https://airflow.example.com/dags/test_dag/grid?task_id=test_task"

    def mock_send_request(url, payload, api_key, logger=None):
        # Verify exception is serialized with traceback
        exception_data = payload["context"]["exception"]
        assert exception_data["_type"] == "exception"
        assert exception_data["exception_type"] == "ValueError"
        assert exception_data["exception_message"] == "Something went wrong in the task"
        assert "traceback" in exception_data
        assert len(exception_data["traceback"]) > 0
        # Verify task_instance is serialized
        assert "task_instance" in payload["context"]
        task_instance_data = payload["context"]["task_instance"]
        assert task_instance_data["_type"] == "object"
        assert task_instance_data["log_url"] == "https://airflow.example.com/dags/test_dag/grid?task_id=test_task"
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "exception": exception_with_traceback,
            "task_instance": MockTaskInstance(),
        }
    )


def test_operator_serialization(monkeypatch):
    """
    Verifies that task objects are serialized in context.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Mock task with operator_name
    class MockTask:
        def __init__(self):
            self.operator_name = "PythonOperator"

    def mock_send_request(url, payload, api_key, logger=None):
        # Task should be serialized in context
        assert "task" in payload["context"]
        task_data = payload["context"]["task"]
        assert task_data["_type"] == "object"
        assert task_data["operator_name"] == "PythonOperator"
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "task": MockTask(),
        }
    )


def test_params_serialization(monkeypatch):
    """
    Verifies that task params are serialized in context.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    test_params = {"key1": "value1", "key2": 123}

    def mock_send_request(url, payload, api_key, logger=None):
        # Params should be in context
        assert "params" in payload["context"]
        assert payload["context"]["params"] == test_params
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "params": test_params,
        }
    )


def test_task_instance_serialization(monkeypatch):
    """
    Verifies that task_instance objects are properly serialized in context.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Mock task instance with execution details
    class MockTaskInstance:
        def __init__(self):
            self.state = "failed"
            self.hostname = "worker-1"
            self.duration = 5.5
            self.start_date = "2024-01-01 00:00:00"
            self.log_url = "https://airflow.example.com/logs"

    def mock_send_request(url, payload, api_key, logger=None):
        # task_instance should be serialized in context
        assert "task_instance" in payload["context"]
        ti_data = payload["context"]["task_instance"]
        assert ti_data["_type"] == "object"
        assert ti_data["state"] == "failed"
        assert ti_data["hostname"] == "worker-1"
        assert ti_data["duration"] == 5.5
        assert ti_data["start_date"] == "2024-01-01 00:00:00"
        assert ti_data["log_url"] == "https://airflow.example.com/logs"
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "task_instance": MockTaskInstance(),
        }
    )


def test_task_dependencies_serialization(monkeypatch):
    """
    Verifies that task objects with dependencies are serialized in context.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Mock task with dependencies
    class MockTask:
        def __init__(self):
            self.operator_name = "PythonOperator"
            self.upstream_task_ids = {"task_a", "task_b"}
            self.downstream_task_ids = {"task_d", "task_e"}

    def mock_send_request(url, payload, api_key, logger=None):
        # Task with dependencies should be serialized
        assert "task" in payload["context"]
        task_data = payload["context"]["task"]
        assert task_data["_type"] == "object"
        # Sets are converted to lists during serialization
        assert set(task_data["upstream_task_ids"]) == {"task_a", "task_b"}
        assert set(task_data["downstream_task_ids"]) == {"task_d", "task_e"}
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "task": MockTask(),
        }
    )


def test_retry_config_serialization(monkeypatch):
    """
    Verifies that task retry configuration is serialized in context.
    """
    from datetime import timedelta

    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Mock task with retry config
    class MockTask:
        def __init__(self):
            self.operator_name = "PythonOperator"
            self.retries = 3
            self.retry_delay = timedelta(minutes=5)
            self.retry_exponential_backoff = True
            self.max_retry_delay = timedelta(hours=1)

    def mock_send_request(url, payload, api_key, logger=None):
        # Task with retry config should be serialized
        assert "task" in payload["context"]
        task_data = payload["context"]["task"]
        assert task_data["retries"] == 3
        # timedelta is serialized as string
        assert "0:05:00" in task_data["retry_delay"]
        assert task_data["retry_exponential_backoff"] is True
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "task": MockTask(),
        }
    )


def test_dag_config_serialization(monkeypatch):
    """
    Verifies that DAG configuration is serialized in context.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Mock DAG with config
    class MockDAG:
        def __init__(self):
            self.schedule_interval = "0 0 * * *"
            self.description = "Test DAG description"
            self.catchup = False
            self.max_active_runs = 3

    def mock_send_request(url, payload, api_key, logger=None):
        # DAG should be serialized in context
        assert "dag" in payload["context"]
        dag_data = payload["context"]["dag"]
        assert dag_data["_type"] == "object"
        assert dag_data["schedule_interval"] == "0 0 * * *"
        assert dag_data["description"] == "Test DAG description"
        assert dag_data["catchup"] is False
        assert dag_data["max_active_runs"] == 3
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "dag": MockDAG(),
        }
    )


def test_data_interval_serialization(monkeypatch):
    """
    Verifies that dag_run with data interval is serialized in context.
    """
    from datetime import datetime

    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Mock dag_run with data interval
    class MockDagRun:
        def __init__(self):
            self.dag_id = "test_dag"
            self.run_id = "run_123"
            self.data_interval_start = datetime(2024, 1, 1, 0, 0, 0)
            self.data_interval_end = datetime(2024, 1, 2, 0, 0, 0)

    def mock_send_request(url, payload, api_key, logger=None):
        # dag_run should be serialized in context
        assert "dag_run" in payload["context"]
        dag_run_data = payload["context"]["dag_run"]
        assert dag_run_data["_type"] == "object"
        # datetime is serialized as ISO format
        assert "2024-01-01" in dag_run_data["data_interval_start"]
        assert "2024-01-02" in dag_run_data["data_interval_end"]
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "dag_run": MockDagRun(),
        }
    )


# ============================================================================
# Plugin Hook Tests - plugin_task_on_failure
# ============================================================================


def test_plugin_task_on_failure_basic(monkeypatch):
    """Verifies plugin_task_on_failure accepts structured parameters."""
    import buster.resources.airflow.v3 as v3_module

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

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_task_on_failure(
        previous_state=MockState(),
        task_instance=MockTaskInstance(),
        error=ValueError("Test error"),
    )


def test_plugin_task_on_failure_string_error(monkeypatch):
    """Verifies handling of string error parameter."""
    import buster.resources.airflow.v3 as v3_module

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
        # String error should be in context
        assert "context" in payload
        assert payload["context"]["error"] == "String error message"
        # msg field should also be set for string errors
        assert "msg" in payload["context"]
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_task_on_failure(
        previous_state=MockState(),
        task_instance=MockTaskInstance(),
        error="String error message",
    )


def test_plugin_task_on_failure_exception_error(monkeypatch):
    """Verifies serialization of exception with traceback."""
    import buster.resources.airflow.v3 as v3_module

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

    # Create exception with traceback
    try:
        raise ValueError("Test exception error")
    except ValueError as e:
        test_exception = e

    def mock_send_request(url, payload, api_key, logger=None):
        # Exception should be serialized in context
        assert "context" in payload
        assert "error" in payload["context"]
        error_data = payload["context"]["error"]
        assert error_data["_type"] == "exception"
        assert error_data["exception_type"] == "ValueError"
        assert error_data["exception_message"] == "Test exception error"
        # Should have traceback
        assert "traceback" in error_data
        # msg field should also be set
        assert "msg" in payload["context"]
        assert "ValueError: Test exception error" in payload["context"]["msg"]
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_task_on_failure(
        previous_state=MockState(),
        task_instance=MockTaskInstance(),
        error=test_exception,
    )


def test_plugin_task_on_failure_previous_state(monkeypatch):
    """Verifies previous_state is included in context."""
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    class MockTaskInstance:
        dag_id = "test_dag"
        run_id = "run_123"
        task_id = "test_task"
        try_number = 1
        max_tries = 3

    class MockState:
        def __str__(self):
            return "queued"

    def mock_send_request(url, payload, api_key, logger=None):
        # previous_state should be in context
        assert "context" in payload
        assert payload["context"]["previous_state"] == "queued"
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_task_on_failure(
        previous_state=MockState(),
        task_instance=MockTaskInstance(),
        error=ValueError("Test error"),
    )


def test_plugin_task_on_failure_execution_context(monkeypatch):
    """Verifies execution context fields are serialized from task_instance."""
    from datetime import datetime

    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    class MockTaskInstance:
        def __init__(self):
            self.dag_id = "test_dag"
            self.run_id = "run_123"
            self.task_id = "test_task"
            self.try_number = 1
            self.max_tries = 3
            self.state = "failed"
            self.hostname = "worker-1"
            self.duration = 42.5
            self.start_date = datetime(2025, 1, 1, 12, 0, 0)
            self.log_url = "http://airflow/logs/test_dag/test_task"

    class MockState:
        def __str__(self):
            return "running"

    def mock_send_request(url, payload, api_key, logger=None):
        # task_instance should be serialized in context with all fields
        assert "context" in payload
        assert "task_instance" in payload["context"]
        ti_data = payload["context"]["task_instance"]
        assert ti_data["state"] == "failed"
        assert ti_data["hostname"] == "worker-1"
        assert ti_data["duration"] == 42.5
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_task_on_failure(
        previous_state=MockState(),
        task_instance=MockTaskInstance(),
        error=ValueError("Test error"),
    )


def test_plugin_task_on_failure_none_error(monkeypatch):
    """Verifies handling of None error parameter."""
    import buster.resources.airflow.v3 as v3_module

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
        # Error should be None in context
        assert "context" in payload
        assert payload["context"]["error"] is None
        # No msg field since error is None
        assert "msg" not in payload["context"]
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_task_on_failure(
        previous_state=MockState(),
        task_instance=MockTaskInstance(),
        error=None,
    )


def test_plugin_task_on_failure_missing_attributes(monkeypatch):
    """Verifies graceful handling of missing task_instance attributes."""
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    class MinimalTaskInstance:
        def __init__(self):
            # Only provide required attributes
            self.dag_id = "test_dag"
            self.run_id = "run_123"
            self.task_id = "test_task"
            # Missing: try_number, max_tries, state, hostname, duration, start_date, log_url

    class MockState:
        def __str__(self):
            return "running"

    def mock_send_request(url, payload, api_key, logger=None):
        # task_instance should still be serialized
        assert "context" in payload
        assert "task_instance" in payload["context"]
        ti_data = payload["context"]["task_instance"]
        assert ti_data["dag_id"] == "test_dag"
        assert ti_data["run_id"] == "run_123"
        assert ti_data["task_id"] == "test_task"
        # The function should handle missing fields gracefully
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_task_on_failure(
        previous_state=MockState(),
        task_instance=MinimalTaskInstance(),
        error=ValueError("Test error"),
    )


def test_plugin_task_on_failure_retry_exhaustion(monkeypatch):
    """Verifies retry exhaustion logic respects config."""
    import buster.resources.airflow.v3 as v3_module

    # Create client with send_when_retries_exhausted=True
    client = Client(
        buster_api_key="test-key",
        airflow_config={"send_when_retries_exhausted": True},
    )

    class MockTaskInstance:
        dag_id = "test_dag"
        run_id = "run_123"
        task_id = "test_task"
        try_number = 2  # Has more retries
        max_tries = 5

    class MockState:
        def __str__(self):
            return "running"

    send_request_called = []

    def mock_send_request(url, payload, api_key, logger=None):
        send_request_called.append(True)
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_task_on_failure(
        previous_state=MockState(),
        task_instance=MockTaskInstance(),
        error=ValueError("Test error"),
    )

    # With send_when_retries_exhausted=True and retries not exhausted,
    # should NOT send the event
    assert len(send_request_called) == 0


# ============================================================================
# Plugin DAG On Failure Tests (plugin_dag_on_failure)
# ============================================================================


def test_plugin_dag_on_failure_basic(monkeypatch):
    """Verifies plugin_dag_on_failure accepts structured parameters."""
    import buster.resources.airflow.v3 as v3_module

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

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_dag_on_failure(
        dag_run=MockDagRun(),
        msg="Test DAG failure message",
    )


def test_plugin_dag_on_failure_with_msg(monkeypatch):
    """Verifies msg parameter is in context."""
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    class MockDagRun:
        dag_id = "test_dag"
        run_id = "run_123"

    expected_msg = "DAG failed due to task timeout"

    def mock_send_request(url, payload, api_key, logger=None):
        # msg should be in context
        assert "context" in payload
        assert payload["context"]["msg"] == expected_msg
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_dag_on_failure(
        dag_run=MockDagRun(),
        msg=expected_msg,
    )


def test_plugin_dag_on_failure_empty_msg(monkeypatch):
    """Verifies empty msg is serialized as-is."""
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    class MockDagRun:
        dag_id = "test_dag"
        run_id = "run_123"

    def mock_send_request(url, payload, api_key, logger=None):
        # Empty msg should be in context as empty string
        assert "context" in payload
        assert payload["context"]["msg"] == ""
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    # Test with empty string
    client.airflow.v3.plugin_dag_on_failure(
        dag_run=MockDagRun(),
        msg="",
    )


def test_plugin_dag_on_failure_execution_context(monkeypatch):
    """Verifies execution context fields are serialized."""
    from datetime import datetime

    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    class MockDagRun:
        def __init__(self):
            self.dag_id = "test_dag"
            self.run_id = "run_123"
            self.state = "failed"
            self.start_date = datetime(2024, 1, 1, 12, 0, 0)
            self.end_date = datetime(2024, 1, 1, 12, 10, 0)
            self.run_type = "scheduled"
            self.logical_date = datetime(2024, 1, 1, 11, 0, 0)
            self.queued_at = datetime(2024, 1, 1, 11, 55, 0)

    def mock_send_request(url, payload, api_key, logger=None):
        # dag_run should be serialized in context
        assert "context" in payload
        assert "dag_run" in payload["context"]
        dag_run_data = payload["context"]["dag_run"]
        assert dag_run_data["state"] == "failed"
        # datetime fields are serialized to ISO format
        assert "2024-01-01" in dag_run_data["start_date"]
        assert "2024-01-01" in dag_run_data["end_date"]
        assert dag_run_data["run_type"] == "scheduled"

        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_dag_on_failure(
        dag_run=MockDagRun(),
        msg="Test message",
    )


def test_plugin_dag_on_failure_with_conf(monkeypatch):
    """Verifies dag_run.conf is serialized in context."""
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    class MockDagRun:
        def __init__(self):
            self.dag_id = "test_dag"
            self.run_id = "run_123"
            self.conf = {"param1": "value1", "param2": 123}

    def mock_send_request(url, payload, api_key, logger=None):
        # dag_run should be serialized in context
        assert "context" in payload
        assert "dag_run" in payload["context"]
        dag_run_data = payload["context"]["dag_run"]
        assert "conf" in dag_run_data
        assert dag_run_data["conf"]["param1"] == "value1"
        assert dag_run_data["conf"]["param2"] == 123
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_dag_on_failure(
        dag_run=MockDagRun(),
        msg="Test message",
    )


def test_plugin_dag_on_failure_no_exception_data(monkeypatch):
    """Verifies that only string message is in context (no exception object)."""
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    class MockDagRun:
        dag_id = "test_dag"
        run_id = "run_123"

    def mock_send_request(url, payload, api_key, logger=None):
        # Plugin hook only receives string message, not exception object
        assert "context" in payload
        # msg should be present
        assert payload["context"]["msg"] == "Test failure"
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_dag_on_failure(
        dag_run=MockDagRun(),
        msg="Test failure",
    )


def test_plugin_dag_on_failure_data_interval(monkeypatch):
    """Verifies data_interval is serialized from dag_run."""
    from datetime import datetime

    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    interval_start = datetime(2024, 1, 1, 0, 0, 0)
    interval_end = datetime(2024, 1, 2, 0, 0, 0)

    class MockDagRun:
        def __init__(self):
            self.dag_id = "test_dag"
            self.run_id = "run_123"
            self.data_interval_start = interval_start
            self.data_interval_end = interval_end

    def mock_send_request(url, payload, api_key, logger=None):
        # dag_run should be serialized in context
        assert "context" in payload
        assert "dag_run" in payload["context"]
        dag_run_data = payload["context"]["dag_run"]
        # datetime fields are serialized to ISO format
        assert "2024-01-01" in dag_run_data["data_interval_start"]
        assert "2024-01-02" in dag_run_data["data_interval_end"]
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_dag_on_failure(
        dag_run=MockDagRun(),
        msg="Test message",
    )


def test_plugin_dag_on_failure_missing_attributes(monkeypatch):
    """Verifies graceful handling of missing attributes."""
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    class MinimalDagRun:
        def __init__(self):
            # Only required attributes
            self.dag_id = "test_dag"
            self.run_id = "run_123"
            # All optional attributes missing

    def mock_send_request(url, payload, api_key, logger=None):
        # Should still work with minimal attributes
        assert payload["event_type"] == "dag_on_failure"
        assert payload["event_trigger_type"] == "plugin"
        assert "context" in payload
        assert "dag_run" in payload["context"]
        dag_run_data = payload["context"]["dag_run"]
        assert dag_run_data["dag_id"] == "test_dag"
        assert dag_run_data["run_id"] == "run_123"

        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_dag_on_failure(
        dag_run=MinimalDagRun(),
        msg="Test message",
    )

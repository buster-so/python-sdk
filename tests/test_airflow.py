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
        assert payload["dag_id"] == "test_dag"
        assert payload["event"] == "dag_run_failed"
        assert api_key == "test-key"
        # Verify mapping
        assert "event_type" not in payload
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
    Verifies that report_error raises ValueError when required fields are missing.
    """
    client = Client(buster_api_key="test-key")

    # Missing dag_id (empty context)
    with pytest.raises(ValueError) as excinfo:
        client.airflow.v3.dag_on_failure(context={"run_id": "run_123"})

    # Check that the error message mentions the missing field in our friendly format
    error_str = str(excinfo.value)
    assert "Invalid arguments provided to report_error" in error_str
    assert "- dag_id: String should have at least 1 character" in error_str


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


def test_airflow_report_error_with_airflow_version(monkeypatch):
    """
    Verifies that report_error uses airflow_version from client config.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key", airflow_config={"airflow_version": "2.5.0"})

    # Mock
    def mock_send_request(url, payload, api_key, logger=None):
        assert payload["airflow_version"] == "2.5.0"
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
    Verifies that None values are included in the payload.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Mock
    def mock_send_request(url, payload, api_key, logger=None):
        # airflow_version should always be present (auto-detected to "3.1")
        assert "airflow_version" in payload
        assert payload["airflow_version"] == "3.1"

        # Optional fields that are None should be included in payload
        assert "params" in payload, "params should be in payload"
        assert payload["params"] is None, "params should be None"
        assert "duration" in payload, "duration should be in payload"
        assert payload["duration"] is None, "duration should be None"
        assert "hostname" in payload, "hostname should be in payload"
        assert payload["hostname"] is None, "hostname should be None"

        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    # Call with minimal context (no optional fields)
    client.airflow.v3.dag_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
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
    Verifies that task_on_failure uses TASK_INSTANCE_FAILED event type.
    """
    import buster.resources.airflow.v3 as v3_module
    from buster.types import AirflowEventType

    client = Client(buster_api_key="test-key")

    # Mock
    def mock_send_request(url, payload, api_key, logger=None):
        assert payload["event"] == AirflowEventType.TASK_INSTANCE_FAILED.value
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    # task_on_failure should use TASK_INSTANCE_FAILED
    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
        }
    )


def test_dag_on_failure_extracts_error(monkeypatch):
    """
    Verifies that dag_on_failure correctly extracts error message from context.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # 1. Test with exception - now includes exception type
    mock_exception_msg = "Test exception message"

    def mock_report_error(
        self,
        extracted,
        error_message,
        event_type,
        exception_location=None,
        execution_context=None,
        traceback_frames=None,
    ):
        # New format includes exception type
        assert error_message == f"Exception: {mock_exception_msg}"
        return {"success": True}

    monkeypatch.setattr(v3_module.AirflowV3, "_report_error", mock_report_error)

    client.airflow.v3.dag_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "exception": Exception(mock_exception_msg),
        }
    )

    # 2. Test with reason (used as fallback when no exception present)
    mock_reason = "Test reason"

    def mock_report_error_reason(
        self,
        extracted,
        error_message,
        event_type,
        exception_location=None,
        execution_context=None,
        traceback_frames=None,
    ):
        # Reason is now formatted as "Exception: {reason}" for consistency
        assert error_message == f"Exception: {mock_reason}"
        return {"success": True}

    monkeypatch.setattr(v3_module.AirflowV3, "_report_error", mock_report_error_reason)

    client.airflow.v3.dag_on_failure(context={"dag_id": "test_dag", "run_id": "run_123", "reason": mock_reason})


def test_task_on_failure_extracts_error(monkeypatch):
    """
    Verifies that task_on_failure correctly extracts error message from context.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    mock_exception_msg = "Task failure exception"

    def mock_report_error(
        self,
        extracted,
        error_message,
        event_type,
        exception_location=None,
        execution_context=None,
        traceback_frames=None,
    ):
        # New format includes exception type
        assert error_message == f"Exception: {mock_exception_msg}"
        return {"success": True}

    monkeypatch.setattr(v3_module.AirflowV3, "_report_error", mock_report_error)

    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "exception": Exception(mock_exception_msg),
        }
    )


def test_extract_error_with_traceback_and_log_url(monkeypatch):
    """
    Verifies that error extraction includes exception details and log URL.
    Traceback is sent separately in traceback_frames field.
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
        log_url = "https://airflow.example.com/dags/test_dag/grid?task_id=test_task"

    def mock_report_error(
        self,
        extracted,
        error_message,
        event_type,
        exception_location=None,
        execution_context=None,
        traceback_frames=None,
    ):
        # Verify error message includes exception type and message
        assert "ValueError: Something went wrong in the task" in error_message
        # Verify log URL is included in error_message
        assert "Logs: https://airflow.example.com" in error_message
        # Verify traceback is sent separately in traceback_frames
        assert traceback_frames is not None
        assert len(traceback_frames) > 0
        assert any("raise ValueError" in str(frame.get("code", "")) for frame in traceback_frames)
        return {"success": True}

    monkeypatch.setattr(v3_module.AirflowV3, "_report_error", mock_report_error)

    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "exception": exception_with_traceback,
            "task_instance": MockTaskInstance(),
        }
    )


def test_extract_exception_location():
    """
    Verifies that extract_exception_location extracts structured data from exceptions.
    """
    from buster.resources.airflow.utils import extract_exception_location

    # Test 1: Exception with traceback
    try:
        raise ValueError("Test error message")
    except ValueError as e:
        location = extract_exception_location(e)
        assert location["type"] == "ValueError"
        assert location["message"] == "Test error message"
        assert "file" in location
        assert "line" in location
        assert "function" in location
        assert location["function"] == "test_extract_exception_location"

    # Test 2: String exception (returns empty dict)
    location = extract_exception_location("string error")
    assert location == {}

    # Test 3: None exception (returns empty dict)
    location = extract_exception_location(None)
    assert location == {}

    # Test 4: Exception without traceback
    exception_no_tb = ValueError("No traceback")
    location = extract_exception_location(exception_no_tb)
    assert location["type"] == "ValueError"
    assert location["message"] == "No traceback"
    # Should have type and message but no file/line/function
    assert "file" not in location
    assert "line" not in location
    assert "function" not in location


def test_operator_extraction(monkeypatch):
    """
    Verifies that operator type is extracted and sent in payload.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Mock task with operator_name
    class MockTask:
        operator_name = "PythonOperator"

    def mock_send_request(url, payload, api_key, logger=None):
        assert payload["operator"] == "PythonOperator"
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


def test_params_extraction(monkeypatch):
    """
    Verifies that task params are extracted and sent in payload.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    test_params = {"key1": "value1", "key2": 123}

    def mock_send_request(url, payload, api_key, logger=None):
        assert payload["params"] == test_params
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


def test_exception_location_in_payload(monkeypatch):
    """
    Verifies that exception_location is extracted and sent in payload.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Create an exception with traceback
    try:
        raise ValueError("Test structured exception")
    except ValueError as e:
        test_exception = e

    def mock_send_request(url, payload, api_key, logger=None):
        assert "exception_location" in payload
        assert payload["exception_location"]["type"] == "ValueError"
        assert payload["exception_location"]["message"] == "Test structured exception"
        assert "file" in payload["exception_location"]
        assert "line" in payload["exception_location"]
        assert "function" in payload["exception_location"]
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "exception": test_exception,
        }
    )


def test_execution_context_in_payload(monkeypatch):
    """
    Verifies that execution context (state, hostname, etc.) is sent in payload.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Mock task instance with execution details
    class MockTaskInstance:
        state = "failed"
        hostname = "worker-1"
        duration = 5.5
        start_date = "2024-01-01 00:00:00"
        log_url = "https://airflow.example.com/logs"

    def mock_send_request(url, payload, api_key, logger=None):
        assert payload["state"] == "failed"
        assert payload["hostname"] == "worker-1"
        assert payload["duration"] == 5.5
        assert payload["start_date"] == "2024-01-01 00:00:00"
        assert payload["log_url"] == "https://airflow.example.com/logs"
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


def test_extract_traceback_frames():
    """
    Verifies that extract_traceback_frames extracts structured frames from exceptions.
    """
    from buster.resources.airflow.utils import extract_traceback_frames

    # Test 1: Exception with traceback
    try:

        def inner_function():
            raise ValueError("Test error in inner function")

        inner_function()
    except ValueError as e:
        frames = extract_traceback_frames(e)
        assert len(frames) >= 2  # At least outer and inner function
        assert all("file" in frame for frame in frames)
        assert all("line" in frame for frame in frames)
        assert all("function" in frame for frame in frames)
        # Check that last frame is the actual error location
        assert frames[-1]["function"] == "inner_function"

    # Test 2: String exception (returns empty list)
    frames = extract_traceback_frames("string error")
    assert frames == []

    # Test 3: None exception (returns empty list)
    frames = extract_traceback_frames(None)
    assert frames == []


def test_task_dependencies_in_payload(monkeypatch):
    """
    Verifies that task dependencies are extracted and sent in payload.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Mock task with dependencies
    class MockTask:
        operator_name = "PythonOperator"
        upstream_task_ids = {"task_a", "task_b"}
        downstream_task_ids = {"task_d", "task_e"}

    def mock_send_request(url, payload, api_key, logger=None):
        assert "task_dependencies" in payload
        assert set(payload["task_dependencies"]["upstream"]) == {"task_a", "task_b"}
        assert set(payload["task_dependencies"]["downstream"]) == {"task_d", "task_e"}
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


def test_retry_config_in_payload(monkeypatch):
    """
    Verifies that retry configuration is extracted and sent in payload.
    """
    from datetime import timedelta

    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Mock task with retry config
    class MockTask:
        operator_name = "PythonOperator"
        retries = 3
        retry_delay = timedelta(minutes=5)
        retry_exponential_backoff = True
        max_retry_delay = timedelta(hours=1)

    def mock_send_request(url, payload, api_key, logger=None):
        assert "retry_config" in payload
        assert payload["retry_config"]["retries"] == 3
        assert "0:05:00" in payload["retry_config"]["retry_delay"]
        assert payload["retry_config"]["retry_exponential_backoff"] is True
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


def test_dag_config_in_payload(monkeypatch):
    """
    Verifies that DAG configuration is extracted and sent in payload.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Mock DAG with config
    class MockDAG:
        schedule_interval = "0 0 * * *"
        description = "Test DAG description"
        catchup = False
        max_active_runs = 3

    def mock_send_request(url, payload, api_key, logger=None):
        assert "dag_config" in payload
        assert payload["dag_config"]["schedule_interval"] == "0 0 * * *"
        assert payload["dag_config"]["description"] == "Test DAG description"
        assert payload["dag_config"]["catchup"] is False
        assert payload["dag_config"]["max_active_runs"] == 3
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


def test_data_interval_in_payload(monkeypatch):
    """
    Verifies that data interval is extracted and sent in payload.
    """
    from datetime import datetime

    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Mock dag_run with data interval
    class MockDagRun:
        dag_id = "test_dag"
        run_id = "run_123"
        data_interval_start = datetime(2024, 1, 1, 0, 0, 0)
        data_interval_end = datetime(2024, 1, 2, 0, 0, 0)

    def mock_send_request(url, payload, api_key, logger=None):
        assert "data_interval" in payload
        assert "2024-01-01" in payload["data_interval"]["start"]
        assert "2024-01-02" in payload["data_interval"]["end"]
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


def test_traceback_frames_in_payload(monkeypatch):
    """
    Verifies that structured traceback frames are sent in payload.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # Create an exception with traceback
    try:
        raise ValueError("Test error with traceback")
    except ValueError as e:
        test_exception = e

    def mock_send_request(url, payload, api_key, logger=None):
        assert "traceback_frames" in payload
        assert isinstance(payload["traceback_frames"], list)
        assert len(payload["traceback_frames"]) > 0
        # Check frame structure
        frame = payload["traceback_frames"][0]
        assert "file" in frame
        assert "line" in frame
        assert "function" in frame
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "task_id": "test_task",
            "exception": test_exception,
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
        assert payload["dag_id"] == "test_dag"
        assert payload["run_id"] == "run_123"
        assert payload["task_id"] == "test_task"
        assert payload["event"] == "task_instance_failed"
        assert payload["try_number"] == 1
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
        assert payload["error_message"] == "String error message"
        # String errors don't have exception_location or traceback_frames
        assert "exception_location" not in payload or payload.get("exception_location") is None
        assert "traceback_frames" not in payload or payload.get("traceback_frames") is None
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_task_on_failure(
        previous_state=MockState(),
        task_instance=MockTaskInstance(),
        error="String error message",
    )


def test_plugin_task_on_failure_exception_error(monkeypatch):
    """Verifies extraction of exception location and traceback."""
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
        assert "ValueError: Test exception error" in payload["error_message"]
        # Should have exception_location
        assert "exception_location" in payload
        assert payload["exception_location"]["type"] == "ValueError"
        # Should have traceback_frames
        assert "traceback_frames" in payload
        assert isinstance(payload["traceback_frames"], list)
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_task_on_failure(
        previous_state=MockState(),
        task_instance=MockTaskInstance(),
        error=test_exception,
    )


def test_plugin_task_on_failure_previous_state(monkeypatch):
    """Verifies previous_state is included in execution_context."""
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
        # previous_state should be in the payload (either directly or in execution context)
        # The _report_error method will merge execution_context into the main payload
        # So we need to check if it's present anywhere in the payload
        assert "queued" in str(payload)
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client.airflow.v3.plugin_task_on_failure(
        previous_state=MockState(),
        task_instance=MockTaskInstance(),
        error=ValueError("Test error"),
    )


def test_plugin_task_on_failure_execution_context(monkeypatch):
    """Verifies execution context fields are extracted from task_instance."""
    import buster.resources.airflow.v3 as v3_module
    from datetime import datetime

    client = Client(buster_api_key="test-key")

    class MockTaskInstance:
        dag_id = "test_dag"
        run_id = "run_123"
        task_id = "test_task"
        try_number = 1
        max_tries = 3
        state = "failed"
        hostname = "worker-1"
        duration = 42.5
        start_date = datetime(2025, 1, 1, 12, 0, 0)
        log_url = "http://airflow/logs/test_dag/test_task"

    class MockState:
        def __str__(self):
            return "running"

    def mock_send_request(url, payload, api_key, logger=None):
        # Check that execution context fields are present
        payload_str = str(payload)
        assert "failed" in payload_str or payload.get("state") == "failed"
        assert "worker-1" in payload_str or payload.get("hostname") == "worker-1"
        assert 42.5 in str(payload) or payload.get("duration") == 42.5
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
        # Should not have error_message
        assert payload.get("error_message") is None
        assert "exception_location" not in payload or payload.get("exception_location") is None
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
        # Only provide required attributes
        dag_id = "test_dag"
        run_id = "run_123"
        task_id = "test_task"
        # Missing: try_number, max_tries, state, hostname, duration, start_date, log_url

    class MockState:
        def __str__(self):
            return "running"

    def mock_send_request(url, payload, api_key, logger=None):
        assert payload["dag_id"] == "test_dag"
        assert payload["run_id"] == "run_123"
        assert payload["task_id"] == "test_task"
        # Missing fields should be None or not present
        # The function should handle this gracefully without errors
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

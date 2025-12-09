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
    from buster.types import AirflowReportConfig, Environment

    config: AirflowReportConfig = {"env": Environment.STAGING}
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

    def mock_send_request(url, payload, api_key):
        assert "api2.buster.so/api/v2/public/airflow-events" in url
        assert payload["dag_id"] == "test_dag"
        assert payload["event"] == "dag_run_failed"
        assert api_key == "test-key"
        # Verify mapping
        assert "event_type" not in payload
        return mock_response

    import buster.resources.airflow.v3 as v3_module

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    # Use the versioned accessor
    from buster.types import AirflowEventType

    response = client.airflow.v3._report_error(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
        },
        event_type=AirflowEventType.DAG_RUN_FAILED,
        error_message="Something went wrong",
    )

    assert response == mock_response


def test_airflow_validation_error():
    """
    Verifies that report_error raises ValueError when required fields are missing.
    """
    client = Client(buster_api_key="test-key")

    # Missing dag_id (empty context)
    from buster.types import AirflowEventType

    with pytest.raises(ValueError) as excinfo:
        client.airflow.v3._report_error(
            context={"run_id": "run_123"},
            event_type=AirflowEventType.DAG_RUN_FAILED,
        )

    # Check that the error message mentions the missing field in our friendly format
    error_str = str(excinfo.value)
    assert "Invalid arguments provided to report_error" in error_str
    assert "- dag_id: Input should be a valid string" in error_str


def test_airflow_report_error_with_api_version(monkeypatch):
    """
    Verifies that report_error accepts api_version argument via client config.
    """
    import buster.resources.airflow.v3 as v3_module
    from buster.types import ApiVersion

    # Pass api_version in config
    client = Client(
        buster_api_key="test-key", airflow_config={"api_version": ApiVersion.V2}
    )

    # Mock
    def mock_send_request(url, payload, api_key):
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    # Pass api_version
    from buster.types import AirflowEventType

    client.airflow.v3._report_error(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
        },
        event_type=AirflowEventType.DAG_RUN_FAILED,
    )
    # If no exception, it passed validation and mock call


def test_airflow_report_error_with_env(monkeypatch):
    """
    Verifies that report_error accepts env argument via client config.
    """
    import buster.resources.airflow.v3 as v3_module
    from buster.types import Environment

    # Pass env in config
    client = Client(
        buster_api_key="test-key", airflow_config={"env": Environment.STAGING}
    )

    # Mock
    def mock_send_request(url, payload, api_key):
        assert "staging" in url
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    from buster.types import AirflowEventType

    client.airflow.v3._report_error(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
        },
        event_type=AirflowEventType.DAG_RUN_FAILED,
    )


def test_airflow_report_error_with_airflow_version(monkeypatch):
    """
    Verifies that report_error uses airflow_version from client config.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(
        buster_api_key="test-key", airflow_config={"airflow_version": "2.5.0"}
    )

    # Mock
    def mock_send_request(url, payload, api_key):
        assert payload["airflow_version"] == "2.5.0"
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    from buster.types import AirflowEventType

    client.airflow.v3._report_error(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
        },
        event_type=AirflowEventType.DAG_RUN_FAILED,
    )


def test_airflow_report_error_skips_on_retries():
    """
    Verifies that report_error returns None when max_tries is not met.
    """
    # Config is on client now
    client = Client(
        buster_api_key="test-key", airflow_config={"send_when_retries_exhausted": True}
    )

    # max_tries=3, try_number=1 -> should skip
    from buster.types import AirflowEventType

    result = client.airflow.v3._report_error(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "try_number": 1,
            "max_tries": 3,
        },
        event_type=AirflowEventType.DAG_RUN_FAILED,
    )
    assert result is None


def test_airflow_report_error_sends_on_exhaustion(monkeypatch):
    """
    Verifies that report_error sends when max_tries is met.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(
        buster_api_key="test-key", airflow_config={"send_when_retries_exhausted": True}
    )

    # Mock
    def mock_send_request(url, payload, api_key):
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    # max_tries=3, try_number=3 -> should send
    from buster.types import AirflowEventType

    result = client.airflow.v3._report_error(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "try_number": 3,
            "max_tries": 3,
        },
        event_type=AirflowEventType.DAG_RUN_FAILED,
    )
    assert result == {"success": True}


def test_airflow_report_error_default_event_type(monkeypatch):
    """
    Verifies that report_error uses default event_type if not provided.
    """
    import buster.resources.airflow.v3 as v3_module
    from buster.types import AirflowEventType

    client = Client(buster_api_key="test-key")

    # Mock
    def mock_send_request(url, payload, api_key):
        assert payload["event"] == AirflowEventType.TASK_INSTANCE_FAILED.value
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    # Do not pass event_type
    client.airflow.v3._report_error(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
        }
    )


def test_dag_on_failure_extracts_error(monkeypatch):
    """
    Verifies that dag_on_failure correctly extracts error message from context.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    # 1. Test with exception
    mock_exception_msg = "Test exception message"

    def mock_report_error(self, context, error_message, event_type):
        assert error_message == mock_exception_msg
        return {"success": True}

    monkeypatch.setattr(v3_module.AirflowV3, "_report_error", mock_report_error)

    client.airflow.v3.dag_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "exception": Exception(mock_exception_msg),
        }
    )

    # 2. Test with reason
    mock_reason = "Test reason"

    def mock_report_error_reason(self, context, error_message, event_type):
        assert error_message == mock_reason
        return {"success": True}

    monkeypatch.setattr(v3_module.AirflowV3, "_report_error", mock_report_error_reason)

    client.airflow.v3.dag_on_failure(
        context={"dag_id": "test_dag", "run_id": "run_123", "reason": mock_reason}
    )


def test_task_on_failure_extracts_error(monkeypatch):
    """
    Verifies that task_on_failure correctly extracts error message from context.
    """
    import buster.resources.airflow.v3 as v3_module

    client = Client(buster_api_key="test-key")

    mock_exception_msg = "Task failure exception"

    def mock_report_error(self, context, error_message, event_type):
        assert error_message == mock_exception_msg
        return {"success": True}

    monkeypatch.setattr(v3_module.AirflowV3, "_report_error", mock_report_error)

    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
            "exception": Exception(mock_exception_msg),
        }
    )

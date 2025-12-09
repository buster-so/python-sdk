from buster.types import Environment, ApiVersion
from buster.resources.airflow.utils import get_airflow_v3_url
from buster.utils import send_request
import pytest
import requests

def test_get_api_url_production():
    url = get_airflow_v3_url(Environment.PRODUCTION, ApiVersion.V2)
    assert url == "https://api2.buster.so/api/v2/public/airflow-events"

def test_get_api_url_development():
    url = get_airflow_v3_url(Environment.DEVELOPMENT, ApiVersion.V2)
    assert url == "http://localhost:3000/api/v2/public/airflow-events"

def test_get_api_url_staging():
    url = get_airflow_v3_url(Environment.STAGING, ApiVersion.V2)
    assert url == "https://api2.staging.buster.so/api/v2/public/airflow-events"

def test_send_request_timeout(monkeypatch):
    """
    Verifies that requests.post is called with timeout=30.
    """
    def mock_post(url, json, headers, timeout):
        assert timeout == 30
        assert headers["Accept"] == "application/json"
        class MockResponse:
            def raise_for_status(self): pass
            def json(self): return {"success": True}
        return MockResponse()

    monkeypatch.setattr(requests, "post", mock_post)
    send_request("http://test.com", {}, "test-key")

def test_send_request_optional_api_key(monkeypatch):
    """
    Verifies that send_request uses env var if api_key is not provided.
    """
    monkeypatch.setenv("BUSTER_API_KEY", "env-key")
    
    def mock_post(url, json, headers, timeout):
        assert headers["Authorization"] == "Bearer env-key"
        class MockResponse:
            def raise_for_status(self): pass
            def json(self): return {"success": True}
        return MockResponse()

    monkeypatch.setattr(requests, "post", mock_post)
    send_request("http://test.com", {})

def test_send_request_missing_api_key(monkeypatch):
    """
    Verifies that send_request raises ValueError if no key is found.
    """
    monkeypatch.delenv("BUSTER_API_KEY", raising=False)
    
    with pytest.raises(ValueError) as excinfo:
        send_request("http://test.com", {})
    assert "Buster API key must be provided" in str(excinfo.value)

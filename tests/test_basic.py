import pytest

from buster import Client


def test_client_init_with_param():
    """
    Verifies that the Client initializes correctly with a param.
    """
    client = Client(buster_api_key="test-key")
    assert client._buster_api_key == "test-key"


def test_client_init_with_env_var(monkeypatch):
    """
    Verifies Client picks up API key from environment variable.
    """
    monkeypatch.setenv("BUSTER_API_KEY", "env-key")
    # No arg passed
    client = Client()
    assert client._buster_api_key == "env-key"


def test_client_init_failure_missing_key(monkeypatch):
    """
    Verifies Client raises ValueError when no key is provided (neither param nor env).
    """
    # Ensure env var is unset
    monkeypatch.delenv("BUSTER_API_KEY", raising=False)

    with pytest.raises(ValueError) as excinfo:
        Client()

    assert "Buster API key must be provided" in str(excinfo.value)


def test_client_init_failure_empty_key(monkeypatch):
    """
    Verifies Client raises ValueError when an empty string is provided,
    even if it's passed explicitly.
    """
    # Ensure env var is unset
    monkeypatch.delenv("BUSTER_API_KEY", raising=False)

    with pytest.raises(ValueError) as excinfo:
        Client(buster_api_key="")

    assert "Buster API key must be provided" in str(excinfo.value)

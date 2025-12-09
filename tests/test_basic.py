from buster import Client
import pytest

def test_client_hello():
    """
    Verifies that the Client hello method returns the expected string.
    """
    client = Client(buster_api_key="test-key")
    response = client.hello()
    assert response == "Hello from Buster SDK!"

def test_client_init_with_env_var(monkeypatch):
    """
    Verifies Client picks up API key from environment variable.
    """
    monkeypatch.setenv("BUSTER_API_KEY", "env-key")
    # No arg passed
    client = Client()
    assert client.buster_api_key == "env-key"

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

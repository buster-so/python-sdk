"""
Tests for Airflow configuration validation in Client initialization.
"""

import pytest

from buster import Client


def test_local_deployment_no_validation_required():
    """Local deployment should not require API credentials."""
    client = Client(buster_api_key="test-key", airflow_config={"deployment_type": "local"})
    assert client is not None


def test_astronomer_without_base_url_raises_error():
    """Astronomer deployment without base_url should raise ValueError."""
    with pytest.raises(ValueError, match="airflow_base_url"):
        Client(buster_api_key="test-key", airflow_config={"deployment_type": "astronomer"})


def test_astronomer_without_credentials_raises_error():
    """Astronomer deployment without credentials should raise ValueError."""
    with pytest.raises(ValueError, match="authentication credentials"):
        Client(
            buster_api_key="test-key",
            airflow_config={"deployment_type": "astronomer", "airflow_base_url": "http://localhost:8080"},
        )


def test_astronomer_with_token_succeeds():
    """Astronomer deployment with api_token should succeed."""
    client = Client(
        buster_api_key="test-key",
        airflow_config={
            "deployment_type": "astronomer",
            "airflow_base_url": "http://localhost:8080",
            "api_token": "test-token",
        },
    )
    assert client is not None


def test_astronomer_with_basic_auth_succeeds():
    """Astronomer deployment with username/password should succeed."""
    client = Client(
        buster_api_key="test-key",
        airflow_config={
            "deployment_type": "astronomer",
            "airflow_base_url": "http://localhost:8080",
            "api_username": "admin",
            "api_password": "admin",
        },
    )
    assert client is not None


def test_astronomer_with_username_only_raises_error():
    """Astronomer deployment with only username (no password) should raise ValueError."""
    with pytest.raises(ValueError, match="authentication credentials"):
        Client(
            buster_api_key="test-key",
            airflow_config={
                "deployment_type": "astronomer",
                "airflow_base_url": "http://localhost:8080",
                "api_username": "admin",
            },
        )


def test_no_deployment_type_no_validation():
    """No deployment_type specified should not require validation."""
    client = Client(buster_api_key="test-key", airflow_config={"send_when_retries_exhausted": True})
    assert client is not None

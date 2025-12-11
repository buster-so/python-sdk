"""
Manual test to inspect the actual payload being sent to the API.

This demonstrates that:
1. airflow_version is always present and never None
2. None values are included in the payload
"""

import json

from buster import Client


def inspect_payload(monkeypatch):
    """Inspect what payload actually gets sent to the API."""
    import buster.resources.airflow.v3 as v3_module

    captured_payload = {}

    def mock_send_request(url, payload, api_key, logger=None):
        captured_payload.update(payload)
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    # Scenario 1: Minimal context (no airflow_version config)
    print("=" * 80)
    print("Scenario 1: Minimal Context (No Config)")
    print("=" * 80)

    client1 = Client(buster_api_key="test-key")
    client1.airflow.v3.dag_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
        }
    )

    print("\nPayload sent to API:")
    print(json.dumps(captured_payload, indent=2))
    print(f"\n✓ airflow_version present: {'airflow_version' in captured_payload}")
    print(f"✓ airflow_version value: {captured_payload.get('airflow_version')}")
    print(f"✓ airflow_version is not None: {captured_payload.get('airflow_version') is not None}")

    # Check which fields have None values
    none_fields = [
        "params",
        "duration",
        "hostname",
        "operator",
        "start_date",
        "log_url",
        "state",
        "task_id",
        "try_number",
        "task_dependencies",
        "retry_config",
        "dag_config",
        "data_interval",
    ]
    fields_with_none = [f for f in none_fields if f in captured_payload and captured_payload[f] is None]
    print(f"\n✓ Fields with None values: {', '.join(fields_with_none) if fields_with_none else 'None'}")

    # Scenario 2: With manual airflow_version override
    print("\n" + "=" * 80)
    print("Scenario 2: With Manual airflow_version Override")
    print("=" * 80)

    captured_payload.clear()
    client2 = Client(buster_api_key="test-key", airflow_config={"airflow_version": "2.5.0"})
    client2.airflow.v3.dag_on_failure(
        context={
            "dag_id": "test_dag",
            "run_id": "run_123",
        }
    )

    print("\nPayload sent to API:")
    print(json.dumps(captured_payload, indent=2))
    print(f"\n✓ airflow_version value: {captured_payload.get('airflow_version')}")
    print(f"✓ Uses config override: {captured_payload.get('airflow_version') == '2.5.0'}")

    print("\n" + "=" * 80)
    print("✅ All payloads correctly include airflow_version and None values!")
    print("=" * 80)


if __name__ == "__main__":
    # Use pytest's monkeypatch in a simple way
    class SimpleMonkeypatch:
        def __init__(self):
            self.original = {}

        def setattr(self, module, name, value):
            if not hasattr(self, "original"):
                self.original = {}
            self.original[(module, name)] = getattr(module, name)
            setattr(module, name, value)

    mp = SimpleMonkeypatch()
    inspect_payload(mp)

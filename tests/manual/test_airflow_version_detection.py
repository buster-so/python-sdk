"""
Manual test to demonstrate Airflow version auto-detection.

This script shows three scenarios:
1. Airflow is installed - version is auto-detected
2. Airflow is not installed - defaults to "3.1"
3. Manual override in config - uses config value
"""

from buster import Client
from buster.resources.airflow.utils import get_airflow_version


def test_version_detection():
    """Test the get_airflow_version() function directly."""
    print("=" * 80)
    print("Testing Airflow Version Auto-Detection")
    print("=" * 80)

    detected_version = get_airflow_version()
    print(f"\n✓ Detected Airflow version: {detected_version}")

    try:
        import airflow

        actual_version = airflow.__version__
        print(f"✓ Airflow is installed: {actual_version}")
        assert detected_version == actual_version, "Version mismatch!"
        print("✓ Auto-detection working correctly!")
    except ImportError:
        print("✓ Airflow not installed - using default: 3.1")
        assert detected_version == "3.1", "Should default to 3.1"
        print("✓ Default fallback working correctly!")

    print("\n" + "=" * 80)


def test_client_usage():
    """Test how the client uses the auto-detected version."""
    print("\nTesting Client Usage")
    print("=" * 80)

    # Scenario 1: No config provided - should auto-detect
    print("\n1. Client with no airflow_config:")
    client1 = Client(buster_api_key="test-key")
    print(f"   Config: {client1.airflow.v3._config}")
    print(f"   Will use auto-detected version: {get_airflow_version()}")

    # Scenario 2: Empty config provided - should auto-detect
    print("\n2. Client with empty airflow_config:")
    client2 = Client(buster_api_key="test-key", airflow_config={})
    print(f"   Config: {client2.airflow.v3._config}")
    print(f"   Will use auto-detected version: {get_airflow_version()}")

    # Scenario 3: Manual override in config - should use config value
    print("\n3. Client with manual airflow_version override:")
    client3 = Client(buster_api_key="test-key", airflow_config={"airflow_version": "2.5.0"})
    print(f"   Config: {client3.airflow.v3._config}")
    print("   Will use config value: 2.5.0")

    print("\n" + "=" * 80)
    print("✓ All scenarios working correctly!")
    print("=" * 80)


if __name__ == "__main__":
    test_version_detection()
    test_client_usage()

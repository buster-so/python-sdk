"""
Manual test script for testing get_all_task_logs function with SDK access.

This script tests the SDK-based log retrieval functionality by querying
Airflow's metadata database directly using the Airflow SDK.

IMPORTANT: Airflow SDK Requirements
====================================
This test requires:
1. Apache Airflow to be installed in your Python environment
2. Access to the Airflow metadata database
3. The Airflow configuration to be accessible

This is useful when running code inside an Airflow environment (e.g., in a DAG or task)
where you have access to Airflow's internal APIs.

Run this script from inside the Airflow Docker container:
docker exec -it <airflow-container-name> python /opt/airflow/python-sdk/tests/manual/test_get_logs_sdk.py
"""

import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from buster.resources.airflow.get_logs import get_all_task_logs

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def check_airflow_available():
    """Check if Airflow is available."""
    print("\n" + "=" * 80)
    print("CHECKING: Airflow SDK Availability")
    print("=" * 80)

    try:
        import airflow
        from airflow import settings
        from airflow.configuration import conf

        print(f"✅ Airflow is installed: version {airflow.__version__}")
        print(f"✅ Database connection: {settings.SQL_ALCHEMY_CONN}")
        print(f"✅ Base log folder: {conf.get('logging', 'base_log_folder')}")
        return True
    except ImportError as e:
        print(f"❌ Airflow is not installed: {e}")
        print("\nTo use SDK-based log retrieval, you need to install Airflow:")
        print("  pip install apache-airflow")
        print("\nOr run this test from inside an Airflow container:")
        print("  docker exec -it <airflow-container> python /opt/airflow/python-sdk/tests/manual/test_get_logs_sdk.py")
        return False


def get_latest_run_id():
    """Get the latest run ID for example_dag using the SDK."""
    print("\n" + "=" * 80)
    print("HELPER: Fetching latest run IDs via SDK")
    print("=" * 80)

    try:
        from airflow import settings
        from airflow.models import DagRun

        session = settings.Session()

        try:
            # Query for the latest DAG runs
            dag_runs = (
                session.query(DagRun)
                .filter(DagRun.dag_id == "example_dag")
                .order_by(DagRun.execution_date.desc())
                .limit(5)
                .all()
            )

            if not dag_runs:
                print("❌ No DAG runs found for example_dag")
                return None

            print(f"\n📋 Found {len(dag_runs)} recent runs for example_dag:")
            print("-" * 80)

            for i, run in enumerate(dag_runs, 1):
                print(f"{i}. Run ID: {run.run_id}")
                print(f"   State: {run.state}")
                print(f"   Execution Date: {run.execution_date}")
                print()

            latest_run_id = dag_runs[0].run_id
            print(f"✅ Latest run ID: {latest_run_id}")
            return latest_run_id

        finally:
            session.close()

    except Exception as e:
        print(f"❌ Failed to fetch DAG runs: {e}")
        import traceback

        traceback.print_exc()
        return None


def test_example_dag_sdk():
    """Test with example_dag using SDK access."""
    print("\n" + "=" * 80)
    print("TEST 1: example_dag - print_hello task (SDK)")
    print("=" * 80)

    # Get the latest run ID
    dag_run_id = get_latest_run_id()

    if not dag_run_id:
        print("⏭️  Skipping test - no run ID available")
        return

    try:
        logs = get_all_task_logs(
            dag_id="example_dag",
            task_id="print_hello",
            dag_run_id=dag_run_id,
        )

        if logs:
            print(f"✅ Successfully retrieved {len(logs)} characters of logs")
            print(f"\nFirst 500 chars:\n{'-' * 80}")
            print(logs[:500])
            print("-" * 80)
        else:
            print("⚠️  No logs retrieved (Airflow SDK may not be available)")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


def test_task_with_multiple_attempts():
    """Test with a task that has multiple retry attempts."""
    print("\n" + "=" * 80)
    print("TEST 2: Task with multiple attempts (SDK)")
    print("=" * 80)

    print("⏭️  Skipping test (requires a task with retries)")
    print("   To test with multiple attempts:")
    print("   1. Find a task that has failed and retried")
    print("   2. Update the test with that dag_id, task_id, and run_id")
    print("   3. Uncomment the test code below")

    # Uncomment to test:
    # dag_run_id = "YOUR_RUN_ID_HERE"
    # try:
    #     logs = get_all_task_logs(
    #         dag_id="your_dag",
    #         task_id="your_task",
    #         dag_run_id=dag_run_id,
    #     )
    #
    #     print(f"✅ Successfully retrieved {len(logs)} characters of logs")
    #     print(f"   Includes logs from all retry attempts")
    #
    # except Exception as e:
    #     print(f"❌ Error: {e}")
    #     import traceback
    #     traceback.print_exc()


if __name__ == "__main__":
    print("\n🧪 Testing get_all_task_logs with Airflow SDK")
    print("=" * 80)

    # First, check if Airflow is available
    if not check_airflow_available():
        print("\n" + "=" * 80)
        print("❌ Tests cannot run without Airflow SDK available")
        print("=" * 80)
        sys.exit(1)

    # Run the SDK test
    test_example_dag_sdk()

    # Test with multiple attempts (if available)
    test_task_with_multiple_attempts()

    print("\n" + "=" * 80)
    print("✅ SDK tests complete!")
    print("=" * 80)

"""
Manual test script for testing get_all_task_logs function.

This script tests the local filesystem log retrieval functionality.
Run this script directly: python tests/manual/test_get_logs.py
"""

import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from buster.resources.airflow.get_logs import get_all_task_logs

# Setup logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Your local logs directory
LOGS_DIR = "/Users/natekelley/Documents/GitHub/airflow/v2_11/logs"


def test_example_dag():
    """Test with example_dag - simple test case."""
    print("\n" + "=" * 80)
    print("TEST 1: example_dag - print_hello task")
    print("=" * 80)

    try:
        logs = get_all_task_logs(
            dag_id="example_dag",
            task_id="print_hello",
            # We need to find a valid execution_date or dag_run_id
            # Let's first check what's in the logs directory
            deployment_type="local",
            logs_directory=LOGS_DIR,
            output_file="test_output_print_hello.log",
        )

        print(f"✅ Successfully retrieved {len(logs)} characters of logs")
        print(f"First 500 chars:\n{logs[:500]}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


def test_dag_with_error():
    """Test with dag_with_error - should have error logs."""
    print("\n" + "=" * 80)
    print("TEST 2: dag_with_error - filter_text_columns task")
    print("=" * 80)

    try:
        logs = get_all_task_logs(
            dag_id="dag_with_error",
            task_id="filter_text_columns",
            deployment_type="local",
            logs_directory=LOGS_DIR,
            output_file="test_output_filter_text_columns.log",
        )

        print(f"✅ Successfully retrieved {len(logs)} characters of logs")
        print(f"First 500 chars:\n{logs[:500]}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


def test_dbt_pipeline():
    """Test with dbt_pipeline_no_errors - longer logs."""
    print("\n" + "=" * 80)
    print("TEST 3: dbt_pipeline_no_errors - run_dbt_build task")
    print("=" * 80)

    try:
        logs = get_all_task_logs(
            dag_id="dbt_pipeline_no_errors",
            task_id="run_dbt_build",
            deployment_type="local",
            logs_directory=LOGS_DIR,
            output_file="test_output_run_dbt_build.log",
        )

        print(f"✅ Successfully retrieved {len(logs)} characters of logs")
        print(f"First 500 chars:\n{logs[:500]}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


def explore_logs_directory():
    """Explore the logs directory to find available execution dates."""
    print("\n" + "=" * 80)
    print("EXPLORING LOGS DIRECTORY")
    print("=" * 80)

    logs_path = Path(LOGS_DIR)

    if not logs_path.exists():
        print(f"❌ Logs directory does not exist: {LOGS_DIR}")
        return

    print(f"📂 Logs directory: {LOGS_DIR}\n")

    # List all DAG directories
    dag_dirs = sorted([d for d in logs_path.iterdir() if d.is_dir()])

    for dag_dir in dag_dirs:
        dag_id = dag_dir.name
        print(f"\n📋 DAG: {dag_id}")

        # List task directories
        task_dirs = sorted([t for t in dag_dir.iterdir() if t.is_dir()])

        for task_dir in task_dirs[:3]:  # Limit to first 3 tasks per DAG
            task_id = task_dir.name
            print(f"  └─ Task: {task_id}")

            # List execution dates
            exec_dates = sorted([e for e in task_dir.iterdir() if e.is_dir()])

            if exec_dates:
                # Show first and last execution date
                print(f"     └─ First execution: {exec_dates[0].name}")
                if len(exec_dates) > 1:
                    print(f"     └─ Latest execution: {exec_dates[-1].name}")
                    print(f"     └─ Total executions: {len(exec_dates)}")

                # Show log files in the latest execution
                latest_exec = exec_dates[-1]
                log_files = sorted(latest_exec.glob("*.log"))
                if log_files:
                    print(f"     └─ Log files: {[f.name for f in log_files]}")
            else:
                print("     └─ No executions found")

        if len(task_dirs) > 3:
            print(f"  └─ ... and {len(task_dirs) - 3} more tasks")


def test_with_actual_log_file():
    """Test by directly reading a log file that exists."""
    print("\n" + "=" * 80)
    print("TEST 4: Reading actual log file directly")
    print("=" * 80)

    # Find a real log file
    logs_path = Path(LOGS_DIR)

    # Navigate to example_dag logs
    example_dag_dirs = sorted(logs_path.glob("dag_id=example_dag/run_id=*/task_id=print_hello"))

    if not example_dag_dirs:
        print("❌ No example_dag print_hello logs found")
        print("Looking for alternative DAG...")

        # Try any DAG
        any_log = list(logs_path.glob("dag_id=*/run_id=*/task_id=*/attempt=*.log"))
        if any_log:
            log_file = any_log[0]
            print(f"📁 Found log file: {log_file}")
            print("📄 Reading file directly...\n")

            with open(log_file, "r") as f:
                content = f.read()
                print(f"✅ Successfully read {len(content)} characters")
                print(f"\n📄 Log content (first 1000 chars):\n{'-' * 80}")
                print(content[:1000])
                print("-" * 80)
        else:
            print("❌ No log files found in directory")
        return

    # Use the LATEST matching directory (sorted, so last one is newest)
    task_dir = example_dag_dirs[-1]
    print(f"📁 Found {len(example_dag_dirs)} runs, using latest: {task_dir}")

    # Find log files
    log_files = sorted(task_dir.glob("attempt=*.log"))

    if not log_files:
        print("❌ No log files found")
        return

    # Read the first log file directly
    log_file = log_files[0]
    print(f"📄 Reading log file: {log_file.name}")

    with open(log_file, "r") as f:
        content = f.read()

    print(f"✅ Successfully read {len(content)} characters")
    print(f"\n📄 Log content:\n{'-' * 80}")
    print(content)
    print("-" * 80)

    # Try to extract run_id and use get_all_task_logs
    print("\n🔄 Now testing with get_all_task_logs function...")

    # Extract dag_run_id from path
    # Path structure: dag_id=example_dag/run_id=manual__2025-12-12T17:32:14.612877+00:00/...
    parts = str(task_dir).split("/")
    run_id_part = [p for p in parts if p.startswith("run_id=")]

    if run_id_part:
        dag_run_id = run_id_part[0].replace("run_id=", "")
        print(f"📅 Extracted dag_run_id: {dag_run_id}")

        try:
            logs = get_all_task_logs(
                dag_id="example_dag",
                task_id="print_hello",
                dag_run_id=dag_run_id,
                deployment_type="local",
                logs_directory=LOGS_DIR,
                output_file="test_output_example_dag.log",
            )

            print(f"✅ get_all_task_logs succeeded! Retrieved {len(logs)} characters")

        except Exception as e:
            print(f"⚠️  get_all_task_logs failed (expected - non-standard log structure): {e}")


if __name__ == "__main__":
    print("\n🧪 Testing get_all_task_logs with local filesystem")
    print("=" * 80)

    # First, explore the directory structure
    explore_logs_directory()

    # Then run tests with actual data
    test_with_actual_log_file()

    print("\n" + "=" * 80)
    print("✅ Tests complete!")
    print("=" * 80)

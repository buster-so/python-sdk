"""
Demonstration of enhanced Airflow error messages with traceback and log URLs.
"""

from buster.resources.airflow.utils import extract_error_message


def simulate_task_failure():
    """Simulates a real task failure with nested function calls."""

    def process_data(value):
        if value < 0:
            raise ValueError(f"Invalid value: {value}. Must be non-negative.")
        return value * 2

    def run_task():
        data = -42
        return process_data(data)

    try:
        run_task()
    except ValueError as e:
        return e


def main():
    print("=" * 80)
    print("ENHANCED AIRFLOW ERROR REPORTING DEMO")
    print("=" * 80)

    # Create a realistic exception with traceback
    exception = simulate_task_failure()

    # Mock task instance with all available properties
    class MockTaskInstance:
        log_url = "https://airflow.example.com/dags/my_dag/grid?dag_run_id=run_123&task_id=process_task"
        state = "failed"
        max_tries = 3
        start_date = "2025-01-15T10:30:00+00:00"
        duration = 45.2
        hostname = "worker-pod-abc123"
        operator = "PythonOperator"

    # Test 1: Full error context with all available information
    print("\n1. FULL ERROR CONTEXT (Exception + Traceback + Execution Details + Log URL):")
    print("-" * 80)
    context = {
        "exception": exception,
        "task_instance": MockTaskInstance(),
        "try_number": 3,  # Final retry attempt
    }
    error_message = extract_error_message(context)
    print(error_message)

    # Test 2: Exception as string (serialized - happens in some Airflow contexts)
    print("\n\n2. SERIALIZED EXCEPTION (String format - Airflow 2+ issue):")
    print("-" * 80)
    context = {
        "exception": "task_failure",  # String instead of exception object
    }
    error_message = extract_error_message(context)
    print(error_message)

    # Test 3: Just exception (no traceback - as happens in some Airflow versions)
    print("\n\n3. BASIC ERROR (Exception object without traceback):")
    print("-" * 80)
    context = {
        "exception": ValueError("task_failure"),  # What you were seeing before
    }
    error_message = extract_error_message(context)
    print(error_message)

    # Test 4: With reason (DAG-level failure)
    print("\n\n4. DAG FAILURE (With reason):")
    print("-" * 80)
    context = {
        "reason": "Task my_task failed after 3 retries",
    }
    error_message = extract_error_message(context)
    print(error_message)

    print("\n" + "=" * 80)
    print("COMPARISON")
    print("=" * 80)
    print("\nBEFORE: Just 'task_failure'")
    print("\nAFTER:  Maximum error context including:")
    print("  ✓ Exception type and message")
    print("  ✓ Full Python traceback with line numbers")
    print("  ✓ Task execution details (state, try number, duration, host, operator)")
    print("  ✓ Execution timing information")
    print("  ✓ Direct link to Airflow logs")
    print(
        "  ✓ Handles both string and exception object formats (Airflow 2+ compatibility)"  # noqa: E501
    )
    print("\nThis gives you EVERYTHING you need to debug failures! 🎉")


if __name__ == "__main__":
    main()

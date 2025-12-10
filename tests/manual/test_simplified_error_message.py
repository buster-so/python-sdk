"""
Demonstration of simplified error_message (Option 2).

Shows before/after comparison of what gets sent to the API.
"""

import json

from buster.resources.airflow.utils import (
    extract_error_message,
    extract_traceback_frames,
)


def extract_error_message_simplified(context) -> str:
    """
    PROPOSED: Simplified version that removes traceback duplication.

    Includes:
    - Exception type and message
    - Execution context (state, retries, timing, hostname)
    - Log URL

    Excludes:
    - Text-based traceback (since traceback_frames has this in structured format)
    """
    parts = []

    # Get the exception
    exception = context.get("exception")

    if exception:
        if isinstance(exception, str):
            parts.append(f"Exception: {exception}")
        else:
            exc_type = type(exception).__name__
            exc_message = str(exception)
            parts.append(f"{exc_type}: {exc_message}")

    # Extract task instance information
    ti = context.get("task_instance") or context.get("ti")
    if ti:
        execution_details = []

        if hasattr(ti, "state"):
            execution_details.append(f"State: {ti.state}")
        if hasattr(ti, "try_number"):
            execution_details.append(f"Try: {ti.try_number}")
        if hasattr(ti, "max_tries"):
            execution_details.append(f"Max Tries: {ti.max_tries}")
        if hasattr(ti, "start_date") and ti.start_date:
            execution_details.append(f"Started: {ti.start_date}")
        if hasattr(ti, "duration") and ti.duration:
            execution_details.append(f"Duration: {ti.duration}s")
        if hasattr(ti, "hostname") and ti.hostname:
            execution_details.append(f"Host: {ti.hostname}")

        if execution_details:
            parts.append("\nExecution Details:\n" + "\n".join(execution_details))

        # Log URL - critical for debugging
        if hasattr(ti, "log_url") and ti.log_url:
            parts.append(f"\nLogs: {ti.log_url}")

    if not parts:
        return "Unknown error - no exception or error details found in context"

    return "\n\n".join(parts)


def compare_old_vs_new():
    """Compare current implementation vs simplified proposal."""

    # Create realistic exception
    try:

        def my_task_logic():
            data = {"value": None}
            result = 100 / data["value"]  # Triggers TypeError
            return result

        my_task_logic()
    except Exception as e:
        exception = e

    # Mock task instance
    class MockTaskInstance:
        state = "failed"
        try_number = 3
        max_tries = 3
        start_date = "2024-01-01 10:00:00"
        duration = 12.45
        hostname = "airflow-worker-pod-xyz"
        log_url = "https://airflow.company.com/dags/etl_pipeline/grid?task_id=transform_data&dag_run_id=scheduled_2024_01_01"

    context = {
        "exception": exception,
        "task_instance": MockTaskInstance(),
    }

    # Get both versions
    old_error_message = extract_error_message(context)
    new_error_message = extract_error_message_simplified(context)
    traceback_frames = extract_traceback_frames(exception)

    print("=" * 100)
    print("BEFORE vs AFTER COMPARISON")
    print("=" * 100)

    print("\n" + "=" * 100)
    print("CURRENT (Option 1): error_message includes everything")
    print("=" * 100)
    print(old_error_message)
    print(f"\n📊 Size: {len(old_error_message)} characters")

    print("\n" + "=" * 100)
    print("PROPOSED (Option 2): error_message without traceback text")
    print("=" * 100)
    print(new_error_message)
    print(f"\n📊 Size: {len(new_error_message)} characters")
    print(
        f"💾 Savings: {len(old_error_message) - len(new_error_message)} characters ({round((1 - len(new_error_message) / len(old_error_message)) * 100)}% reduction)"
    )

    print("\n" + "=" * 100)
    print("WHAT GETS SENT TO API (Simplified)")
    print("=" * 100)

    payload = {
        "dag_id": "etl_pipeline",
        "run_id": "scheduled_2024_01_01",
        "task_id": "transform_data",
        "event": "task_instance_failed",
        "error_message": new_error_message,  # Simplified version
        "traceback_frames": traceback_frames,  # Structured stack trace
        "exception_location": {
            "type": "TypeError",
            "message": str(exception),
            "file": __file__,
            "line": 73,
            "function": "my_task_logic",
        },
    }

    print(json.dumps(payload, indent=2))

    print("\n" + "=" * 100)
    print("BENEFITS OF SIMPLIFIED APPROACH")
    print("=" * 100)

    print("\n✅ Clear Separation of Concerns:")
    print("   • error_message: Context (state, timing, where to look)")
    print("   • traceback_frames: Stack trace (structured for LLM)")
    print("   • exception_location: Error origin (quick lookup)")

    print("\n✅ No Information Loss:")
    print("   • All traceback data still sent (in traceback_frames)")
    print("   • All context data still sent (in error_message)")
    print("   • LLM can easily parse both")

    print("\n✅ Smaller Payload:")
    print(f"   • {round((1 - len(new_error_message) / len(old_error_message)) * 100)}% reduction in error_message field")
    print("   • Faster transmission, lower costs")

    print("\n✅ Better for Both Humans AND AI:")
    print("   • Humans: Quick context + log link in error_message")
    print("   • AI: Structured traceback_frames for code analysis")

    print("\n" + "=" * 100)


if __name__ == "__main__":
    compare_old_vs_new()

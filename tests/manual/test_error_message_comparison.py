"""
Manual test to compare error_message vs traceback_frames.

This demonstrates what information each field contains and whether there's overlap.
"""

import json

from buster.resources.airflow.utils import (
    extract_error_message,
    extract_exception_location,
    extract_traceback_frames,
)


def create_test_exception():
    """Create a realistic exception with traceback for testing."""
    try:

        def level_3_function():
            """Deepest function that raises the error."""
            x = 10
            y = 0
            result = x / y  # This will raise ZeroDivisionError
            return result

        def level_2_function():
            """Middle function."""
            return level_3_function()

        def level_1_function():
            """Outer function."""
            return level_2_function()

        level_1_function()
    except ZeroDivisionError as e:
        return e


def compare_error_representations():
    """Compare what error_message, traceback_frames, and exception_location contain."""

    exception = create_test_exception()

    # Mock task instance for realistic context
    class MockTaskInstance:
        state = "failed"
        try_number = 2
        max_tries = 3
        start_date = "2024-01-01 10:00:00"
        duration = 5.23
        hostname = "worker-node-1"
        log_url = "https://airflow.example.com/dags/my_dag/grid?task_id=my_task&dag_run_id=123"

    context = {
        "exception": exception,
        "task_instance": MockTaskInstance(),
        "dag_id": "my_dag",
        "run_id": "run_123",
    }

    # Extract all three representations
    error_message = extract_error_message(context)
    traceback_frames = extract_traceback_frames(exception)
    exception_location = extract_exception_location(exception)

    print("=" * 100)
    print("COMPARISON: error_message vs traceback_frames vs exception_location")
    print("=" * 100)

    print("\n" + "=" * 100)
    print("1. ERROR_MESSAGE (String - Human Readable)")
    print("=" * 100)
    print(error_message)
    print(f"\nLength: {len(error_message)} characters")

    print("\n" + "=" * 100)
    print("2. TRACEBACK_FRAMES (Structured Array - LLM Friendly)")
    print("=" * 100)
    print(json.dumps(traceback_frames, indent=2))

    print("\n" + "=" * 100)
    print("3. EXCEPTION_LOCATION (Single Frame - Error Origin)")
    print("=" * 100)
    print(json.dumps(exception_location, indent=2))

    print("\n" + "=" * 100)
    print("ANALYSIS: What's Different?")
    print("=" * 100)

    print("\n📝 ERROR_MESSAGE contains:")
    print("   ✓ Exception type and message")
    print("   ✓ Full text-based traceback (Python's format_exception output)")
    print("   ✓ Execution details (state, try number, duration, hostname)")
    print("   ✓ Airflow log URL")
    print("   → Format: Unstructured text block, human-readable")
    print("   → Best for: Quick human reading, logs, debugging")

    print("\n🔍 TRACEBACK_FRAMES contains:")
    print("   ✓ Structured array of ALL stack frames")
    print("   ✓ Each frame has: file, line, function, code")
    print("   ✓ Complete call stack from outermost to innermost")
    print("   → Format: Structured JSON array")
    print("   → Best for: LLM parsing, programmatic analysis, code navigation")

    print("\n🎯 EXCEPTION_LOCATION contains:")
    print("   ✓ Only the FINAL error location (where exception was raised)")
    print("   ✓ Single frame: type, message, file, line, function")
    print("   → Format: Structured JSON object")
    print("   → Best for: Quick location lookup, error grouping")

    print("\n" + "=" * 100)
    print("OVERLAP ANALYSIS")
    print("=" * 100)

    print("\n❓ Is error_message redundant with traceback_frames?")
    print("\n   PARTIALLY YES:")
    print("   - Both contain the traceback information")
    print("   - traceback_frames is more structured and easier for LLMs to parse")
    print("   - error_message has the SAME traceback but as formatted text")

    print("\n   BUT error_message ALSO includes:")
    print("   - Execution context (state, retries, timing) ← NOT in traceback_frames")
    print("   - Log URL ← NOT in traceback_frames")
    print("   - Human-friendly formatting ← Better for logs/debugging")

    print("\n" + "=" * 100)
    print("RECOMMENDATION")
    print("=" * 100)

    print("\n🎯 OPTION 1: Keep Both (Current) - RECOMMENDED")
    print("   Pros:")
    print("   - error_message: Human-readable, includes execution context + log URL")
    print("   - traceback_frames: LLM-friendly structured data")
    print("   - Different use cases: humans vs AI")
    print("   Cons:")
    print("   - Some duplication of traceback data (~30-40% overlap)")
    print("   - Larger payload size")

    print("\n🎯 OPTION 2: Simplify error_message")
    print("   Change error_message to ONLY include:")
    print("   - Exception type and message")
    print("   - Execution context (state, tries, duration, host)")
    print("   - Log URL")
    print("   - Remove the text traceback (since traceback_frames has it)")
    print("   Pros:")
    print("   - Smaller payload")
    print("   - Clear separation: error_message=context, traceback_frames=stack")
    print("   Cons:")
    print("   - Less useful for quick human debugging")
    print("   - Breaking change for existing consumers")

    print("\n🎯 OPTION 3: Remove error_message entirely")
    print("   Rely only on traceback_frames + exception_location")
    print("   Pros:")
    print("   - Smallest payload")
    print("   - No duplication")
    print("   Cons:")
    print("   - Lose execution context and log URL")
    print("   - Less human-friendly")
    print("   - Major breaking change")

    print("\n" + "=" * 100)
    print("MY RECOMMENDATION:")
    print("=" * 100)
    print("\n✅ OPTION 2: Simplify error_message")
    print("\nReasoning:")
    print("1. Keep the unique value: execution context + log URL")
    print("2. Remove duplication: text traceback (already in traceback_frames)")
    print("3. Smaller payload without losing important data")
    print("4. Better separation of concerns:")
    print("   - error_message: WHY it failed + WHERE to look (context + logs)")
    print("   - traceback_frames: HOW it failed (stack trace)")
    print("   - exception_location: WHAT failed (error origin)")
    print("\n" + "=" * 100)


if __name__ == "__main__":
    compare_error_representations()

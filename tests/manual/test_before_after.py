"""
Clear demonstration of BEFORE (with traceback) vs AFTER (simplified).
"""

import json

from buster import Client


def demo_before_after(monkeypatch):
    """Show exact payload difference."""
    import buster.resources.airflow.v3 as v3_module

    # Create a realistic exception with deep stack
    try:

        def process_data(value):
            return 100 / value

        def validate_input(data):
            return process_data(data["value"])

        def run_task():
            input_data = {"value": None}
            return validate_input(input_data)

        run_task()
    except Exception as e:
        exception = e

    # Mock task instance with full context
    class MockTaskInstance:
        state = "failed"
        try_number = 3
        max_tries = 3
        start_date = "2024-12-10 15:30:00"
        duration = 8.54
        hostname = "k8s-worker-pod-abc123"
        log_url = "https://airflow.example.com/dags/etl_pipeline/grid?task_id=transform&dag_run_id=manual_2024_12_10"

    captured_payloads = []

    def mock_send_request(url, payload, api_key, logger=None):
        captured_payloads.append(payload)
        return {"success": True}

    monkeypatch.setattr(v3_module, "send_request", mock_send_request)

    client = Client(buster_api_key="test-key")
    client.airflow.v3.task_on_failure(
        context={
            "dag_id": "etl_pipeline",
            "run_id": "manual_2024_12_10",
            "task_id": "transform",
            "exception": exception,
            "task_instance": MockTaskInstance(),
        }
    )

    payload = captured_payloads[0]

    print("=" * 100)
    print("✨ SIMPLIFIED error_message (NEW)")
    print("=" * 100)
    print("\nPayload sent to API:")
    print(
        json.dumps(
            {
                "dag_id": payload["dag_id"],
                "run_id": payload["run_id"],
                "task_id": payload["task_id"],
                "event": payload["event"],
                "airflow_version": payload["airflow_version"],
            },
            indent=2,
        )
    )

    print("\n" + "-" * 100)
    print("error_message field (context only, NO traceback):")
    print("-" * 100)
    print(payload["error_message"])
    print(f"\n📏 Length: {len(payload['error_message'])} characters")

    print("\n" + "-" * 100)
    print("traceback_frames field (structured stack trace):")
    print("-" * 100)
    print(json.dumps(payload["traceback_frames"], indent=2))
    print(f"\n📏 Frames: {len(payload['traceback_frames'])} frames")

    print("\n" + "-" * 100)
    print("exception_location field (error origin):")
    print("-" * 100)
    print(json.dumps(payload["exception_location"], indent=2))

    print("\n" + "=" * 100)
    print("✅ BENEFITS")
    print("=" * 100)
    print("\n1. NO DUPLICATION:")
    print("   • error_message = Exception type + execution context + log URL")
    print("   • traceback_frames = Full stack trace (structured)")
    print("   • exception_location = Error origin (single frame)")

    print("\n2. CLEAR SEPARATION:")
    print("   • WHY: error_message explains what failed and execution context")
    print("   • HOW: traceback_frames shows the full call stack")
    print("   • WHERE: exception_location pinpoints the exact line")

    print("\n3. OPTIMIZED FOR BOTH HUMANS & AI:")
    print("   • Humans: Quick scan of error_message + click log URL")
    print("   • AI/LLM: Parse structured traceback_frames for code analysis")

    print("\n4. SMALLER PAYLOAD:")
    print("   • No redundant text traceback in error_message")
    print("   • Structured data is more efficient")

    print("\n" + "=" * 100)


if __name__ == "__main__":

    class SimpleMonkeypatch:
        def setattr(self, module, name, value):
            setattr(module, name, value)

    mp = SimpleMonkeypatch()
    demo_before_after(mp)

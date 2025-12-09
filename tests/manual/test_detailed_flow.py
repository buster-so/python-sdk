#!/usr/bin/env python3
"""
Shows the most detailed logging flow possible - full event reporting with DEBUG level.
"""

import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.buster import Client
from src.buster.types import AirflowCallbackContext

print("\n" + "=" * 80)
print("DETAILED FLOW: Airflow Task Failure Reporting (DEBUG Level)")
print("=" * 80)
print("\nThis shows EVERY log statement when reporting an Airflow task failure.\n")

# Create client with DEBUG level
print("Step 1: Creating Buster client with DEBUG level...")
print("-" * 80)
client = Client(
    buster_api_key="test_api_key_for_demo",
    debug="debug",
)

# Simulate a task failure with retries exhausted
print("\n" + "=" * 80)
print("Step 2: Simulating task failure with retries exhausted...")
print("-" * 80)

context: AirflowCallbackContext = {
    "dag_id": "data_pipeline",
    "run_id": "scheduled__2024-01-15T10:00:00+00:00",
    "task_id": "transform_user_data",
    "try_number": 3,  # Final retry
    "max_tries": 3,  # Retries exhausted
    "exception": Exception("Database deadlock detected after 30s timeout"),
    "reason": None,
}

print("\nContext details:")
print(f"  • DAG: {context['dag_id']}")
print(f"  • Task: {context['task_id']}")
print(f"  • Try: {context['try_number']}/{context['max_tries']}")
print(f"  • Error: {context['exception']}")
print("\nTriggering task_on_failure callback...\n")

try:
    result = client.airflow.v3.task_on_failure(context)
except Exception as e:
    print(f"\nAPI call failed (expected with test key): {type(e).__name__}: {e}")

print("\n" + "=" * 80)
print("COMPLETE LOG BREAKDOWN")
print("=" * 80)
print("""
Logs shown in this flow:

CLIENT INITIALIZATION (Step 1):
  DEBUG: Initializing Buster SDK client...
  DEBUG: API key loaded from parameter
  DEBUG: Initializing Airflow resource...
  DEBUG: AirflowV3 handler initialized
  DEBUG: Airflow resource initialized
  INFO:  ✓ Buster SDK client initialized successfully

TASK FAILURE CALLBACK (Step 2):
  DEBUG: Task failure callback triggered
  DEBUG: Error message: Database deadlock detected after 30s timeout
  INFO:  📋 Reporting task_instance_failed with dag_id, run_id, task_id
  DEBUG: Event details: try_number=3, max_tries=3
  DEBUG: Validating event data...
  DEBUG: Event validation successful
  DEBUG: Sending request to: https://api2.buster.so/... (env, api_version)
  DEBUG: Sending POST request to <url>
  DEBUG: Payload keys: ['dag_id', 'run_id', 'task_id', ...]
  ERROR: HTTP error occurred: 401 Client Error
  ERROR: Response status: 401
  ERROR: Response body: {"error":"Invalid or expired API key"}

Note: With a valid API key, you would see:
  DEBUG: Response status: 200
  INFO:  ✓ Event reported successfully
""")

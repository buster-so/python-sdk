#!/usr/bin/env python3
"""
Comprehensive test script to demonstrate all logging scenarios in the Buster SDK.
This shows every log statement across different log levels.
"""
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.buster import Client, DebugLevel
from src.buster.types import AirflowContext, Environment, ApiVersion

print("\n" + "=" * 80)
print("BUSTER SDK - COMPREHENSIVE LOGGING TEST")
print("=" * 80)

# =============================================================================
# TEST 1: Client Initialization - DEBUG Level
# =============================================================================
print("\n" + "-" * 80)
print("TEST 1: Client Initialization with DEBUG Level")
print("-" * 80)
print("Shows: All DEBUG and INFO logs during client setup\n")

client_debug = Client(
    buster_api_key="test_api_key_debug",
    airflow_config={
        "env": Environment.DEVELOPMENT,
        "api_version": ApiVersion.V2,
        "airflow_version": "2.7.0",
    },
    debug=DebugLevel.DEBUG,
)

# =============================================================================
# TEST 2: Client Initialization - INFO Level
# =============================================================================
print("\n" + "-" * 80)
print("TEST 2: Client Initialization with INFO Level")
print("-" * 80)
print("Shows: Only INFO logs (DEBUG logs are hidden)\n")

client_info = Client(
    buster_api_key="test_api_key_info",
    debug=DebugLevel.INFO,
)

# =============================================================================
# TEST 3: Client Initialization - ERROR Level
# =============================================================================
print("\n" + "-" * 80)
print("TEST 3: Client Initialization with ERROR Level")
print("-" * 80)
print("Shows: Only ERROR logs (no errors during successful init, so nothing shown)\n")

client_error = Client(
    buster_api_key="test_api_key_error",
    debug=DebugLevel.ERROR,
)
print("(No logs shown - initialization was successful with no errors)")

# =============================================================================
# TEST 4: Missing API Key Error
# =============================================================================
print("\n" + "-" * 80)
print("TEST 4: Error Logging - Missing API Key")
print("-" * 80)
print("Shows: ERROR level log when API key is missing\n")

try:
    client_no_key = Client(debug=DebugLevel.ERROR)
except ValueError as e:
    print(f"Exception raised: {e}")

# =============================================================================
# TEST 5: Airflow Task Failure - Retries Not Exhausted
# =============================================================================
print("\n" + "-" * 80)
print("TEST 5: Airflow Task Failure (Retries Not Exhausted)")
print("-" * 80)
print("Shows: INFO logs for skipping report due to retry logic\n")

context_with_retries: AirflowContext = {
    "dag_id": "example_dag",
    "run_id": "scheduled__2024-01-15T00:00:00",
    "task_id": "extract_data",
    "try_number": 1,
    "max_tries": 3,
    "exception": Exception("Database connection timeout"),
    "reason": None,
}

result = client_info.airflow.v3.task_on_failure(context_with_retries)

# =============================================================================
# TEST 6: Airflow DAG Failure - DEBUG Level
# =============================================================================
print("\n" + "-" * 80)
print("TEST 6: Airflow DAG Failure with DEBUG Level")
print("-" * 80)
print("Shows: DEBUG logs showing callback trigger and error extraction\n")

context_dag_failure: AirflowContext = {
    "dag_id": "critical_pipeline",
    "run_id": "manual__2024-01-15T12:00:00",
    "task_id": None,
    "try_number": 1,
    "max_tries": 1,
    "exception": None,
    "reason": "DAG failed due to upstream task failure",
}

try:
    client_debug.airflow.v3.dag_on_failure(context_dag_failure)
except Exception as e:
    print(f"\nAPI call failed (expected): {type(e).__name__}")

# =============================================================================
# TEST 7: Airflow Task Failure with Retries Exhausted - Full DEBUG
# =============================================================================
print("\n" + "-" * 80)
print("TEST 7: Task Failure with Retries Exhausted (Full DEBUG logging)")
print("-" * 80)
print("Shows: Complete flow with all DEBUG logs including HTTP request details\n")

context_exhausted: AirflowContext = {
    "dag_id": "payment_processing",
    "run_id": "scheduled__2024-01-15T06:00:00",
    "task_id": "process_payments",
    "try_number": 3,
    "max_tries": 3,
    "exception": Exception("Payment gateway returned 503 Service Unavailable"),
    "reason": None,
}

try:
    client_debug.airflow.v3.task_on_failure(context_exhausted)
except Exception as e:
    print(f"\nAPI call failed (expected with test key): {type(e).__name__}")

# =============================================================================
# TEST 8: Validation Error
# =============================================================================
print("\n" + "-" * 80)
print("TEST 8: Validation Error (Missing Required Fields)")
print("-" * 80)
print("Shows: ERROR logs when validation fails\n")

invalid_context: AirflowContext = {
    "dag_id": "",  # Invalid: empty string
    "run_id": "",  # Invalid: empty string
    "task_id": None,
    "try_number": None,
    "max_tries": None,
    "exception": None,
    "reason": None,
}

try:
    client_error.airflow.v3.task_on_failure(invalid_context)
except (ValueError, Exception) as e:
    print(f"Validation failed (expected): {type(e).__name__}")

# =============================================================================
# TEST 9: Different Log Levels Side by Side
# =============================================================================
print("\n" + "-" * 80)
print("TEST 9: Comparing Different Log Levels")
print("-" * 80)

from src.buster.utils import setup_logger

print("\nDEBUG Level Logger:")
logger_debug = setup_logger("test.debug", DebugLevel.DEBUG)
logger_debug.debug("This is a DEBUG message")
logger_debug.info("This is an INFO message")
logger_debug.warning("This is a WARNING message")
logger_debug.error("This is an ERROR message")

print("\nINFO Level Logger (DEBUG hidden):")
logger_info = setup_logger("test.info", DebugLevel.INFO)
logger_info.debug("This DEBUG message is hidden")
logger_info.info("This is an INFO message")
logger_info.warning("This is a WARNING message")
logger_info.error("This is an ERROR message")

print("\nWARN Level Logger (DEBUG and INFO hidden):")
logger_warn = setup_logger("test.warn", DebugLevel.WARN)
logger_warn.debug("This DEBUG message is hidden")
logger_warn.info("This INFO message is hidden")
logger_warn.warning("This is a WARNING message")
logger_warn.error("This is an ERROR message")

print("\nERROR Level Logger (only errors shown):")
logger_error = setup_logger("test.error", DebugLevel.ERROR)
logger_error.debug("This DEBUG message is hidden")
logger_error.info("This INFO message is hidden")
logger_error.warning("This WARNING message is hidden")
logger_error.error("This is an ERROR message")

# =============================================================================
# Summary
# =============================================================================
print("\n" + "=" * 80)
print("LOGGING TEST COMPLETE")
print("=" * 80)
print("""
Summary of log levels used:
- DEBUG: Cyan    - Detailed debugging (initialization steps, validation, etc.)
- INFO:  Green   - Important events (successful operations, skipped reports)
- WARN:  Yellow  - Warnings (not yet used in SDK)
- ERROR: Red     - Errors (API failures, missing keys, validation errors)

Note: Colors are visible in terminal but not in piped/redirected output.
""")

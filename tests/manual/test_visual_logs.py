#!/usr/bin/env python3
"""
Visual test showing each type of log with clear labels.
"""

import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.buster import Client
from src.buster.types import AirflowCallbackContext
from src.buster.utils import setup_logger


def section(title):
    """Print a section header"""
    print("\n" + "=" * 70, file=sys.stderr)
    print(f"  {title}", file=sys.stderr)
    print("=" * 70, file=sys.stderr)
    sys.stderr.flush()


def subsection(desc):
    """Print a subsection"""
    print(f"\n>> {desc}", file=sys.stderr)
    sys.stderr.flush()


# =============================================================================
section("1. COLOR-CODED LOG LEVELS")
# =============================================================================

subsection("All log levels with their colors:")
logger = setup_logger("demo", "debug")
logger.debug("DEBUG level - Cyan - Detailed debugging information")
logger.info("INFO level - Green - General informational messages")
logger.warning("WARNING level - Yellow - Warning messages")
logger.error("ERROR level - Red - Error messages")

# =============================================================================
section("2. CLIENT INITIALIZATION - DEBUG LEVEL")
# =============================================================================

subsection("Creating client with DEBUG level shows all initialization steps:")
client_debug = Client(
    buster_api_key="test_key_12345",
    debug="debug",
)

# =============================================================================
section("3. CLIENT INITIALIZATION - INFO LEVEL")
# =============================================================================

subsection("Creating client with INFO level shows only important events:")
client_info = Client(
    buster_api_key="test_key_67890",
    debug="info",
)

# =============================================================================
section("4. AIRFLOW TASK FAILURE - RETRIES NOT EXHAUSTED")
# =============================================================================

subsection("When retries are available, SDK skips reporting:")
context_retry: AirflowCallbackContext = {
    "dag_id": "etl_pipeline",
    "run_id": "scheduled__2024-01-15",
    "task_id": "extract_data",
    "try_number": 1,
    "max_tries": 3,
    "exception": Exception("Connection timeout"),
    "reason": None,
}
client_info.airflow.v3.task_on_failure(context_retry)

# =============================================================================
section("5. AIRFLOW DAG FAILURE - WITH DEBUG")
# =============================================================================

subsection("DAG failure callback with detailed DEBUG logging:")
context_dag: AirflowCallbackContext = {
    "dag_id": "critical_pipeline",
    "run_id": "manual__2024-01-15",
    "task_id": None,
    "try_number": 1,
    "max_tries": 1,
    "exception": None,
    "reason": "Upstream task failed in dependency check",
}
try:
    client_debug.airflow.v3.dag_on_failure(context_dag)
except Exception:
    pass

# =============================================================================
section("6. FULL REPORTING FLOW - DEBUG LEVEL")
# =============================================================================

subsection("Complete flow when retries are exhausted (shows all logs):")
context_exhausted: AirflowCallbackContext = {
    "dag_id": "payment_processing",
    "run_id": "scheduled__2024-01-15",
    "task_id": "charge_customers",
    "try_number": 3,
    "max_tries": 3,
    "exception": Exception("Payment API returned 503"),
    "reason": None,
}
try:
    client_debug.airflow.v3.task_on_failure(context_exhausted)
except Exception:
    pass

# =============================================================================
section("7. ERROR SCENARIOS")
# =============================================================================

subsection("Missing API key error:")
try:
    Client(debug="error")
except ValueError:
    pass

# =============================================================================
section("8. LOG LEVEL FILTERING")
# =============================================================================

subsection("DEBUG level - shows everything:")
l1 = setup_logger("level_test.debug", "debug")
l1.debug("✓ DEBUG visible")
l1.info("✓ INFO visible")
l1.error("✓ ERROR visible")

subsection("INFO level - hides DEBUG:")
l2 = setup_logger("level_test.info", "info")
l2.debug("✗ DEBUG hidden")
l2.info("✓ INFO visible")
l2.error("✓ ERROR visible")

subsection("ERROR level - only errors:")
l3 = setup_logger("level_test.error", "error")
l3.debug("✗ DEBUG hidden")
l3.info("✗ INFO hidden")
l3.error("✓ ERROR visible")

# =============================================================================
section("TEST COMPLETE")
# =============================================================================
print("\n✓ All log types demonstrated!\n", file=sys.stderr)
print("In a real terminal, you'll see:", file=sys.stderr)
print("  - DEBUG in cyan", file=sys.stderr)
print("  - INFO in green", file=sys.stderr)
print("  - WARNING in yellow", file=sys.stderr)
print("  - ERROR in red\n", file=sys.stderr)

"""Quick test for SDK-based log retrieval.

This test uses the simplified API that always uses Airflow SDK.
It must be run from inside the Airflow Docker container where Airflow is installed.

Run from inside container:
docker exec -it v2_11-airflow-webserver-1 python /opt/airflow/python-sdk/tests/manual/test_api_quick.py
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from buster.resources.airflow.get_logs import get_all_task_logs

# Set up logging to see debug output
logging.basicConfig(level=logging.INFO, format="%(message)s")

print("Testing SDK-based log retrieval...")
print("=" * 80)

logs = get_all_task_logs(
    dag_id="example_dag",
    task_id="print_hello",
    dag_run_id="manual__2025-12-12T21:54:41+00:00",
)

if logs:
    print(f"\n✅ SUCCESS! Retrieved {len(logs)} characters of logs")
    print("\nFirst 600 characters:")
    print("-" * 80)
    print(logs[:600])
    print("-" * 80)
else:
    print("\n⚠️  No logs retrieved (Airflow SDK may not be available)")
    print("Make sure to run this test from inside the Airflow Docker container")

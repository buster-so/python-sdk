import traceback
from datetime import datetime, timedelta
from typing import Any, Dict

from buster.types import ApiVersion, Environment
from buster.types.api import AirflowFlowVersion
from buster.utils import get_buster_url


def serialize_airflow_context(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Serialize an Airflow context dictionary to a JSON-safe format.

    Handles:
    - Airflow objects (TaskInstance, DagRun, DAG, Task, etc.)
    - datetime and timedelta objects
    - Exception objects (converts to dict with type, message, and traceback)
    - Sets (converts to lists)
    - Other non-serializable objects (converts to string representation)

    Args:
        context: The Airflow context dictionary to serialize

    Returns:
        A JSON-serializable dictionary
    """
    serialized: Dict[str, Any] = {}

    for key, value in context.items():
        serialized[key] = _serialize_value(value)

    return serialized


def _serialize_value(value: Any) -> Any:
    """
    Recursively serialize a value to a JSON-safe format.

    Args:
        value: The value to serialize

    Returns:
        A JSON-serializable version of the value
    """
    # Handle None
    if value is None:
        return None

    # Handle primitives (str, int, float, bool)
    if isinstance(value, (str, int, float, bool)):
        return value

    # Handle datetime objects
    if isinstance(value, datetime):
        return value.isoformat()

    # Handle timedelta objects
    if isinstance(value, timedelta):
        return str(value)

    # Handle Exception objects - serialize with type, message, and traceback
    if isinstance(value, BaseException):
        return {
            "_type": "exception",
            "exception_type": type(value).__name__,
            "exception_message": str(value),
            "traceback": traceback.format_exception(type(value), value, value.__traceback__),
        }

    # Handle lists
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]

    # Handle tuples (convert to list)
    if isinstance(value, tuple):
        return [_serialize_value(item) for item in value]

    # Handle sets (convert to list)
    if isinstance(value, set):
        return [_serialize_value(item) for item in value]

    # Handle dictionaries
    if isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}

    # Handle objects with __dict__ attribute (serialize their attributes)
    if hasattr(value, "__dict__"):
        obj_dict = {"_type": "object", "_class": type(value).__name__}
        try:
            # Try to serialize the object's attributes
            for attr_key, attr_value in value.__dict__.items():
                # Skip private/protected attributes
                if not attr_key.startswith("_"):
                    try:
                        obj_dict[attr_key] = _serialize_value(attr_value)
                    except Exception:
                        # If serialization fails for an attribute, convert to string
                        obj_dict[attr_key] = str(attr_value)
            return obj_dict
        except Exception:
            # If we can't serialize the object's dict, fall back to string
            return str(value)

    # Handle callable objects (functions, methods)
    if callable(value):
        return f"<callable: {getattr(value, '__name__', str(value))}>"

    # Fallback: convert to string
    try:
        return str(value)
    except Exception:
        return "<unserializable>"


def get_airflow_version(flow_version: AirflowFlowVersion = "3.1") -> str:
    """
    Attempts to detect the installed Airflow version using the official pattern.

    This follows the same approach used by Apache Airflow's official provider packages:
    https://airflow.apache.org/docs/apache-airflow-providers/

    Returns:
        The Airflow version string (e.g., "2.5.0" or "3.1")
    """
    try:
        # Try importing __version__ directly (preferred method)
        from airflow import (  # type: ignore[import-not-found] # pyright: ignore[reportMissingImports]
            __version__ as airflow_version,
        )

        return str(airflow_version)
    except ImportError:
        try:
            # Fallback to airflow.version.version (used in some environments)
            from airflow.version import (  # type: ignore[import-not-found] # pyright: ignore[reportMissingImports]
                version as airflow_version,
            )

            return str(airflow_version)
        except (ImportError, AttributeError):
            # Airflow not installed or version not available, use default
            return flow_version


def get_airflow_v3_url(env: Environment, api_version: ApiVersion) -> str:
    """
    Constructs the full API URL based on the environment and API version.
    """
    base_url = get_buster_url(env, api_version)
    return f"{base_url}/public/airflow-events"

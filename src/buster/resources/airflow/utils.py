from buster.types import AirflowContext, ApiVersion, Environment


def get_airflow_v3_url(env: Environment, api_version: ApiVersion) -> str:
    """
    Constructs the full API URL based on the environment and API version.
    """
    base_urls = {
        Environment.PRODUCTION: "https://api2.buster.so",
        Environment.DEVELOPMENT: "http://localhost:3000",
        Environment.STAGING: "https://api2.staging.buster.so",
    }

    base_url = base_urls.get(env)
    # This should theoretically not happen if typing is respected, but good for safety
    if not base_url:
        raise ValueError(f"Unknown environment: {env}")

    return f"{base_url}/api/{api_version.value}/public/airflow-events"


def extract_error_message(context: AirflowContext) -> str:
    """
    Extracts a human-readable error message from the Airflow context.
    Prioritizes 'reason' (DAG failures) then 'exception' (Task/DAG failures).
    """
    # unexpected error
    reason = context.get("reason")
    if reason:
        return reason

    # expected failure
    exception = context.get("exception")
    if exception:
        return str(exception)

    return "Unknown error"

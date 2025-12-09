from buster.types import ApiVersion, Environment


def get_buster_url(env: Environment, api_version: ApiVersion) -> str:
    """
    Returns the Buster API URL for the given environment and API version.

    Args:
        env: The target environment (production, staging, development)
        api_version: The API version to use (v2, etc.)

    Returns:
        The full URL for the Buster API including the API version path

    Raises:
        ValueError: If an unknown environment is provided
    """
    base_urls: dict[Environment, str] = {
        "production": "https://api2.buster.so",
        "development": "http://host.docker.internal:3002",
        "development-local": "http://localhost:3002",
        "staging": "https://api2.staging.buster.so",
    }

    base_url = base_urls.get(env)
    if not base_url:
        raise ValueError(f"Unknown environment: {env}")

    return f"{base_url}/api/{api_version}"

from typing import Optional

from .resources.airflow import AirflowResource
from .types import AirflowReportConfig, ApiVersion, DebugLevel, Environment
from .utils import get_buster_url, setup_logger
from .validation import validate_airflow_config, validate_and_get_api_key


class Client:
    """
    A client for the Buster SDK.
    """

    def __init__(
        self,
        buster_api_key: Optional[str] = None,
        env: Optional[Environment] = None,
        api_version: Optional[ApiVersion] = None,
        airflow_config: Optional[AirflowReportConfig] = None,
        debug: Optional[DebugLevel] = None,
    ):
        # Setup logger based on debug level
        self.logger = setup_logger("buster", debug)
        self.logger.debug("Initializing Buster SDK client...")

        # Set environment (default to production if not provided)
        self.env = env or "production"
        # Set API version (default to v2 if not provided)
        self.api_version = api_version or "v2"
        base_url = get_buster_url(self.env, self.api_version)
        self.logger.debug(f"Environment: {self.env}")
        self.logger.debug(f"API Version: {self.api_version}")
        self.logger.debug(f"Base URL: {base_url}")

        # Validate and retrieve API key
        self._buster_api_key = validate_and_get_api_key(buster_api_key, self.logger)

        # Log and validate Airflow configuration
        if airflow_config:
            self.logger.debug(f"Airflow configuration provided: {airflow_config}")
            validate_airflow_config(airflow_config, self.logger)

        self.airflow = AirflowResource(self, config=airflow_config)

        self.logger.info(f"✓ Buster SDK client initialized (debug level: {debug if debug else 'off'})")

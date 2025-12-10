from typing import Optional

from .resources.airflow import AirflowResource
from .types import AirflowReportConfig, ApiVersion, DebugLevel, Environment
from .utils import get_buster_url, setup_logger


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
        import os

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

        # 1. Try param
        self._buster_api_key = buster_api_key
        if self._buster_api_key:
            self.logger.debug("API key loaded from parameter")

        # 2. Try env var
        if not self._buster_api_key:
            self._buster_api_key = os.environ.get("BUSTER_API_KEY")
            if self._buster_api_key:
                self.logger.debug("API key loaded from environment variable")

        # 3. Fail if missing
        if not self._buster_api_key:
            self.logger.error("API key not found in parameter or environment variable")
            raise ValueError(
                "Buster API key must be provided via 'buster_api_key' param or 'BUSTER_API_KEY' environment variable."
            )

        # Log configuration
        if airflow_config:
            self.logger.debug(f"Airflow configuration provided: {airflow_config}")

        self.airflow = AirflowResource(self, config=airflow_config)

        self.logger.info(f"✓ Buster SDK client initialized (debug level: {debug if debug else 'off'})")

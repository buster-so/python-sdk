from .v2_11 import AirflowV2_11
from .v3 import AirflowV3


class AirflowResource:
    """
    Airflow integration resource providing version-specific handlers.

    Attributes:
        v3: Handler for Airflow 3.x callbacks
        v2_11: Handler for Airflow 2.11 callbacks
    """

    def __init__(self, client, config=None):
        self.client = client
        client.logger.debug("Initializing Airflow resource...")

        # Initialize both version handlers
        self.v3 = AirflowV3(client, config)
        self.v2_11 = AirflowV2_11(client, config)

        client.logger.debug("Airflow resource initialized (v3 + v2_11)")

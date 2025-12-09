from .v3 import AirflowV3


class AirflowResource:
    def __init__(self, client, config=None):
        self.client = client
        client.logger.debug("Initializing Airflow resource...")
        self.v3 = AirflowV3(client, config)
        client.logger.debug("Airflow resource initialized")

from .v3 import AirflowV3

class AirflowResource:
    def __init__(self, client, config=None):
        self.v3 = AirflowV3(client, config)

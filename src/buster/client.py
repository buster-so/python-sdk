from .resources.airflow import AirflowResource
from typing import Optional
from .types import AirflowReportConfig

class Client:
    """
    A client for the Buster SDK.
    """
    def __init__(self, buster_api_key: Optional[str] = None, airflow_config: Optional[AirflowReportConfig] = None):
        import os
        
        # 1. Try param
        self._buster_api_key = buster_api_key
        
        # 2. Try env var
        if not self._buster_api_key:
            self._buster_api_key = os.environ.get("BUSTER_API_KEY")
            
        # 3. Fail if missing
        if not self._buster_api_key:
            raise ValueError("Buster API key must be provided via 'buster_api_key' param or 'BUSTER_API_KEY' environment variable.")
            
        self.airflow = AirflowResource(self, config=airflow_config)
    


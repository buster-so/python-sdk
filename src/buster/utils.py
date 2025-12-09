import requests
import os
from typing import Optional

def send_request(url: str, payload: dict, api_key: Optional[str] = None) -> dict:
    """
    Sends a POST request to the specified URL with the given payload and API key.
    If api_key is not provided, it attempts to load it from the BUSTER_API_KEY environment variable.
    """
    if not api_key:
        api_key = os.environ.get("BUSTER_API_KEY")
        
    if not api_key:
         raise ValueError("Buster API key must be provided via argument or 'BUSTER_API_KEY' environment variable.")
  
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    
    response = requests.post(url, json=payload, headers=headers, timeout=30)
    response.raise_for_status()
    from typing import cast, Dict, Any
    return cast(Dict[Any, Any], response.json())


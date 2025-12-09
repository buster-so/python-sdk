from typing import Literal

AirflowFlowVersion = Literal["2.5.0", "3.1"]

# API version literal type - currently only v2 is supported
ApiVersion = Literal["v2"]

# Environment literal type for API endpoints
Environment = Literal["production", "development", "staging", "development-local"]

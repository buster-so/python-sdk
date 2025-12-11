from typing import Any, Dict, cast

from pydantic import BaseModel

from buster.types import (
    AirflowEventsPayload,
    AirflowEventTriggerType,
    AirflowEventType,
    ApiVersion,
    Environment,
)

from .utils import get_airflow_version


class AirflowErrorEvent(BaseModel):
    """
    Pydantic model for validating Airflow error events before sending to API.

    The new structure sends the complete Airflow callback context to the server:
    {
        event_type: str,
        event_trigger_type: str,
        airflow_version: str,
        context: Dict[str, Any]  # Full serialized callback context
    }
    """

    event_type: AirflowEventType
    event_trigger_type: AirflowEventTriggerType
    context: Dict[str, Any]  # Serialized callback context
    api_version: ApiVersion
    env: Environment

    def to_payload(self) -> AirflowEventsPayload:
        """
        Convert the validated event to the API payload format.

        Auto-detects the Airflow version and includes it in the payload.

        Returns:
            AirflowEventsPayload ready to send to the API.
        """
        return cast(
            AirflowEventsPayload,
            {
                "event_type": self.event_type.value,
                "airflow_version": get_airflow_version(),
                "context": self.context,
                "event_trigger_type": self.event_trigger_type.value,
            },
        )

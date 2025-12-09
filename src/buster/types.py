from enum import Enum
from typing import Optional, TypedDict


class ApiVersion(str, Enum):
    V2 = "v2"


class Environment(str, Enum):
    PRODUCTION = "production"
    DEVELOPMENT = "development"
    STAGING = "staging"


class AirflowEventType(str, Enum):
    DAG_RUN_FAILED = "dag_run_failed"
    TASK_INSTANCE_FAILED = "task_instance_failed"


class AirflowEventsPayload(TypedDict):
    dag_id: str
    run_id: str
    task_id: Optional[str]
    try_number: Optional[int]
    event: str
    error_message: Optional[str]

    airflow_version: Optional[str]


class AirflowContext(TypedDict):
    dag_id: str
    run_id: str
    task_id: Optional[str]
    try_number: Optional[int]
    max_tries: Optional[int]
    exception: Optional[Exception]
    reason: Optional[str]


class AirflowReportConfig(TypedDict, total=False):
    airflow_version: Optional[str]
    api_version: ApiVersion
    env: Environment
    send_when_retries_exhausted: bool

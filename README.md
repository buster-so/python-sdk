# Buster Python SDK

The official Python SDK for Buster.

## Installation

```bash
pip install buster-sdk
```

## Quick Start

Set your API key and add Buster to your Airflow DAG:

```bash
export BUSTER_API_KEY="your-secret-key"
```

```python
from datetime import datetime
from airflow import DAG
from airflow.sdk import task
from buster import Client

client = Client()

with DAG(
    dag_id="my_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "on_failure_callback": client.airflow.v3.task_on_failure,
    },
    on_failure_callback=client.airflow.v3.dag_on_failure,
) as dag:

    @task
    def extract():
        # Your extraction logic
        pass

    @task
    def transform():
        # Your transformation logic
        pass

    extract() >> transform()
```

## Configuration

### Client Parameters

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `buster_api_key` | `str` | `None` | Your Buster API key. If not provided, uses `BUSTER_API_KEY` environment variable (recommended). |
| `debug` | `str` | `None` | Enable debug logging: `"off"`, `"error"`, `"warn"`, `"info"`, `"debug"`. |
| `env` | `str` | `"production"` | Target environment: `"production"`, `"staging"`, `"development"`. Rarely needed. |
| `api_version` | `str` | `"v2"` | API version. Currently only `"v2"` is supported. Rarely needed. |
| `airflow_config` | `dict` | `None` | Airflow-specific configuration (see Airflow Integration section). |

### Configuration Examples

**Basic:**
```python
from buster import Client

client = Client()  # Uses BUSTER_API_KEY environment variable
```

**With debug logging:**
```python
client = Client(debug="info")
```

**With explicit API key:**
```python
client = Client(buster_api_key="your-secret-key")
```

## Integrations

### Airflow

Monitor and debug your Airflow DAGs by automatically reporting task and DAG failures to Buster.

#### Basic Setup

Use `default_args` to report all task failures in your DAG:

```python
from datetime import datetime
from airflow import DAG
from airflow.sdk import task
from buster import Client

client = Client()

with DAG(
    dag_id="my_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "on_failure_callback": client.airflow.v3.task_on_failure,
    },
    on_failure_callback=client.airflow.v3.dag_on_failure,
) as dag:

    @task
    def my_task():
        # Your task logic
        pass

    my_task()
```

#### Per-Task Callbacks

For more granular control, attach callbacks to specific tasks:

```python
from airflow import DAG
from airflow.sdk import task
from buster import Client

client = Client()

with DAG(dag_id="my_dag", ...) as dag:
    @task(on_failure_callback=client.airflow.v3.task_on_failure)
    def critical_task():
        # Only this task reports failures
        pass
```

#### Plugin Integration

For centralized error reporting across all DAGs without modifying individual DAG files, use an Airflow plugin. This approach automatically captures failures from all DAGs in your Airflow instance.

Create a plugin file in your Airflow plugins directory (e.g., `plugins/buster_plugin.py`):

```python
import sys
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl
from airflow.utils.state import TaskInstanceState
from airflow.models.dagrun import DagRun
from buster import Client

client = Client()

try:
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
except ImportError:
    from airflow.models.taskinstance import TaskInstance as RuntimeTaskInstance

@hookimpl
def on_task_instance_failed(
    previous_state: TaskInstanceState,
    task_instance: RuntimeTaskInstance,
    error: str | BaseException | None,
):
    """Event listener for task instance failures."""
    client.airflow.v3.plugin_task_on_failure(
        previous_state=previous_state,
        task_instance=task_instance,
        error=error,
    )

@hookimpl
def on_dag_run_failed(dag_run: DagRun, msg: str):
    """Event listener for DAG run failures."""
    client.airflow.v3.plugin_dag_on_failure(
        dag_run=dag_run,
        msg=msg,
    )

class BusterPlugin(AirflowPlugin):
    name = "buster_plugin"
    listeners = [sys.modules[__name__]]
```

**Benefits of plugin approach:**
- Centralized error reporting for all DAGs
- No need to modify individual DAG files
- Automatically captures failures from new DAGs
- Easier to maintain and update

#### Airflow Configuration Options

Configure Airflow-specific behavior using the `airflow_config` parameter:

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `send_when_retries_exhausted` | `bool` | `True` | If `True`, only reports errors when the task has exhausted all retries. This should rarely be set to false |


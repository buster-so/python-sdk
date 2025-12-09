# Buster Python SDK

The official Python SDK for Buster.

## Installation

Install the package via pip:

```bash
pip install buster-sdk
```

## Authentication

You need a Buster API Key to use the SDK. You can provide it in one of two ways:

1.  **Environment Variable (Recommended)**: Set `BUSTER_API_KEY`.
    ```bash
    export BUSTER_API_KEY="your-secret-key"
    ```
    ```python
    client = Client()
    ```

2.  **Constructor Parameter**: Pass `buster_api_key` directly.
    ```python
    client = Client(buster_api_key="your-secret-key")
    ```

## Debug Logging

The SDK includes comprehensive logging to help you debug issues. Enable it by passing the `debug` parameter when initializing the client:

```python
from buster import Client, DebugLevel

# Enable DEBUG level logging (most verbose)
client = Client(
    buster_api_key="your-secret-key",
    debug=DebugLevel.DEBUG
)

# Or use INFO level for less verbose output
client = Client(
    buster_api_key="your-secret-key",
    debug=DebugLevel.INFO
)
```

### Available Debug Levels

| Level | Description |
| :--- | :--- |
| `DebugLevel.OFF` | No logging (default) |
| `DebugLevel.ERROR` | Only error messages |
| `DebugLevel.WARN` | Warnings and errors |
| `DebugLevel.INFO` | General information, warnings, and errors |
| `DebugLevel.DEBUG` | Detailed debugging information (most verbose) |

Logs are color-coded in your terminal for easy scanning:
- **DEBUG**: Cyan
- **INFO**: Green
- **WARNING**: Yellow
- **ERROR**: Red

> [!NOTE]
> Colors are automatically disabled when output is redirected to files or non-terminal environments.

## Configuration (Optional)

You can customize the client behavior by passing optional configuration parameters.

```python
from buster import Client, DebugLevel, Environment, ApiVersion

client = Client(
    buster_api_key="your-secret-key",
    env=Environment.STAGING,  # Optional: default is PRODUCTION
    api_version=ApiVersion.V2,  # Optional: default is V2
    airflow_config={
        "send_when_retries_exhausted": True
    },
    debug=DebugLevel.INFO  # Optional: enable logging
)
```

### Configuration Reference

> [!NOTE]
> These configuration options are **rarely needed**. The SDK is designed to work out-of-the-box with sensible defaults (Production environment, V2 API, and auto-detected Airflow version).

**Client-level parameters:**

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `buster_api_key` | `str` | `None` | Your Buster API key. If not provided, will use `BUSTER_API_KEY` environment variable. |
| `env` | `Environment` | `Environment.PRODUCTION` | The target environment. Available: `Environment.PRODUCTION`, `Environment.STAGING`, `Environment.DEVELOPMENT`. |
| `api_version` | `ApiVersion` | `ApiVersion.V2` | The Buster API version to use. Available: `ApiVersion.V2`. |
| `debug` | `DebugLevel` | `None` | Enable debug logging (`DEBUG`, `INFO`, `WARN`, `ERROR`, `OFF`). |

**Airflow-specific configuration:**

The `airflow_config` dictionary accepts the following optional keys:

| Key | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `airflow_version` | `str` | Auto-detected (defaults to `"3.1"`) | **Optional**. Manually override the auto-detected Airflow version. The SDK automatically detects the installed Airflow version via `airflow.__version__`. If detection fails, defaults to `"3.1"`. |
| `send_when_retries_exhausted` | `bool` | `True` | If `True`, only reports errors when the task has exhausted all retries. |

> [!NOTE]
> If no key is provided via either method, the SDK will raise a `ValueError`.

## Usage

### Airflow Integration

Report errors from your Airflow DAGs using the `airflow.v3` resource.

```python
from buster import Client, DebugLevel
from airflow import DAG
from airflow.operators.python import PythonOperator

# 1. Initialize Client (global or imported)
client = Client(
    debug=DebugLevel.INFO  # Optional: enable logging
)

# 2. Use in DAG definition
with DAG(
    dag_id="daily_etl_job",
    # Report if the entire DAG fails
    on_failure_callback=client.airflow.v3.dag_on_failure,
    ...
) as dag:

    # 3. Use in Task definition
    task1 = PythonOperator(
        task_id="extract_data",
        python_callable=my_func,
        # Report if this specific task fails
        on_failure_callback=client.airflow.v3.task_on_failure
    )
```

**Example log output with `debug=DebugLevel.INFO`:**
```
INFO: 📋 Reporting task_instance_failed: dag_id=daily_etl_job, run_id=scheduled_123
INFO: ✓ Event reported successfully
```

## Development

This project uses `uv` for dependency management and a `Makefile` for common tasks.

### Prerequisites

You need to have `uv` installed. It is an extremely fast Python package manager written in Rust.

**MacOS / Linux:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows:**
```powershell
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Once installed, restart your terminal.

### Setup

1. **Clone and Install Dependencies**:
   This will create a virtual environment and install the package in editable mode (`-e`).
   ```bash
   make install
   ```


2. **Run Tests**:
   > [!IMPORTANT]
   > You should always run the test suite before building or publishing.
   
   Run the test suite using `pytest`.
   ```bash
   make test
   ```

3. **Build the Package**:
   Generates distribution files in `dist/`.
   ```bash
   make build
   ```

4. **Publish**:
   Uploads the package to PyPI (requires credentials).
   ```bash
   uv publish
   ```
   > [!IMPORTANT]
   > PyPI packages are immutable. You **must** increment the `version` in `pyproject.toml` before publishing a new release. You cannot overwrite an existing version.

5. **Publish to TestPyPI (Optional)**:
   
   **What is TestPyPI?**
   [TestPyPI](https://test.pypi.org/) is a separate instance of the Python Package Index that allows you to try out distribution tools and processes without affecting the real PyPI repository. It is a sandbox for testing package publication and installation.

   **Usage:**
   1. You must create a **separate account** on [TestPyPI](https://test.pypi.org/account/register/). Your credentials from the real PyPI will not work here.
   2. Create an API token in your TestPyPI account settings.
   3. Run the following command (you will be prompted for your TestPyPI token):
      ```bash
      make publish-test
      ```
   4. This command uses `uv` to upload your package to the TestPyPI repository.

   **Installing from TestPyPI:**
   To use the package from TestPyPI in another project:
   ```bash
   pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ buster-sdk
   ```
   > [!NOTE]
   > We use `--extra-index-url https://pypi.org/simple/` to ensure that any dependencies of `buster-sdk` (like `pydantic`) can be found on the main PyPI index, as they might not be present on TestPyPI.


### Project Structure

*   **Package Name (PyPI)**: `buster-sdk`
*   **Import Name (Python)**: `buster`

Code is located in `src/buster`.

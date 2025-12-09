# Manual Logging Tests

These are manual test scripts to demonstrate the Buster SDK's logging functionality.

## Quick Start

Run from the project root directory:

```bash
# Activate virtual environment
source .venv/bin/activate

# Run any test
python tests/manual/test_visual_logs.py
python tests/manual/test_detailed_flow.py
python tests/manual/test_all_logs.py
```

## Test Files

### `test_visual_logs.py` ⭐ **Recommended**
**Best starting point!** Shows each type of log with clear section headers.

```bash
python tests/manual/test_visual_logs.py
```

**Demonstrates:**
- All color-coded log levels (DEBUG, INFO, WARNING, ERROR)
- Client initialization at different levels (DEBUG, INFO, ERROR)
- Airflow task failure scenarios
- Log level filtering behavior

### `test_detailed_flow.py`
Shows the complete, detailed flow of reporting an Airflow task failure with full DEBUG logging.

```bash
python tests/manual/test_detailed_flow.py
```

**Demonstrates:**
- Every log statement in a typical reporting flow
- Includes a breakdown of all logs with explanations

### `test_all_logs.py`
Comprehensive test covering all logging scenarios including edge cases.

```bash
python tests/manual/test_all_logs.py
```

**Demonstrates:**
- All possible logging scenarios
- Edge cases (validation errors, missing API keys)
- Multiple log level comparisons side by side

## Log Level Colors

When running in a terminal, you'll see:
- **DEBUG**: Cyan
- **INFO**: Green
- **WARNING**: Yellow
- **ERROR**: Red

Colors are automatically disabled when output is redirected to files or non-terminal environments.

## Usage Example

```python
from buster import Client, DebugLevel

# Enable DEBUG logging to see everything
client = Client(
    buster_api_key='your_key',
    debug=DebugLevel.DEBUG
)

# Or use INFO for less verbose output
client = Client(
    buster_api_key='your_key',
    debug=DebugLevel.INFO
)
```

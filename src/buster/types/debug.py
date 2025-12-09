from typing import Literal

# Debug logging level literal type
# Ordered from least to most verbose: off < error < warn < info < debug
DebugLevel = Literal["off", "error", "warn", "info", "debug"]

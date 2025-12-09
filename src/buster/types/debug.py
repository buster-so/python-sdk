from enum import Enum


class DebugLevel(str, Enum):
    """Debug level enumeration for controlling logging verbosity."""

    OFF = "off"
    ERROR = "error"
    WARN = "warn"
    INFO = "info"
    DEBUG = "debug"

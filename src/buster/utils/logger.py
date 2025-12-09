import logging
import sys
from typing import Optional

from buster.types import DebugLevel


class ColoredFormatter(logging.Formatter):
    """
    Custom formatter that adds colors to log levels for terminal output.
    Colors are only applied when output is a TTY (terminal).
    """

    # ANSI color codes
    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"
    BOLD = "\033[1m"

    def __init__(
        self, fmt: str, datefmt: Optional[str] = None, use_colors: bool = True
    ):
        super().__init__(fmt, datefmt)
        self.use_colors = (
            use_colors and hasattr(sys.stderr, "isatty") and sys.stderr.isatty()
        )

    def format(self, record: logging.LogRecord) -> str:
        if self.use_colors:
            # Save the original levelname
            levelname_original = record.levelname

            # Color the levelname
            levelname_color = self.COLORS.get(record.levelname, "")
            colored_levelname = (
                f"{self.BOLD}{levelname_color}{record.levelname}{self.RESET}"
            )
            record.levelname = colored_levelname

            # Format the message
            result = super().format(record)

            # Restore the original levelname for other handlers
            record.levelname = levelname_original

            return result
        else:
            return super().format(record)


def setup_logger(name: str, debug_level: Optional[DebugLevel] = None) -> logging.Logger:
    """
    Sets up and returns a logger with the specified debug level.

    Args:
        name: The name of the logger
        debug_level: The debug level to use (OFF, ERROR, WARN, INFO, DEBUG)

    Returns:
        A configured logger instance
    """
    logger = logging.getLogger(name)

    # Clear any existing handlers
    logger.handlers = []

    # Map DebugLevel to logging levels
    level_map = {
        DebugLevel.OFF: logging.CRITICAL + 1,  # Effectively disable logging
        DebugLevel.ERROR: logging.ERROR,
        DebugLevel.WARN: logging.WARNING,
        DebugLevel.INFO: logging.INFO,
        DebugLevel.DEBUG: logging.DEBUG,
    }

    # Set the logging level
    if debug_level and debug_level != DebugLevel.OFF:
        logger.setLevel(level_map.get(debug_level, logging.INFO))

        # Create console handler with formatting
        handler = logging.StreamHandler()
        handler.setLevel(level_map.get(debug_level, logging.INFO))
        # Flush immediately for real-time output
        handler.flush = lambda: handler.stream.flush()

        # Create colored formatter
        formatter = ColoredFormatter(
            "%(levelname)s: %(message)s",
            use_colors=True,
        )
        handler.setFormatter(formatter)

        # Add handler to logger
        logger.addHandler(handler)

        # Prevent propagation to root logger to avoid duplicate logs
        logger.propagate = False
    else:
        # Disable logging by setting to a very high level
        logger.setLevel(logging.CRITICAL + 1)

    return logger

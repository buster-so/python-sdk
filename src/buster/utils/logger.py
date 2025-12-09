import logging
from typing import Optional

from buster.types import DebugLevel


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

        # Create formatter with color-coded level names
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
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

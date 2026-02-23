"""
Logging module for the scheduler application.

Provides a centralized logging configuration for monitoring
executions, API requests, and runtime error handling.
"""

import logging
import sys
from typing import Optional

from .config import Config


def setup_logger(
    name: str, log_file: Optional[str] = None, log_level: Optional[str] = None
) -> logging.Logger:
    """
    Set up and configure a logger instance.

    Args:
        name: Name of the logger
        log_file: Optional log file path
        log_level: Optional log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)

    # Set log level
    level = log_level or Config.LOG_LEVEL
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Clear existing handlers
    logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter(Config.LOG_FORMAT)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (if log file is specified)
    file = log_file or Config.LOG_FILE
    if file:
        try:
            file_handler = logging.FileHandler(file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"Could not create file handler for {file}: {e}")

    return logger


# Default logger for the application
default_logger = setup_logger("apscheduler_oracle")

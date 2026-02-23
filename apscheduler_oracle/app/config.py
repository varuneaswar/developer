"""
Configuration module for APScheduler Oracle integration.

This module contains all configuration settings for the scheduler,
database connection, and API server.
"""

import os
from typing import Optional


class Config:
    """Configuration class for the scheduler application."""

    # Oracle Database Configuration
    ORACLE_USER: str = os.getenv("ORACLE_USER", "scheduler_user")
    ORACLE_PASSWORD: str = os.getenv("ORACLE_PASSWORD", "scheduler_password")
    ORACLE_HOST: str = os.getenv("ORACLE_HOST", "localhost")
    ORACLE_PORT: int = int(os.getenv("ORACLE_PORT", "1521"))
    ORACLE_SERVICE: str = os.getenv("ORACLE_SERVICE", "XEPDB1")

    # Build Oracle connection URI
    @classmethod
    def get_oracle_uri(cls) -> str:
        """
        Construct and return the Oracle database URI.

        Returns:
            str: Oracle database connection URI
        """
        return (
            f"oracle+cx_oracle://{cls.ORACLE_USER}:{cls.ORACLE_PASSWORD}"
            f"@{cls.ORACLE_HOST}:{cls.ORACLE_PORT}/?service_name={cls.ORACLE_SERVICE}"
        )

    # API Configuration
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")  # nosec B104
    API_PORT: int = int(os.getenv("API_PORT", "5000"))
    API_DEBUG: bool = os.getenv("API_DEBUG", "False").lower() == "true"

    # Scheduler Configuration
    SCHEDULER_JOBSTORE_TABLENAME: str = os.getenv(
        "SCHEDULER_JOBSTORE_TABLENAME", "apscheduler_jobs"
    )
    SCHEDULER_TIMEZONE: str = os.getenv("SCHEDULER_TIMEZONE", "UTC")

    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: Optional[str] = os.getenv("LOG_FILE", "scheduler.log")
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

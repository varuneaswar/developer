"""
Configuration management for APScheduler Oracle project.
Handles environment variables and global settings.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


class Config:
    """
    Configuration class for APScheduler Oracle integration.

    Environment Variables:
        ORACLE_DB_URI: Oracle database connection string
        FLASK_HOST: Flask server host (default: 0.0.0.0)
        FLASK_PORT: Flask server port (default: 5000)
        FLASK_DEBUG: Flask debug mode (default: False)
        LOG_LEVEL: Logging level (default: INFO)
        LOG_FILE: Log file path (default: logs/scheduler.log)
    """

    # Oracle Database Configuration
    ORACLE_DB_URI = os.getenv(
        "ORACLE_DB_URI", "oracle+cx_oracle://username:password@host:port/service_name"
    )

    # Flask Configuration
    FLASK_HOST = os.getenv("FLASK_HOST", "0.0.0.0")
    FLASK_PORT = int(os.getenv("FLASK_PORT", 5000))
    FLASK_DEBUG = os.getenv("FLASK_DEBUG", "False").lower() == "true"

    # Logging Configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("LOG_FILE", "logs/scheduler.log")

    # APScheduler Configuration
    SCHEDULER_TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "UTC")
    SCHEDULER_JOB_DEFAULTS = {"coalesce": False, "max_instances": 3}

    @classmethod
    def validate(cls):
        """Validate critical configuration settings."""
        if "username:password@host:port" in cls.ORACLE_DB_URI:
            raise ValueError(
                "ORACLE_DB_URI not properly configured. "
                "Please set the environment variable or update .env file."
            )
        return True

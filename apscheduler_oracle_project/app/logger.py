"""
Logging configuration for the APScheduler Oracle project.
Provides centralized logging setup for all modules.
"""
import logging
import os
from pathlib import Path
from logging.handlers import RotatingFileHandler


def setup_logger(name: str = 'apscheduler_oracle', log_level: str = 'INFO', log_file: str = None):
    """
    Configure and return a logger instance.
    
    Args:
        name: Logger name (default: 'apscheduler_oracle')
        log_level: Logging level (default: 'INFO')
        log_file: Path to log file (optional)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Set log level
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if log_file is specified)
    if log_file:
        # Create logs directory if it doesn't exist
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str = None):
    """
    Get or create a logger instance.
    
    Args:
        name: Logger name (optional, uses 'apscheduler_oracle' if not provided)
    
    Returns:
        logging.Logger: Logger instance
    """
    if name:
        return logging.getLogger(f'apscheduler_oracle.{name}')
    return logging.getLogger('apscheduler_oracle')

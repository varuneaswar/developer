"""
Main entry point for the APScheduler Oracle project.
Initializes and starts the scheduler and Flask API server.
"""
import signal
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.config import Config
from app.logger import setup_logger
from app.scheduler import SchedulerManager
from app.api import init_app, run_app

logger = None


def signal_handler(sig, frame):
    """Handle shutdown signals gracefully."""
    logger.info("Shutdown signal received. Stopping scheduler...")
    if scheduler_manager:
        scheduler_manager.shutdown(wait=True)
    logger.info("Scheduler stopped. Exiting.")
    sys.exit(0)


def main():
    """Main entry point."""
    global logger, scheduler_manager
    
    try:
        # Setup logging
        logger = setup_logger(
            name='apscheduler_oracle',
            log_level=Config.LOG_LEVEL,
            log_file=Config.LOG_FILE
        )
        
        logger.info("="*60)
        logger.info("Starting APScheduler Oracle Integration")
        logger.info("="*60)
        
        # Validate configuration (skip validation for default placeholder)
        try:
            if 'username:password@host:port' not in Config.ORACLE_DB_URI:
                Config.validate()
                logger.info("Configuration validated successfully")
            else:
                logger.warning(
                    "Using default Oracle DB URI. "
                    "Please configure ORACLE_DB_URI in .env file for production use."
                )
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
            sys.exit(1)
        
        # Initialize scheduler
        logger.info("Initializing scheduler...")
        scheduler_manager = SchedulerManager()
        scheduler_manager.start()
        logger.info("Scheduler started successfully")
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Initialize and run Flask app
        logger.info("Initializing Flask API...")
        init_app(scheduler_manager)
        
        logger.info(f"Starting Flask API on {Config.FLASK_HOST}:{Config.FLASK_PORT}")
        run_app()
    
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down...")
        if scheduler_manager:
            scheduler_manager.shutdown(wait=True)
        sys.exit(0)
    
    except Exception as e:
        if logger:
            logger.error(f"Fatal error: {str(e)}", exc_info=True)
        else:
            print(f"Fatal error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()

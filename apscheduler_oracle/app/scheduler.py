"""
Scheduler module for APScheduler with Oracle backend.

This module sets up the APScheduler with Oracle database as job store
and provides functions for job management.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from apscheduler.executors.pool import ProcessPoolExecutor, ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from pytz import timezone as pytz_timezone

from .config import Config
from .jobs import job_wrapper
from .logger import setup_logger

logger = setup_logger("scheduler")


class SchedulerManager:
    """Manager class for APScheduler with Oracle backend."""

    def __init__(self):
        """Initialize the scheduler with Oracle job store."""
        self.scheduler = None
        self._setup_scheduler()

    def _setup_scheduler(self):
        """Set up APScheduler with Oracle database backend."""
        try:
            # Configure job store
            jobstores = {
                "default": SQLAlchemyJobStore(
                    url=Config.get_oracle_uri(), tablename=Config.SCHEDULER_JOBSTORE_TABLENAME
                )
            }

            # Configure executors
            executors = {"default": ThreadPoolExecutor(20), "processpool": ProcessPoolExecutor(5)}

            # Job defaults
            job_defaults = {"coalesce": False, "max_instances": 3}

            # Create scheduler
            self.scheduler = BackgroundScheduler(
                jobstores=jobstores,
                executors=executors,
                job_defaults=job_defaults,
                timezone=pytz_timezone(Config.SCHEDULER_TIMEZONE),
            )

            logger.info("Scheduler initialized successfully with Oracle backend")

        except Exception as e:
            logger.error(f"Failed to initialize scheduler: {str(e)}")
            raise

    def start(self):
        """Start the scheduler."""
        if self.scheduler and not self.scheduler.running:
            self.scheduler.start()
            logger.info("Scheduler started")
        else:
            logger.warning("Scheduler is already running or not initialized")

    def shutdown(self, wait: bool = True):
        """
        Shutdown the scheduler.

        Args:
            wait: Whether to wait for jobs to complete before shutting down
        """
        if self.scheduler and self.scheduler.running:
            self.scheduler.shutdown(wait=wait)
            logger.info("Scheduler shut down")
        else:
            logger.warning("Scheduler is not running")

    def add_bash_job(
        self, job_id: str, script_path: str, run_date: datetime, args: Optional[list] = None
    ) -> str:
        """
        Schedule a Bash script to run at a specific date/time.

        Args:
            job_id: Unique identifier for the job
            script_path: Path to the Bash script
            run_date: Date and time to run the job
            args: Optional arguments for the script

        Returns:
            str: Job ID
        """
        logger.info(f"Adding Bash job: {job_id} to run at {run_date}")

        try:
            job = self.scheduler.add_job(
                func=job_wrapper,
                trigger=DateTrigger(run_date=run_date),
                args=[],
                kwargs={"job_type": "bash", "script_path": script_path, "args": args or []},
                id=job_id,
                name=f"Bash: {script_path}",
                replace_existing=False,
            )

            logger.info(f"Bash job added successfully: {job_id}")
            return job.id

        except Exception as e:
            logger.error(f"Failed to add Bash job {job_id}: {str(e)}")
            raise

    def add_python_job(
        self,
        job_id: str,
        module_name: str,
        run_date: datetime,
        function_name: str = "main",
        args: Optional[list] = None,
        kwargs: Optional[dict] = None,
    ) -> str:
        """
        Schedule a Python module to run at a specific date/time.

        Args:
            job_id: Unique identifier for the job
            module_name: Name of the Python module
            run_date: Date and time to run the job
            function_name: Name of the function to execute (default: 'main')
            args: Optional positional arguments
            kwargs: Optional keyword arguments

        Returns:
            str: Job ID
        """
        logger.info(f"Adding Python job: {job_id} to run at {run_date}")

        try:
            job = self.scheduler.add_job(
                func=job_wrapper,
                trigger=DateTrigger(run_date=run_date),
                args=[],
                kwargs={
                    "job_type": "python",
                    "module_name": module_name,
                    "function_name": function_name,
                    "args": args or [],
                    "kwargs": kwargs or {},
                },
                id=job_id,
                name=f"Python: {module_name}.{function_name}",
                replace_existing=False,
            )

            logger.info(f"Python job added successfully: {job_id}")
            return job.id

        except Exception as e:
            logger.error(f"Failed to add Python job {job_id}: {str(e)}")
            raise

    def remove_job(self, job_id: str) -> bool:
        """
        Remove a job from the scheduler.

        Args:
            job_id: ID of the job to remove

        Returns:
            bool: True if job was removed, False otherwise
        """
        logger.info(f"Removing job: {job_id}")

        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"Job removed successfully: {job_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to remove job {job_id}: {str(e)}")
            return False

    def pause_job(self, job_id: str) -> bool:
        """
        Pause a job.

        Args:
            job_id: ID of the job to pause

        Returns:
            bool: True if job was paused, False otherwise
        """
        logger.info(f"Pausing job: {job_id}")

        try:
            self.scheduler.pause_job(job_id)
            logger.info(f"Job paused successfully: {job_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to pause job {job_id}: {str(e)}")
            return False

    def resume_job(self, job_id: str) -> bool:
        """
        Resume a paused job.

        Args:
            job_id: ID of the job to resume

        Returns:
            bool: True if job was resumed, False otherwise
        """
        logger.info(f"Resuming job: {job_id}")

        try:
            self.scheduler.resume_job(job_id)
            logger.info(f"Job resumed successfully: {job_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to resume job {job_id}: {str(e)}")
            return False

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get details of a specific job.

        Args:
            job_id: ID of the job

        Returns:
            Optional[Dict[str, Any]]: Job details or None if not found
        """
        try:
            job = self.scheduler.get_job(job_id)
            if job:
                return {
                    "id": job.id,
                    "name": job.name,
                    "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
                    "trigger": str(job.trigger),
                }
            return None

        except Exception as e:
            logger.error(f"Failed to get job {job_id}: {str(e)}")
            return None

    def get_all_jobs(self) -> list:
        """
        Get all scheduled jobs.

        Returns:
            list: List of job details
        """
        try:
            jobs = self.scheduler.get_jobs()
            return [
                {
                    "id": job.id,
                    "name": job.name,
                    "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
                    "trigger": str(job.trigger),
                }
                for job in jobs
            ]

        except Exception as e:
            logger.error(f"Failed to get all jobs: {str(e)}")
            return []


# Global scheduler instance
scheduler_manager = None


def get_scheduler_manager() -> SchedulerManager:
    """
    Get or create the global scheduler manager instance.

    Returns:
        SchedulerManager: The scheduler manager instance
    """
    global scheduler_manager
    if scheduler_manager is None:
        scheduler_manager = SchedulerManager()
    return scheduler_manager

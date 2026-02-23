"""
Scheduler setup and job management with Oracle database backend.
Handles APScheduler configuration, job store, and job lifecycle management.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from app.config import Config
from app.jobs import execute_job
from app.logger import get_logger
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from pytz import timezone as pytz_timezone

logger = get_logger("scheduler")


class SchedulerManager:
    """
    Manages the APScheduler instance with Oracle database backend.
    Provides methods to add, remove, pause, resume, and list jobs.
    """

    def __init__(self, db_uri: str = None, tz: str = None):
        """
        Initialize the scheduler manager.

        Args:
            db_uri: Oracle database connection URI (uses Config.ORACLE_DB_URI if not provided)
            tz: Timezone for the scheduler (uses Config.SCHEDULER_TIMEZONE if not provided)
        """
        self.db_uri = db_uri or Config.ORACLE_DB_URI
        self.timezone = pytz_timezone(tz or Config.SCHEDULER_TIMEZONE)
        self.scheduler = None
        self._initialize_scheduler()

    def _initialize_scheduler(self):
        """Initialize APScheduler with Oracle job store."""
        try:
            # Configure job store
            jobstores = {"default": SQLAlchemyJobStore(url=self.db_uri)}

            # Configure executors
            executors = {"default": ThreadPoolExecutor(max_workers=10)}

            # Create scheduler
            self.scheduler = BackgroundScheduler(
                jobstores=jobstores,
                executors=executors,
                job_defaults=Config.SCHEDULER_JOB_DEFAULTS,
                timezone=self.timezone,
            )

            logger.info("Scheduler initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize scheduler: {str(e)}", exc_info=True)
            raise

    def start(self):
        """Start the scheduler."""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Scheduler started")
        else:
            logger.warning("Scheduler is already running")

    def shutdown(self, wait: bool = True):
        """
        Shutdown the scheduler.

        Args:
            wait: If True, wait for all jobs to complete before shutting down
        """
        if self.scheduler.running:
            self.scheduler.shutdown(wait=wait)
            logger.info("Scheduler shut down")
        else:
            logger.warning("Scheduler is not running")

    def add_job(
        self,
        job_id: str,
        job_type: str,
        script_path: str,
        run_date: datetime,
        args: List[str] = None,
        replace_existing: bool = False,
    ) -> Dict[str, Any]:
        """
        Add a new job to the scheduler.

        Args:
            job_id: Unique identifier for the job
            job_type: Type of job ('bash' or 'python')
            script_path: Path to the script to execute
            run_date: Date and time to execute the job
            args: Arguments to pass to the script (optional)
            replace_existing: If True, replace existing job with same ID

        Returns:
            Dict containing job details

        Raises:
            ValueError: If job parameters are invalid
        """
        if not job_id:
            raise ValueError("job_id is required")

        if not job_type or job_type.lower() not in ["bash", "python"]:
            raise ValueError("job_type must be 'bash' or 'python'")

        if not script_path:
            raise ValueError("script_path is required")

        if not run_date:
            raise ValueError("run_date is required")

        # Ensure run_date is timezone-aware
        if run_date.tzinfo is None:
            run_date = self.timezone.localize(run_date)

        try:
            # Create date trigger
            trigger = DateTrigger(run_date=run_date, timezone=self.timezone)

            # Add job to scheduler
            job = self.scheduler.add_job(
                func=execute_job,
                trigger=trigger,
                args=[job_type, script_path, args],
                id=job_id,
                name=f"{job_type}:{script_path}",
                replace_existing=replace_existing,
            )

            logger.info(f"Job added: {job_id} ({job_type}:{script_path}) scheduled for {run_date}")

            return {
                "job_id": job.id,
                "name": job.name,
                "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
                "job_type": job_type,
                "script_path": script_path,
                "args": args,
            }

        except Exception as e:
            logger.error(f"Failed to add job {job_id}: {str(e)}", exc_info=True)
            raise

    def remove_job(self, job_id: str) -> bool:
        """
        Remove a job from the scheduler.

        Args:
            job_id: ID of the job to remove

        Returns:
            True if job was removed, False otherwise

        Raises:
            ValueError: If job_id is not provided
        """
        if not job_id:
            raise ValueError("job_id is required")

        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"Job removed: {job_id}")
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
            True if job was paused, False otherwise

        Raises:
            ValueError: If job_id is not provided
        """
        if not job_id:
            raise ValueError("job_id is required")

        try:
            self.scheduler.pause_job(job_id)
            logger.info(f"Job paused: {job_id}")
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
            True if job was resumed, False otherwise

        Raises:
            ValueError: If job_id is not provided
        """
        if not job_id:
            raise ValueError("job_id is required")

        try:
            self.scheduler.resume_job(job_id)
            logger.info(f"Job resumed: {job_id}")
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
            Dict containing job details or None if job not found
        """
        try:
            job = self.scheduler.get_job(job_id)
            if job:
                return self._job_to_dict(job)
            return None

        except Exception as e:
            logger.error(f"Failed to get job {job_id}: {str(e)}")
            return None

    def list_jobs(self) -> List[Dict[str, Any]]:
        """
        Get list of all scheduled jobs.

        Returns:
            List of job dictionaries
        """
        try:
            jobs = self.scheduler.get_jobs()
            return [self._job_to_dict(job) for job in jobs]

        except Exception as e:
            logger.error(f"Failed to list jobs: {str(e)}")
            return []

    def _job_to_dict(self, job) -> Dict[str, Any]:
        """
        Convert APScheduler job object to dictionary.

        Args:
            job: APScheduler job object

        Returns:
            Dict containing job details
        """
        return {
            "job_id": job.id,
            "name": job.name,
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger),
            "executor": job.executor,
            "pending": job.pending,
        }

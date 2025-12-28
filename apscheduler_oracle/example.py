#!/usr/bin/env python3
"""
Example demonstrating the APScheduler Oracle job scheduling module.

This example shows how to:
1. Set up the scheduler
2. Schedule Bash and Python jobs
3. List and manage jobs
"""

import sys
import os
from datetime import datetime, timedelta

# Add the parent directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.scheduler import get_scheduler_manager
from app.logger import setup_logger

# Set up logger
logger = setup_logger("example")


def example_function(name: str, greeting: str = "Hello"):
    """Example function for Python job execution."""
    message = f"{greeting}, {name}!"
    logger.info(f"Example function called: {message}")
    return message


def main():
    """Main example function."""
    logger.info("Starting APScheduler Oracle example")

    # Get scheduler manager
    scheduler = get_scheduler_manager()

    # Start the scheduler
    scheduler.start()
    logger.info("Scheduler started")

    try:
        # Schedule a Bash job (example - will fail if script doesn't exist)
        try:
            bash_job_time = datetime.now() + timedelta(seconds=10)
            bash_job_id = scheduler.add_bash_job(
                job_id="example_bash_job",
                script_path="/bin/echo",
                run_date=bash_job_time,
                args=["Hello from Bash!"],
            )
            logger.info(f"Scheduled Bash job: {bash_job_id} at {bash_job_time}")
        except Exception as e:
            logger.error(f"Failed to schedule Bash job: {e}")

        # Schedule a Python job
        try:
            python_job_time = datetime.now() + timedelta(seconds=15)
            python_job_id = scheduler.add_python_job(
                job_id="example_python_job",
                module_name="__main__",
                function_name="example_function",
                run_date=python_job_time,
                args=["World"],
                kwargs={"greeting": "Greetings"},
            )
            logger.info(f"Scheduled Python job: {python_job_id} at {python_job_time}")
        except Exception as e:
            logger.error(f"Failed to schedule Python job: {e}")

        # List all jobs
        jobs = scheduler.get_all_jobs()
        logger.info(f"Total jobs scheduled: {len(jobs)}")
        for job in jobs:
            logger.info(
                f"  Job: {job['id']} - {job['name']} - "
                f"Next run: {job['next_run_time']}"
            )

        # Get specific job details
        job_details = scheduler.get_job("example_bash_job")
        if job_details:
            logger.info(f"Job details: {job_details}")

        # Pause a job
        if scheduler.pause_job("example_bash_job"):
            logger.info("Paused example_bash_job")

        # Resume a job
        if scheduler.resume_job("example_bash_job"):
            logger.info("Resumed example_bash_job")

        # Keep the script running for a bit to let jobs execute
        logger.info("Waiting for jobs to execute... (30 seconds)")
        import time

        time.sleep(30)

        # Remove jobs
        for job_id in ["example_bash_job", "example_python_job"]:
            if scheduler.remove_job(job_id):
                logger.info(f"Removed job: {job_id}")

    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        # Shutdown the scheduler
        scheduler.shutdown()
        logger.info("Scheduler shut down")


if __name__ == "__main__":
    main()

"""
REST API for job management using Flask.
Provides endpoints for scheduling, removing, pausing, resuming, and listing jobs.
"""
from datetime import datetime
from flask import Flask, request, jsonify

from app.config import Config
from app.scheduler import SchedulerManager
from app.logger import get_logger

# Initialize Flask app
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

# Initialize logger
logger = get_logger("api")

# Global scheduler instance
scheduler_manager: SchedulerManager = None


def init_app(scheduler: SchedulerManager = None):
    """
    Initialize the Flask application with a scheduler instance.

    Args:
        scheduler: SchedulerManager instance (creates new if not provided)
    """
    global scheduler_manager

    if scheduler:
        scheduler_manager = scheduler
    else:
        scheduler_manager = SchedulerManager()
        scheduler_manager.start()

    logger.info("Flask app initialized")


def parse_datetime(date_str: str) -> datetime:
    """
    Parse datetime string to datetime object.
    Supports ISO 8601 format: YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS

    Args:
        date_str: Datetime string

    Returns:
        datetime object

    Raises:
        ValueError: If datetime format is invalid
    """
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    raise ValueError(
        f"Invalid datetime format: {date_str}. "
        f"Expected format: YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS"
    )


@app.route("/api/health", methods=["GET"])
def health_check():
    """
    Health check endpoint.

    Returns:
        JSON response with status
    """
    return (
        jsonify(
            {
                "status": "healthy",
                "scheduler_running": scheduler_manager.scheduler.running
                if scheduler_manager
                else False,
            }
        ),
        200,
    )


@app.route("/api/schedule-job", methods=["POST"])
def schedule_job():
    """
    Schedule a new job.

    Request Body (JSON):
        {
            "job_id": "unique_job_id",
            "job_type": "bash" or "python",
            "script_path": "/path/to/script",
            "run_date": "2024-12-31T23:59:59",
            "args": ["arg1", "arg2"],  # optional
            "replace_existing": false  # optional
        }

    Returns:
        JSON response with job details or error message
    """
    try:
        data = request.get_json()

        # Validate required fields
        required_fields = ["job_id", "job_type", "script_path", "run_date"]
        missing_fields = [field for field in required_fields if field not in data]

        if missing_fields:
            return jsonify({"error": f"Missing required fields: {', '.join(missing_fields)}"}), 400

        # Extract data
        job_id = data["job_id"]
        job_type = data["job_type"]
        script_path = data["script_path"]
        run_date_str = data["run_date"]
        args = data.get("args", [])
        replace_existing = data.get("replace_existing", False)

        # Validate job_type
        if job_type.lower() not in ["bash", "python"]:
            return jsonify({"error": "job_type must be 'bash' or 'python'"}), 400

        # Parse run_date
        try:
            run_date = parse_datetime(run_date_str)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

        # Validate run_date is in the future
        if run_date <= datetime.now():
            return jsonify({"error": "run_date must be in the future"}), 400

        # Add job
        job_details = scheduler_manager.add_job(
            job_id=job_id,
            job_type=job_type,
            script_path=script_path,
            run_date=run_date,
            args=args,
            replace_existing=replace_existing,
        )

        logger.info(f"Job scheduled via API: {job_id}")

        return jsonify({"message": "Job scheduled successfully", "job": job_details}), 200

    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.error(f"Error scheduling job: {str(e)}", exc_info=True)
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@app.route("/api/remove-job/<job_id>", methods=["DELETE"])
def remove_job(job_id: str):
    """
    Remove a scheduled job.

    URL Parameters:
        job_id: ID of the job to remove

    Returns:
        JSON response with success message or error
    """
    try:
        if not job_id:
            return jsonify({"error": "job_id is required"}), 400

        # Check if job exists
        job = scheduler_manager.get_job(job_id)
        if not job:
            return jsonify({"error": f"Job not found: {job_id}"}), 404

        # Remove job
        success = scheduler_manager.remove_job(job_id)

        if success:
            logger.info(f"Job removed via API: {job_id}")
            return jsonify({"message": f"Job {job_id} removed successfully"}), 200
        else:
            return jsonify({"error": f"Failed to remove job: {job_id}"}), 500

    except Exception as e:
        logger.error(f"Error removing job: {str(e)}", exc_info=True)
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@app.route("/api/jobs", methods=["GET"])
def list_jobs():
    """
    List all scheduled jobs.

    Returns:
        JSON response with list of jobs
    """
    try:
        jobs = scheduler_manager.list_jobs()

        return jsonify({"count": len(jobs), "jobs": jobs}), 200

    except Exception as e:
        logger.error(f"Error listing jobs: {str(e)}", exc_info=True)
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@app.route("/api/pause-job/<job_id>", methods=["PUT"])
def pause_job(job_id: str):
    """
    Pause a scheduled job.

    URL Parameters:
        job_id: ID of the job to pause

    Returns:
        JSON response with success message or error
    """
    try:
        if not job_id:
            return jsonify({"error": "job_id is required"}), 400

        # Check if job exists
        job = scheduler_manager.get_job(job_id)
        if not job:
            return jsonify({"error": f"Job not found: {job_id}"}), 404

        # Pause job
        success = scheduler_manager.pause_job(job_id)

        if success:
            logger.info(f"Job paused via API: {job_id}")
            return jsonify({"message": f"Job {job_id} paused successfully"}), 200
        else:
            return jsonify({"error": f"Failed to pause job: {job_id}"}), 500

    except Exception as e:
        logger.error(f"Error pausing job: {str(e)}", exc_info=True)
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@app.route("/api/resume-job/<job_id>", methods=["PUT"])
def resume_job(job_id: str):
    """
    Resume a paused job.

    URL Parameters:
        job_id: ID of the job to resume

    Returns:
        JSON response with success message or error
    """
    try:
        if not job_id:
            return jsonify({"error": "job_id is required"}), 400

        # Check if job exists
        job = scheduler_manager.get_job(job_id)
        if not job:
            return jsonify({"error": f"Job not found: {job_id}"}), 404

        # Resume job
        success = scheduler_manager.resume_job(job_id)

        if success:
            logger.info(f"Job resumed via API: {job_id}")
            return jsonify({"message": f"Job {job_id} resumed successfully"}), 200
        else:
            return jsonify({"error": f"Failed to resume job: {job_id}"}), 500

    except Exception as e:
        logger.error(f"Error resuming job: {str(e)}", exc_info=True)
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@app.route("/api/job/<job_id>", methods=["GET"])
def get_job(job_id: str):
    """
    Get details of a specific job.

    URL Parameters:
        job_id: ID of the job

    Returns:
        JSON response with job details or error
    """
    try:
        if not job_id:
            return jsonify({"error": "job_id is required"}), 400

        job = scheduler_manager.get_job(job_id)

        if job:
            return jsonify({"job": job}), 200
        else:
            return jsonify({"error": f"Job not found: {job_id}"}), 404

    except Exception as e:
        logger.error(f"Error getting job: {str(e)}", exc_info=True)
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors."""
    return jsonify({"error": "Endpoint not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors."""
    return jsonify({"error": "Internal server error"}), 500


def run_app(host: str = None, port: int = None, debug: bool = None):
    """
    Run the Flask application.

    Args:
        host: Host to bind to (uses Config.FLASK_HOST if not provided)
        port: Port to bind to (uses Config.FLASK_PORT if not provided)
        debug: Debug mode (uses Config.FLASK_DEBUG if not provided)
    """
    host = host or Config.FLASK_HOST
    port = port or Config.FLASK_PORT
    debug = debug if debug is not None else Config.FLASK_DEBUG

    logger.info(f"Starting Flask app on {host}:{port} (debug={debug})")
    app.run(host=host, port=port, debug=debug, use_reloader=False)

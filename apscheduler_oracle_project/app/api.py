"""
REST API for job management using Flask-RESTX with Swagger UI.
Provides endpoints for scheduling, removing, pausing, resuming, and listing jobs.
"""
from datetime import datetime
from flask import Flask, request
from flask_restx import Api, Resource, fields

from app.config import Config
from app.scheduler import SchedulerManager
from app.logger import get_logger

# Initialize Flask app
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

# Initialize Flask-RESTX with Swagger UI configuration
api = Api(
    app,
    version='1.0',
    title='APScheduler Job Management API',
    description='REST API for scheduling, managing and monitoring jobs using APScheduler with Oracle backend',
    doc='/',  # Swagger UI available at root
    prefix='/api'
)

# Create namespaces
ns = api.namespace('', description='Job management operations')

# Initialize logger
logger = get_logger("api")

# Global scheduler instance
scheduler_manager: SchedulerManager = None

# Define API models for request/response documentation
schedule_job_model = api.model('ScheduleJob', {
    'job_id': fields.String(required=True, description='Unique identifier for the job', example='my_job_1'),
    'job_type': fields.String(required=True, description='Type of job', enum=['bash', 'python'], example='bash'),
    'script_path': fields.String(required=True, description='Full path to the script file', example='/path/to/script.sh'),
    'run_date': fields.String(required=True, description='ISO 8601 datetime string (YYYY-MM-DDTHH:MM:SS)', example='2024-12-31T23:59:59'),
    'args': fields.List(fields.String, description='List of arguments to pass to the script', example=['arg1', 'arg2']),
    'replace_existing': fields.Boolean(description='Replace existing job with same ID', default=False, example=False)
})

job_details_model = api.model('JobDetails', {
    'job_id': fields.String(description='Job identifier'),
    'name': fields.String(description='Job name'),
    'next_run_time': fields.String(description='Next scheduled run time'),
    'job_type': fields.String(description='Type of job'),
    'script_path': fields.String(description='Path to the script'),
    'args': fields.List(fields.String, description='Job arguments'),
    'trigger': fields.String(description='Trigger information'),
    'executor': fields.String(description='Executor name'),
    'pending': fields.Boolean(description='Is job pending')
})

success_response_model = api.model('SuccessResponse', {
    'message': fields.String(description='Success message'),
    'job': fields.Nested(job_details_model, description='Job details')
})

error_response_model = api.model('ErrorResponse', {
    'error': fields.String(description='Error message')
})

jobs_list_model = api.model('JobsList', {
    'count': fields.Integer(description='Number of jobs'),
    'jobs': fields.List(fields.Nested(job_details_model), description='List of jobs')
})

job_response_model = api.model('JobResponse', {
    'job': fields.Nested(job_details_model, description='Job details')
})

health_response_model = api.model('HealthResponse', {
    'status': fields.String(description='Service status'),
    'scheduler_running': fields.Boolean(description='Is scheduler running')
})

message_response_model = api.model('MessageResponse', {
    'message': fields.String(description='Response message')
})


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




# Resource classes for endpoints
@ns.route('/health')
class HealthCheck(Resource):
    """Health check endpoint"""
    
    @ns.doc('health_check', description='Check if the service is running')
    @ns.response(200, 'Success', health_response_model)
    def get(self):
        """Check if the service is running"""
        return {
            "status": "healthy",
            "scheduler_running": scheduler_manager.scheduler.running if scheduler_manager else False,
        }, 200


@ns.route('/schedule-job')
class ScheduleJob(Resource):
    """Schedule a new job"""
    
    @ns.doc('schedule_job', description='Schedule a new Bash or Python script execution')
    @ns.expect(schedule_job_model)
    @ns.response(200, 'Job scheduled successfully', success_response_model)
    @ns.response(400, 'Validation error', error_response_model)
    @ns.response(500, 'Internal server error', error_response_model)
    def post(self):
        """Schedule a new job"""
        data = api.payload

        # Validate required fields
        required_fields = ["job_id", "job_type", "script_path", "run_date"]
        missing_fields = [field for field in required_fields if field not in data]

        if missing_fields:
            return {"error": f"Missing required fields: {', '.join(missing_fields)}"}, 400

        # Extract data
        job_id = data["job_id"]
        job_type = data["job_type"]
        script_path = data["script_path"]
        run_date_str = data["run_date"]
        args = data.get("args", [])
        replace_existing = data.get("replace_existing", False)

        # Validate job_type
        if job_type.lower() not in ["bash", "python"]:
            return {"error": "job_type must be 'bash' or 'python'"}, 400

        # Parse run_date
        try:
            run_date = parse_datetime(run_date_str)
        except ValueError as e:
            return {"error": str(e)}, 400

        # Validate run_date is in the future
        if run_date <= datetime.now():
            return {"error": "run_date must be in the future"}, 400

        # Add job
        try:
            job_details = scheduler_manager.add_job(
                job_id=job_id,
                job_type=job_type,
                script_path=script_path,
                run_date=run_date,
                args=args,
                replace_existing=replace_existing,
            )

            logger.info(f"Job scheduled via API: {job_id}")

            return {"message": "Job scheduled successfully", "job": job_details}, 200
        except Exception as e:
            logger.error(f"Error scheduling job: {str(e)}", exc_info=True)
            return {"error": f"Internal server error: {str(e)}"}, 500


@ns.route('/remove-job/<string:job_id>')
@ns.param('job_id', 'The job identifier')
class RemoveJob(Resource):
    """Remove a scheduled job"""
    
    @ns.doc('remove_job', description='Remove a scheduled job by ID')
    @ns.response(200, 'Job removed successfully', message_response_model)
    @ns.response(400, 'Validation error', error_response_model)
    @ns.response(404, 'Job not found', error_response_model)
    @ns.response(500, 'Internal server error', error_response_model)
    def delete(self, job_id):
        """Remove a scheduled job"""
        if not job_id:
            return {"error": "job_id is required"}, 400

        # Check if job exists
        job = scheduler_manager.get_job(job_id)
        if not job:
            return {"error": f"Job not found: {job_id}"}, 404

        # Remove job
        success = scheduler_manager.remove_job(job_id)

        if success:
            logger.info(f"Job removed via API: {job_id}")
            return {"message": f"Job {job_id} removed successfully"}, 200
        else:
            return {"error": f"Failed to remove job: {job_id}"}, 500


@ns.route('/jobs')
class ListJobs(Resource):
    """List all scheduled jobs"""
    
    @ns.doc('list_jobs', description='Get a list of all scheduled jobs')
    @ns.response(200, 'List of jobs', jobs_list_model)
    @ns.response(500, 'Internal server error', error_response_model)
    def get(self):
        """List all scheduled jobs"""
        try:
            jobs = scheduler_manager.list_jobs()
            return {"count": len(jobs), "jobs": jobs}, 200
        except Exception as e:
            logger.error(f"Error listing jobs: {str(e)}", exc_info=True)
            return {"error": f"Internal server error: {str(e)}"}, 500


@ns.route('/pause-job/<string:job_id>')
@ns.param('job_id', 'The job identifier')
class PauseJob(Resource):
    """Pause a scheduled job"""
    
    @ns.doc('pause_job', description='Pause a scheduled job by ID')
    @ns.response(200, 'Job paused successfully', message_response_model)
    @ns.response(400, 'Validation error', error_response_model)
    @ns.response(404, 'Job not found', error_response_model)
    @ns.response(500, 'Internal server error', error_response_model)
    def put(self, job_id):
        """Pause a scheduled job"""
        if not job_id:
            return {"error": "job_id is required"}, 400

        # Check if job exists
        job = scheduler_manager.get_job(job_id)
        if not job:
            return {"error": f"Job not found: {job_id}"}, 404

        # Pause job
        success = scheduler_manager.pause_job(job_id)

        if success:
            logger.info(f"Job paused via API: {job_id}")
            return {"message": f"Job {job_id} paused successfully"}, 200
        else:
            return {"error": f"Failed to pause job: {job_id}"}, 500


@ns.route('/resume-job/<string:job_id>')
@ns.param('job_id', 'The job identifier')
class ResumeJob(Resource):
    """Resume a paused job"""
    
    @ns.doc('resume_job', description='Resume a paused job by ID')
    @ns.response(200, 'Job resumed successfully', message_response_model)
    @ns.response(400, 'Validation error', error_response_model)
    @ns.response(404, 'Job not found', error_response_model)
    @ns.response(500, 'Internal server error', error_response_model)
    def put(self, job_id):
        """Resume a paused job"""
        if not job_id:
            return {"error": "job_id is required"}, 400

        # Check if job exists
        job = scheduler_manager.get_job(job_id)
        if not job:
            return {"error": f"Job not found: {job_id}"}, 404

        # Resume job
        success = scheduler_manager.resume_job(job_id)

        if success:
            logger.info(f"Job resumed via API: {job_id}")
            return {"message": f"Job {job_id} resumed successfully"}, 200
        else:
            return {"error": f"Failed to resume job: {job_id}"}, 500


@ns.route('/job/<string:job_id>')
@ns.param('job_id', 'The job identifier')
class GetJob(Resource):
    """Get details of a specific job"""
    
    @ns.doc('get_job', description='Get details of a specific job by ID')
    @ns.response(200, 'Job details', job_response_model)
    @ns.response(400, 'Validation error', error_response_model)
    @ns.response(404, 'Job not found', error_response_model)
    @ns.response(500, 'Internal server error', error_response_model)
    def get(self, job_id):
        """Get details of a specific job"""
        if not job_id:
            return {"error": "job_id is required"}, 400

        job = scheduler_manager.get_job(job_id)

        if job:
            return {"job": job}, 200
        else:
            return {"error": f"Job not found: {job_id}"}, 404


# Add a route for /docs to redirect to Swagger UI at /
@app.route('/docs')
def docs_redirect():
    """Redirect /docs to Swagger UI at root"""
    from flask import redirect
    return redirect('/', code=302)


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

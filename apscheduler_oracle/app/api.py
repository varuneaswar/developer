"""
REST API module for job scheduling management.

This module provides Flask-based REST API endpoints for CRUD operations
on scheduled jobs.
"""

from datetime import datetime, timezone
from flask import Flask, request, jsonify
from typing import Dict, Any

from .config import Config
from .logger import setup_logger
from .scheduler import get_scheduler_manager


logger = setup_logger('api')


def create_app() -> Flask:
    """
    Create and configure the Flask application.
    
    Returns:
        Flask: Configured Flask application
    """
    app = Flask(__name__)
    app.config['JSON_SORT_KEYS'] = False
    
    @app.route('/health', methods=['GET'])
    def health_check():
        """Health check endpoint."""
        return jsonify({
            'status': 'healthy',
            'service': 'apscheduler-oracle',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 200
    
    @app.route('/api/schedule-job', methods=['POST'])
    def schedule_job():
        """
        Schedule a new job.
        
        Expected JSON payload:
        {
            "job_id": "unique_job_id",
            "job_type": "bash" or "python",
            "run_date": "ISO 8601 datetime string",
            
            # For bash jobs:
            "script_path": "/path/to/script.sh",
            "args": ["arg1", "arg2"],  # optional
            
            # For python jobs:
            "module_name": "module.name",
            "function_name": "main",  # optional, defaults to 'main'
            "args": [args],  # optional
            "kwargs": {kwargs}  # optional
        }
        """
        logger.info("Received schedule job request")
        
        # Get scheduler manager
        scheduler_mgr = get_scheduler_manager()
        
        try:
            data = request.get_json()
            
            if not data:
                return jsonify({'error': 'No JSON data provided'}), 400
            
            # Validate required fields
            required_fields = ['job_id', 'job_type', 'run_date']
            missing_fields = [field for field in required_fields if field not in data]
            
            if missing_fields:
                return jsonify({
                    'error': f'Missing required fields: {", ".join(missing_fields)}'
                }), 400
            
            job_id = data['job_id']
            job_type = data['job_type']
            
            # Parse run_date
            try:
                run_date = datetime.fromisoformat(data['run_date'].replace('Z', '+00:00'))
            except ValueError as e:
                return jsonify({
                    'error': f'Invalid run_date format: {str(e)}'
                }), 400
            
            # Schedule based on job type
            if job_type == 'bash':
                if 'script_path' not in data:
                    return jsonify({'error': 'script_path is required for bash jobs'}), 400
                
                script_path = data['script_path']
                args = data.get('args', [])
                
                job_id = scheduler_mgr.add_bash_job(
                    job_id=job_id,
                    script_path=script_path,
                    run_date=run_date,
                    args=args
                )
                
                logger.info(f"Bash job scheduled: {job_id}")
                return jsonify({
                    'message': 'Bash job scheduled successfully',
                    'job_id': job_id,
                    'run_date': run_date.isoformat()
                }), 201
                
            elif job_type == 'python':
                if 'module_name' not in data:
                    return jsonify({'error': 'module_name is required for python jobs'}), 400
                
                module_name = data['module_name']
                function_name = data.get('function_name', 'main')
                args = data.get('args', [])
                kwargs = data.get('kwargs', {})
                
                job_id = scheduler_mgr.add_python_job(
                    job_id=job_id,
                    module_name=module_name,
                    run_date=run_date,
                    function_name=function_name,
                    args=args,
                    kwargs=kwargs
                )
                
                logger.info(f"Python job scheduled: {job_id}")
                return jsonify({
                    'message': 'Python job scheduled successfully',
                    'job_id': job_id,
                    'run_date': run_date.isoformat()
                }), 201
                
            else:
                return jsonify({
                    'error': f'Invalid job_type: {job_type}. Must be "bash" or "python"'
                }), 400
                
        except Exception as e:
            logger.error(f"Error scheduling job: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/remove-job/<job_id>', methods=['DELETE'])
    def remove_job(job_id: str):
        """
        Remove a scheduled job.
        
        Args:
            job_id: ID of the job to remove
        """
        logger.info(f"Received remove job request for: {job_id}")
        
        # Get scheduler manager
        scheduler_mgr = get_scheduler_manager()
        
        try:
            success = scheduler_mgr.remove_job(job_id)
            
            if success:
                logger.info(f"Job removed: {job_id}")
                return jsonify({
                    'message': 'Job removed successfully',
                    'job_id': job_id
                }), 200
            else:
                return jsonify({
                    'error': f'Job not found or could not be removed: {job_id}'
                }), 404
                
        except Exception as e:
            logger.error(f"Error removing job {job_id}: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/jobs', methods=['GET'])
    def list_jobs():
        """List all scheduled jobs."""
        logger.info("Received list jobs request")
        
        # Get scheduler manager
        scheduler_mgr = get_scheduler_manager()
        
        try:
            jobs = scheduler_mgr.get_all_jobs()
            
            return jsonify({
                'jobs': jobs,
                'count': len(jobs)
            }), 200
            
        except Exception as e:
            logger.error(f"Error listing jobs: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/jobs/<job_id>', methods=['GET'])
    def get_job(job_id: str):
        """
        Get details of a specific job.
        
        Args:
            job_id: ID of the job
        """
        logger.info(f"Received get job request for: {job_id}")
        
        # Get scheduler manager
        scheduler_mgr = get_scheduler_manager()
        
        try:
            job = scheduler_mgr.get_job(job_id)
            
            if job:
                return jsonify(job), 200
            else:
                return jsonify({'error': f'Job not found: {job_id}'}), 404
                
        except Exception as e:
            logger.error(f"Error getting job {job_id}: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/jobs/<job_id>/pause', methods=['POST'])
    def pause_job(job_id: str):
        """
        Pause a scheduled job.
        
        Args:
            job_id: ID of the job to pause
        """
        logger.info(f"Received pause job request for: {job_id}")
        
        # Get scheduler manager
        scheduler_mgr = get_scheduler_manager()
        
        try:
            success = scheduler_mgr.pause_job(job_id)
            
            if success:
                logger.info(f"Job paused: {job_id}")
                return jsonify({
                    'message': 'Job paused successfully',
                    'job_id': job_id
                }), 200
            else:
                return jsonify({
                    'error': f'Job not found or could not be paused: {job_id}'
                }), 404
                
        except Exception as e:
            logger.error(f"Error pausing job {job_id}: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/jobs/<job_id>/resume', methods=['POST'])
    def resume_job(job_id: str):
        """
        Resume a paused job.
        
        Args:
            job_id: ID of the job to resume
        """
        logger.info(f"Received resume job request for: {job_id}")
        
        # Get scheduler manager
        scheduler_mgr = get_scheduler_manager()
        
        try:
            success = scheduler_mgr.resume_job(job_id)
            
            if success:
                logger.info(f"Job resumed: {job_id}")
                return jsonify({
                    'message': 'Job resumed successfully',
                    'job_id': job_id
                }), 200
            else:
                return jsonify({
                    'error': f'Job not found or could not be resumed: {job_id}'
                }), 404
                
        except Exception as e:
            logger.error(f"Error resuming job {job_id}: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    @app.errorhandler(404)
    def not_found(error):
        """Handle 404 errors."""
        return jsonify({'error': 'Endpoint not found'}), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        """Handle 500 errors."""
        logger.error(f"Internal server error: {str(error)}")
        return jsonify({'error': 'Internal server error'}), 500
    
    return app


def run_api_server():
    """Run the Flask API server."""
    logger.info("Starting API server")
    
    # Start the scheduler
    scheduler_mgr = get_scheduler_manager()
    scheduler_mgr.start()
    
    # Create and run Flask app
    app = create_app()
    
    try:
        app.run(
            host=Config.API_HOST,
            port=Config.API_PORT,
            debug=Config.API_DEBUG
        )
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        scheduler_mgr.shutdown()
        logger.info("API server stopped")


if __name__ == '__main__':
    run_api_server()

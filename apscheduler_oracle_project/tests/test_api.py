"""
Unit tests for REST API endpoints.
"""
import pytest
import sys
import json
from pathlib import Path
from datetime import datetime, timedelta
from pytz import UTC

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.api import app, init_app
from app.scheduler import SchedulerManager


@pytest.fixture
def mock_db_uri():
    """Mock Oracle database URI."""
    return "sqlite:///:memory:"


@pytest.fixture
def scheduler_manager(mock_db_uri):
    """Create a SchedulerManager with mocked database."""
    manager = SchedulerManager(db_uri=mock_db_uri)
    manager.start()
    yield manager
    manager.shutdown(wait=False)


@pytest.fixture
def client(scheduler_manager):
    """Create Flask test client."""
    init_app(scheduler_manager)
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


class TestHealthEndpoint:
    """Tests for health check endpoint."""
    
    def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get('/api/health')
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['status'] == 'healthy'
        assert 'scheduler_running' in data


class TestScheduleJobEndpoint:
    """Tests for schedule job endpoint."""
    
    def test_schedule_job_success(self, client):
        """Test scheduling a job successfully."""
        run_date = (datetime.now(UTC) + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S')
        
        payload = {
            'job_id': 'test_api_job_1',
            'job_type': 'bash',
            'script_path': '/path/to/script.sh',
            'run_date': run_date
        }
        
        response = client.post(
            '/api/schedule-job',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['message'] == 'Job scheduled successfully'
        assert data['job']['job_id'] == 'test_api_job_1'
    
    def test_schedule_job_with_args(self, client):
        """Test scheduling a job with arguments."""
        run_date = (datetime.now(UTC) + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S')
        
        payload = {
            'job_id': 'test_api_job_2',
            'job_type': 'python',
            'script_path': '/path/to/script.py',
            'run_date': run_date,
            'args': ['arg1', 'arg2']
        }
        
        response = client.post(
            '/api/schedule-job',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['job']['args'] == ['arg1', 'arg2']
    
    def test_schedule_job_missing_fields(self, client):
        """Test scheduling a job with missing fields."""
        payload = {
            'job_id': 'test_api_job_3',
            'job_type': 'bash'
        }
        
        response = client.post(
            '/api/schedule-job',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 400
        data = response.get_json()
        assert 'error' in data
        assert 'Missing required fields' in data['error']
    
    def test_schedule_job_invalid_type(self, client):
        """Test scheduling a job with invalid type."""
        run_date = (datetime.now(UTC) + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S')
        
        payload = {
            'job_id': 'test_api_job_4',
            'job_type': 'invalid',
            'script_path': '/path/to/script.sh',
            'run_date': run_date
        }
        
        response = client.post(
            '/api/schedule-job',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 400
        data = response.get_json()
        assert 'error' in data
    
    def test_schedule_job_invalid_date_format(self, client):
        """Test scheduling a job with invalid date format."""
        payload = {
            'job_id': 'test_api_job_5',
            'job_type': 'bash',
            'script_path': '/path/to/script.sh',
            'run_date': 'invalid_date'
        }
        
        response = client.post(
            '/api/schedule-job',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 400
        data = response.get_json()
        assert 'error' in data
    
    def test_schedule_job_past_date(self, client):
        """Test scheduling a job with a date in the past."""
        run_date = (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S')
        
        payload = {
            'job_id': 'test_api_job_6',
            'job_type': 'bash',
            'script_path': '/path/to/script.sh',
            'run_date': run_date
        }
        
        response = client.post(
            '/api/schedule-job',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        assert response.status_code == 400
        data = response.get_json()
        assert 'run_date must be in the future' in data['error']


class TestListJobsEndpoint:
    """Tests for list jobs endpoint."""
    
    def test_list_jobs_empty(self, client):
        """Test listing jobs when no jobs exist."""
        response = client.get('/api/jobs')
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['count'] == 0
        assert data['jobs'] == []
    
    def test_list_jobs_with_jobs(self, client):
        """Test listing jobs with multiple jobs."""
        run_date = (datetime.now(UTC) + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S')
        
        # Add multiple jobs
        for i in range(3):
            payload = {
                'job_id': f'test_list_job_{i}',
                'job_type': 'bash',
                'script_path': f'/path/to/script_{i}.sh',
                'run_date': run_date
            }
            client.post(
                '/api/schedule-job',
                data=json.dumps(payload),
                content_type='application/json'
            )
        
        # List jobs
        response = client.get('/api/jobs')
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['count'] >= 3


class TestRemoveJobEndpoint:
    """Tests for remove job endpoint."""
    
    def test_remove_job_success(self, client):
        """Test removing a job successfully."""
        run_date = (datetime.now(UTC) + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S')
        
        # Add job
        payload = {
            'job_id': 'test_remove_job_1',
            'job_type': 'bash',
            'script_path': '/path/to/script.sh',
            'run_date': run_date
        }
        client.post(
            '/api/schedule-job',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        # Remove job
        response = client.delete('/api/remove-job/test_remove_job_1')
        
        assert response.status_code == 200
        data = response.get_json()
        assert 'removed successfully' in data['message']
    
    def test_remove_nonexistent_job(self, client):
        """Test removing a non-existent job."""
        response = client.delete('/api/remove-job/nonexistent_job')
        
        assert response.status_code == 404
        data = response.get_json()
        assert 'Job not found' in data['error']


class TestPauseJobEndpoint:
    """Tests for pause job endpoint."""
    
    def test_pause_job_success(self, client):
        """Test pausing a job successfully."""
        run_date = (datetime.now(UTC) + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S')
        
        # Add job
        payload = {
            'job_id': 'test_pause_job_1',
            'job_type': 'bash',
            'script_path': '/path/to/script.sh',
            'run_date': run_date
        }
        client.post(
            '/api/schedule-job',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        # Pause job
        response = client.put('/api/pause-job/test_pause_job_1')
        
        assert response.status_code == 200
        data = response.get_json()
        assert 'paused successfully' in data['message']
    
    def test_pause_nonexistent_job(self, client):
        """Test pausing a non-existent job."""
        response = client.put('/api/pause-job/nonexistent_job')
        
        assert response.status_code == 404
        data = response.get_json()
        assert 'Job not found' in data['error']


class TestResumeJobEndpoint:
    """Tests for resume job endpoint."""
    
    def test_resume_job_success(self, client):
        """Test resuming a job successfully."""
        run_date = (datetime.now(UTC) + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S')
        
        # Add job
        payload = {
            'job_id': 'test_resume_job_1',
            'job_type': 'bash',
            'script_path': '/path/to/script.sh',
            'run_date': run_date
        }
        client.post(
            '/api/schedule-job',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        # Pause and resume
        client.put('/api/pause-job/test_resume_job_1')
        response = client.put('/api/resume-job/test_resume_job_1')
        
        assert response.status_code == 200
        data = response.get_json()
        assert 'resumed successfully' in data['message']
    
    def test_resume_nonexistent_job(self, client):
        """Test resuming a non-existent job."""
        response = client.put('/api/resume-job/nonexistent_job')
        
        assert response.status_code == 404
        data = response.get_json()
        assert 'Job not found' in data['error']


class TestGetJobEndpoint:
    """Tests for get job endpoint."""
    
    def test_get_job_success(self, client):
        """Test getting a job successfully."""
        run_date = (datetime.now(UTC) + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S')
        
        # Add job
        payload = {
            'job_id': 'test_get_job_1',
            'job_type': 'bash',
            'script_path': '/path/to/script.sh',
            'run_date': run_date
        }
        client.post(
            '/api/schedule-job',
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        # Get job
        response = client.get('/api/job/test_get_job_1')
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['job']['job_id'] == 'test_get_job_1'
    
    def test_get_nonexistent_job(self, client):
        """Test getting a non-existent job."""
        response = client.get('/api/job/nonexistent_job')
        
        assert response.status_code == 404
        data = response.get_json()
        assert 'Job not found' in data['error']


class TestErrorHandlers:
    """Tests for error handlers."""
    
    def test_404_error(self, client):
        """Test 404 error handler."""
        response = client.get('/api/nonexistent-endpoint')
        
        assert response.status_code == 404
        data = response.get_json()
        assert 'Endpoint not found' in data['error']

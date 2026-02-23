"""
Unit tests for REST API module.

Tests API endpoints for job scheduling management.
"""

import json
from unittest.mock import Mock, patch

import pytest
from app.api import create_app


@pytest.fixture
def client():
    """Create test client for Flask app."""
    app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


@pytest.fixture
def mock_scheduler():
    """Mock scheduler manager."""
    with patch("app.api.get_scheduler_manager") as mock:
        scheduler = Mock()
        mock.return_value = scheduler
        yield scheduler


class TestHealthEndpoint:
    """Tests for health check endpoint."""

    def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get("/health")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["status"] == "healthy"
        assert "timestamp" in data


class TestScheduleJobEndpoint:
    """Tests for schedule job endpoint."""

    def test_schedule_bash_job_success(self, client, mock_scheduler):
        """Test scheduling a Bash job successfully."""
        # Arrange
        mock_scheduler.add_bash_job.return_value = "test_job_1"

        payload = {
            "job_id": "test_job_1",
            "job_type": "bash",
            "run_date": "2024-12-31T23:59:59",
            "script_path": "/path/to/script.sh",
            "args": ["arg1"],
        }

        # Act
        response = client.post(
            "/api/schedule-job", data=json.dumps(payload), content_type="application/json"
        )

        # Assert
        assert response.status_code == 201
        data = json.loads(response.data)
        assert data["job_id"] == "test_job_1"
        assert "Bash job scheduled successfully" in data["message"]
        mock_scheduler.add_bash_job.assert_called_once()

    def test_schedule_python_job_success(self, client, mock_scheduler):
        """Test scheduling a Python job successfully."""
        # Arrange
        mock_scheduler.add_python_job.return_value = "test_job_2"

        payload = {
            "job_id": "test_job_2",
            "job_type": "python",
            "run_date": "2024-12-31T23:59:59",
            "module_name": "test_module",
            "function_name": "main",
            "args": [],
            "kwargs": {"key": "value"},
        }

        # Act
        response = client.post(
            "/api/schedule-job", data=json.dumps(payload), content_type="application/json"
        )

        # Assert
        assert response.status_code == 201
        data = json.loads(response.data)
        assert data["job_id"] == "test_job_2"
        assert "Python job scheduled successfully" in data["message"]
        mock_scheduler.add_python_job.assert_called_once()

    def test_schedule_job_missing_fields(self, client, mock_scheduler):
        """Test scheduling a job with missing required fields."""
        # Arrange
        payload = {
            "job_id": "test_job_3",
            "job_type": "bash",
            # Missing run_date
        }

        # Act
        response = client.post(
            "/api/schedule-job", data=json.dumps(payload), content_type="application/json"
        )

        # Assert
        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "Missing required fields" in data["error"]

    def test_schedule_job_invalid_date(self, client, mock_scheduler):
        """Test scheduling a job with invalid date format."""
        # Arrange
        payload = {
            "job_id": "test_job_4",
            "job_type": "bash",
            "run_date": "invalid-date",
            "script_path": "/path/to/script.sh",
        }

        # Act
        response = client.post(
            "/api/schedule-job", data=json.dumps(payload), content_type="application/json"
        )

        # Assert
        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "Invalid run_date format" in data["error"]

    def test_schedule_job_invalid_type(self, client, mock_scheduler):
        """Test scheduling a job with invalid job type."""
        # Arrange
        payload = {"job_id": "test_job_5", "job_type": "invalid", "run_date": "2024-12-31T23:59:59"}

        # Act
        response = client.post(
            "/api/schedule-job", data=json.dumps(payload), content_type="application/json"
        )

        # Assert
        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "Invalid job_type" in data["error"]

    def test_schedule_bash_job_missing_script_path(self, client, mock_scheduler):
        """Test scheduling a Bash job without script_path."""
        # Arrange
        payload = {"job_id": "test_job_6", "job_type": "bash", "run_date": "2024-12-31T23:59:59"}

        # Act
        response = client.post(
            "/api/schedule-job", data=json.dumps(payload), content_type="application/json"
        )

        # Assert
        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "script_path is required" in data["error"]

    def test_schedule_python_job_missing_module_name(self, client, mock_scheduler):
        """Test scheduling a Python job without module_name."""
        # Arrange
        payload = {"job_id": "test_job_7", "job_type": "python", "run_date": "2024-12-31T23:59:59"}

        # Act
        response = client.post(
            "/api/schedule-job", data=json.dumps(payload), content_type="application/json"
        )

        # Assert
        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "module_name is required" in data["error"]


class TestListJobsEndpoint:
    """Tests for list jobs endpoint."""

    def test_list_jobs_success(self, client, mock_scheduler):
        """Test listing all jobs successfully."""
        # Arrange
        mock_jobs = [
            {"id": "job1", "name": "Job 1", "next_run_time": "2024-12-31T23:59:59"},
            {"id": "job2", "name": "Job 2", "next_run_time": "2024-12-31T23:59:59"},
        ]
        mock_scheduler.get_all_jobs.return_value = mock_jobs

        # Act
        response = client.get("/api/jobs")

        # Assert
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["count"] == 2
        assert len(data["jobs"]) == 2
        mock_scheduler.get_all_jobs.assert_called_once()

    def test_list_jobs_empty(self, client, mock_scheduler):
        """Test listing jobs when no jobs exist."""
        # Arrange
        mock_scheduler.get_all_jobs.return_value = []

        # Act
        response = client.get("/api/jobs")

        # Assert
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["count"] == 0
        assert len(data["jobs"]) == 0


class TestGetJobEndpoint:
    """Tests for get job endpoint."""

    def test_get_job_success(self, client, mock_scheduler):
        """Test getting a specific job successfully."""
        # Arrange
        mock_job = {"id": "test_job", "name": "Test Job", "next_run_time": "2024-12-31T23:59:59"}
        mock_scheduler.get_job.return_value = mock_job

        # Act
        response = client.get("/api/jobs/test_job")

        # Assert
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["id"] == "test_job"
        mock_scheduler.get_job.assert_called_once_with("test_job")

    def test_get_job_not_found(self, client, mock_scheduler):
        """Test getting a non-existent job."""
        # Arrange
        mock_scheduler.get_job.return_value = None

        # Act
        response = client.get("/api/jobs/nonexistent")

        # Assert
        assert response.status_code == 404
        data = json.loads(response.data)
        assert "error" in data


class TestRemoveJobEndpoint:
    """Tests for remove job endpoint."""

    def test_remove_job_success(self, client, mock_scheduler):
        """Test removing a job successfully."""
        # Arrange
        mock_scheduler.remove_job.return_value = True

        # Act
        response = client.delete("/api/remove-job/test_job")

        # Assert
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "Job removed successfully" in data["message"]
        mock_scheduler.remove_job.assert_called_once_with("test_job")

    def test_remove_job_not_found(self, client, mock_scheduler):
        """Test removing a non-existent job."""
        # Arrange
        mock_scheduler.remove_job.return_value = False

        # Act
        response = client.delete("/api/remove-job/nonexistent")

        # Assert
        assert response.status_code == 404
        data = json.loads(response.data)
        assert "error" in data


class TestPauseResumeJobEndpoints:
    """Tests for pause and resume job endpoints."""

    def test_pause_job_success(self, client, mock_scheduler):
        """Test pausing a job successfully."""
        # Arrange
        mock_scheduler.pause_job.return_value = True

        # Act
        response = client.post("/api/jobs/test_job/pause")

        # Assert
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "Job paused successfully" in data["message"]
        mock_scheduler.pause_job.assert_called_once_with("test_job")

    def test_pause_job_not_found(self, client, mock_scheduler):
        """Test pausing a non-existent job."""
        # Arrange
        mock_scheduler.pause_job.return_value = False

        # Act
        response = client.post("/api/jobs/nonexistent/pause")

        # Assert
        assert response.status_code == 404
        data = json.loads(response.data)
        assert "error" in data

    def test_resume_job_success(self, client, mock_scheduler):
        """Test resuming a job successfully."""
        # Arrange
        mock_scheduler.resume_job.return_value = True

        # Act
        response = client.post("/api/jobs/test_job/resume")

        # Assert
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "Job resumed successfully" in data["message"]
        mock_scheduler.resume_job.assert_called_once_with("test_job")

    def test_resume_job_not_found(self, client, mock_scheduler):
        """Test resuming a non-existent job."""
        # Arrange
        mock_scheduler.resume_job.return_value = False

        # Act
        response = client.post("/api/jobs/nonexistent/resume")

        # Assert
        assert response.status_code == 404
        data = json.loads(response.data)
        assert "error" in data

"""
Unit tests for scheduler logic with mocked Oracle database.
"""
import pytest
import sys
from pathlib import Path
from datetime import datetime, timedelta
from pytz import UTC

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.scheduler import SchedulerManager  # noqa: E402


@pytest.fixture
def mock_db_uri():
    """Mock Oracle database URI."""
    return "sqlite:///:memory:"  # Use SQLite in-memory for testing


@pytest.fixture
def scheduler_manager(mock_db_uri):
    """Create a SchedulerManager with mocked database."""
    manager = SchedulerManager(db_uri=mock_db_uri)
    manager.start()
    yield manager
    manager.shutdown(wait=False)


class TestSchedulerInitialization:
    """Tests for scheduler initialization."""

    def test_scheduler_init(self, mock_db_uri):
        """Test scheduler initialization with mock database."""
        manager = SchedulerManager(db_uri=mock_db_uri)

        assert manager is not None
        assert manager.scheduler is not None
        assert manager.db_uri == mock_db_uri

    def test_scheduler_start(self, scheduler_manager):
        """Test scheduler start."""
        assert scheduler_manager.scheduler.running is True

    def test_scheduler_shutdown(self, mock_db_uri):
        """Test scheduler shutdown."""
        manager = SchedulerManager(db_uri=mock_db_uri)
        manager.start()
        manager.shutdown(wait=False)

        assert manager.scheduler.running is False


class TestJobManagement:
    """Tests for job management operations."""

    def test_add_job(self, scheduler_manager):
        """Test adding a new job."""
        run_date = datetime.now(UTC) + timedelta(hours=1)

        job_details = scheduler_manager.add_job(
            job_id="test_job_1",
            job_type="bash",
            script_path="/path/to/script.sh",
            run_date=run_date,
        )

        assert job_details is not None
        assert job_details["job_id"] == "test_job_1"
        assert job_details["job_type"] == "bash"
        assert job_details["script_path"] == "/path/to/script.sh"

    def test_add_job_with_args(self, scheduler_manager):
        """Test adding a job with arguments."""
        run_date = datetime.now(UTC) + timedelta(hours=1)

        job_details = scheduler_manager.add_job(
            job_id="test_job_2",
            job_type="python",
            script_path="/path/to/script.py",
            run_date=run_date,
            args=["arg1", "arg2"],
        )

        assert job_details is not None
        assert job_details["args"] == ["arg1", "arg2"]

    def test_add_job_replace_existing(self, scheduler_manager):
        """Test replacing an existing job."""
        run_date = datetime.now(UTC) + timedelta(hours=1)

        # Add first job
        scheduler_manager.add_job(
            job_id="test_job_3",
            job_type="bash",
            script_path="/path/to/script1.sh",
            run_date=run_date,
        )

        # Replace with new job
        job_details = scheduler_manager.add_job(
            job_id="test_job_3",
            job_type="python",
            script_path="/path/to/script2.py",
            run_date=run_date,
            replace_existing=True,
        )

        assert job_details is not None
        assert job_details["job_type"] == "python"
        assert job_details["script_path"] == "/path/to/script2.py"

    def test_add_job_invalid_type(self, scheduler_manager):
        """Test adding a job with invalid type."""
        run_date = datetime.now(UTC) + timedelta(hours=1)

        with pytest.raises(ValueError, match="job_type must be 'bash' or 'python'"):
            scheduler_manager.add_job(
                job_id="test_job_4",
                job_type="invalid",
                script_path="/path/to/script.sh",
                run_date=run_date,
            )

    def test_add_job_missing_params(self, scheduler_manager):
        """Test adding a job with missing parameters."""
        run_date = datetime.now(UTC) + timedelta(hours=1)

        with pytest.raises(ValueError):
            scheduler_manager.add_job(
                job_id="", job_type="bash", script_path="/path/to/script.sh", run_date=run_date
            )

    def test_remove_job(self, scheduler_manager):
        """Test removing a job."""
        run_date = datetime.now(UTC) + timedelta(hours=1)

        # Add job
        scheduler_manager.add_job(
            job_id="test_job_5",
            job_type="bash",
            script_path="/path/to/script.sh",
            run_date=run_date,
        )

        # Remove job
        success = scheduler_manager.remove_job("test_job_5")

        assert success is True
        assert scheduler_manager.get_job("test_job_5") is None

    def test_remove_nonexistent_job(self, scheduler_manager):
        """Test removing a non-existent job."""
        success = scheduler_manager.remove_job("nonexistent_job")
        assert success is False

    def test_pause_job(self, scheduler_manager):
        """Test pausing a job."""
        run_date = datetime.now(UTC) + timedelta(hours=1)

        # Add job
        scheduler_manager.add_job(
            job_id="test_job_6",
            job_type="bash",
            script_path="/path/to/script.sh",
            run_date=run_date,
        )

        # Pause job
        success = scheduler_manager.pause_job("test_job_6")
        assert success is True

    def test_resume_job(self, scheduler_manager):
        """Test resuming a job."""
        run_date = datetime.now(UTC) + timedelta(hours=1)

        # Add job
        scheduler_manager.add_job(
            job_id="test_job_7",
            job_type="bash",
            script_path="/path/to/script.sh",
            run_date=run_date,
        )

        # Pause and resume
        scheduler_manager.pause_job("test_job_7")
        success = scheduler_manager.resume_job("test_job_7")

        assert success is True

    def test_get_job(self, scheduler_manager):
        """Test getting job details."""
        run_date = datetime.now(UTC) + timedelta(hours=1)

        # Add job
        scheduler_manager.add_job(
            job_id="test_job_8",
            job_type="bash",
            script_path="/path/to/script.sh",
            run_date=run_date,
        )

        # Get job
        job = scheduler_manager.get_job("test_job_8")

        assert job is not None
        assert job["job_id"] == "test_job_8"

    def test_get_nonexistent_job(self, scheduler_manager):
        """Test getting a non-existent job."""
        job = scheduler_manager.get_job("nonexistent_job")
        assert job is None

    def test_list_jobs(self, scheduler_manager):
        """Test listing all jobs."""
        run_date = datetime.now(UTC) + timedelta(hours=1)

        # Add multiple jobs
        for i in range(3):
            scheduler_manager.add_job(
                job_id=f"test_job_{i}",
                job_type="bash",
                script_path=f"/path/to/script_{i}.sh",
                run_date=run_date,
            )

        # List jobs
        jobs = scheduler_manager.list_jobs()

        assert len(jobs) >= 3
        job_ids = [job["job_id"] for job in jobs]
        assert "test_job_0" in job_ids
        assert "test_job_1" in job_ids
        assert "test_job_2" in job_ids

    def test_list_jobs_empty(self, mock_db_uri):
        """Test listing jobs when no jobs exist."""
        manager = SchedulerManager(db_uri=mock_db_uri)
        manager.start()

        jobs = manager.list_jobs()

        assert jobs == []

        manager.shutdown(wait=False)

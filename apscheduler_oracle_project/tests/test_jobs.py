"""
Unit tests for job execution (Bash and Python scripts).
"""
import pytest
import sys
import subprocess
from pathlib import Path
from unittest.mock import patch

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.jobs import execute_bash_job, execute_python_job, execute_job  # noqa: E402


class TestBashJobExecution:
    """Tests for Bash script execution."""

    @pytest.fixture
    def temp_bash_script(self, tmp_path):
        """Create a temporary Bash script for testing."""
        script = tmp_path / "test_script.sh"
        script.write_text("#!/bin/bash\necho 'Hello from Bash'\nexit 0")
        script.chmod(0o755)
        return script

    @pytest.fixture
    def temp_failing_bash_script(self, tmp_path):
        """Create a temporary failing Bash script for testing."""
        script = tmp_path / "test_failing_script.sh"
        script.write_text("#!/bin/bash\necho 'Error' >&2\nexit 1")
        script.chmod(0o755)
        return script

    def test_execute_bash_job_success(self, temp_bash_script):
        """Test successful Bash script execution."""
        result = execute_bash_job(str(temp_bash_script))

        assert result["success"] is True
        assert result["returncode"] == 0
        assert "Hello from Bash" in result["stdout"]
        assert result["stderr"] == ""

    def test_execute_bash_job_with_args(self, tmp_path):
        """Test Bash script execution with arguments."""
        script = tmp_path / "test_args.sh"
        script.write_text("#!/bin/bash\necho $1 $2")
        script.chmod(0o755)

        result = execute_bash_job(str(script), ["arg1", "arg2"])

        assert result["success"] is True
        assert "arg1 arg2" in result["stdout"]

    def test_execute_bash_job_failure(self, temp_failing_bash_script):
        """Test Bash script execution that fails."""
        result = execute_bash_job(str(temp_failing_bash_script))

        assert result["success"] is False
        assert result["returncode"] == 1
        assert "Error" in result["stderr"]

    def test_execute_bash_job_file_not_found(self):
        """Test Bash script execution with non-existent file."""
        with pytest.raises(FileNotFoundError):
            execute_bash_job("/path/to/nonexistent/script.sh")

    def test_execute_bash_job_timeout(self, tmp_path):
        """Test Bash script execution timeout."""
        script = tmp_path / "test_timeout.sh"
        script.write_text("#!/bin/bash\nsleep 10")
        script.chmod(0o755)

        with patch("app.jobs.subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired("bash", 300)
            result = execute_bash_job(str(script))

            assert result["success"] is False
            assert result["returncode"] == -1
            assert "error" in result


class TestPythonJobExecution:
    """Tests for Python script execution."""

    @pytest.fixture
    def temp_python_script(self, tmp_path):
        """Create a temporary Python script for testing."""
        script = tmp_path / "test_script.py"
        script.write_text(
            """
def main():
    return "Hello from Python"
"""
        )
        return script

    @pytest.fixture
    def temp_python_script_with_args(self, tmp_path):
        """Create a temporary Python script with arguments."""
        script = tmp_path / "test_args.py"
        script.write_text(
            """
def main(arg1, arg2):
    return f"{arg1} {arg2}"
"""
        )
        return script

    @pytest.fixture
    def temp_python_script_without_main(self, tmp_path):
        """Create a temporary Python script without main function."""
        script = tmp_path / "test_no_main.py"
        script.write_text(
            """
def some_function():
    return "No main function"
"""
        )
        return script

    def test_execute_python_job_success(self, temp_python_script):
        """Test successful Python script execution."""
        result = execute_python_job(str(temp_python_script))

        assert result["success"] is True
        assert result["result"] == "Hello from Python"

    def test_execute_python_job_with_args(self, temp_python_script_with_args):
        """Test Python script execution with arguments."""
        result = execute_python_job(str(temp_python_script_with_args), ["arg1", "arg2"])

        assert result["success"] is True
        assert result["result"] == "arg1 arg2"

    def test_execute_python_job_without_main(self, temp_python_script_without_main):
        """Test Python script without main function."""
        result = execute_python_job(str(temp_python_script_without_main))

        assert result["success"] is False
        assert "error" in result
        assert "main()" in result["error"]

    def test_execute_python_job_file_not_found(self):
        """Test Python script execution with non-existent file."""
        with pytest.raises(FileNotFoundError):
            execute_python_job("/path/to/nonexistent/script.py")

    def test_execute_python_job_not_python_file(self, tmp_path):
        """Test Python script execution with non-Python file."""
        script = tmp_path / "test_script.txt"
        script.write_text("Not a Python file")

        with pytest.raises(ValueError):
            execute_python_job(str(script))

    def test_execute_python_job_with_exception(self, tmp_path):
        """Test Python script that raises an exception."""
        script = tmp_path / "test_exception.py"
        script.write_text(
            """
def main():
    raise ValueError("Test exception")
"""
        )

        result = execute_python_job(str(script))

        assert result["success"] is False
        assert "error" in result
        assert "Test exception" in result["error"]


class TestExecuteJob:
    """Tests for generic job execution."""

    @pytest.fixture
    def temp_bash_script(self, tmp_path):
        """Create a temporary Bash script."""
        script = tmp_path / "test.sh"
        script.write_text("#!/bin/bash\necho 'test'")
        script.chmod(0o755)
        return script

    @pytest.fixture
    def temp_python_script(self, tmp_path):
        """Create a temporary Python script."""
        script = tmp_path / "test.py"
        script.write_text("def main():\n    return 'test'")
        return script

    def test_execute_job_bash(self, temp_bash_script):
        """Test execute_job with bash type."""
        result = execute_job("bash", str(temp_bash_script))
        assert result["success"] is True

    def test_execute_job_python(self, temp_python_script):
        """Test execute_job with python type."""
        result = execute_job("python", str(temp_python_script))
        assert result["success"] is True

    def test_execute_job_invalid_type(self, temp_bash_script):
        """Test execute_job with invalid job type."""
        with pytest.raises(ValueError, match="Unsupported job type"):
            execute_job("invalid", str(temp_bash_script))

    def test_execute_job_case_insensitive(self, temp_bash_script):
        """Test execute_job with case-insensitive job type."""
        result = execute_job("BASH", str(temp_bash_script))
        assert result["success"] is True

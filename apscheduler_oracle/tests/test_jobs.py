"""
Unit tests for job execution module.

Tests Bash script execution and Python module execution functionality.
"""

import subprocess
from unittest.mock import Mock, patch

import pytest
from app.jobs import execute_bash_script, execute_python_module, job_wrapper


class TestBashScriptExecution:
    """Tests for Bash script execution."""

    @patch("app.jobs.subprocess.run")
    def test_execute_bash_script_success(self, mock_run):
        """Test successful Bash script execution."""
        # Arrange
        mock_run.return_value = Mock(returncode=0, stdout="Script executed successfully", stderr="")

        # Act
        result = execute_bash_script("/path/to/script.sh")

        # Assert
        assert result["success"] is True
        assert result["returncode"] == 0
        assert "Script executed successfully" in result["stdout"]
        mock_run.assert_called_once()

    @patch("app.jobs.subprocess.run")
    def test_execute_bash_script_with_args(self, mock_run):
        """Test Bash script execution with arguments."""
        # Arrange
        mock_run.return_value = Mock(returncode=0, stdout="Output", stderr="")
        args = ["arg1", "arg2"]

        # Act
        result = execute_bash_script("/path/to/script.sh", args)

        # Assert
        assert result["success"] is True
        call_args = mock_run.call_args[0][0]
        assert "arg1" in call_args
        assert "arg2" in call_args

    @patch("app.jobs.subprocess.run")
    def test_execute_bash_script_failure(self, mock_run):
        """Test Bash script execution failure."""
        # Arrange
        mock_run.return_value = Mock(returncode=1, stdout="", stderr="Error occurred")

        # Act
        result = execute_bash_script("/path/to/script.sh")

        # Assert
        assert result["success"] is False
        assert result["returncode"] == 1
        assert "Error occurred" in result["stderr"]

    @patch("app.jobs.subprocess.run")
    def test_execute_bash_script_timeout(self, mock_run):
        """Test Bash script execution timeout."""
        # Arrange
        mock_run.side_effect = subprocess.TimeoutExpired("bash", 300)

        # Act
        result = execute_bash_script("/path/to/script.sh")

        # Assert
        assert result["success"] is False
        assert result["returncode"] == -1
        assert "timed out" in result["stderr"]

    @patch("app.jobs.subprocess.run")
    def test_execute_bash_script_not_found(self, mock_run):
        """Test Bash script file not found."""
        # Arrange
        mock_run.side_effect = FileNotFoundError()

        # Act & Assert
        with pytest.raises(FileNotFoundError):
            execute_bash_script("/nonexistent/script.sh")


class TestPythonModuleExecution:
    """Tests for Python module execution."""

    @patch("app.jobs.importlib.import_module")
    def test_execute_python_module_success(self, mock_import):
        """Test successful Python module execution."""
        # Arrange
        mock_module = Mock()
        mock_module.main = Mock(return_value="Success")
        mock_import.return_value = mock_module

        # Act
        result = execute_python_module("test_module")

        # Assert
        assert result == "Success"
        mock_module.main.assert_called_once()

    @patch("app.jobs.importlib.import_module")
    def test_execute_python_module_with_args(self, mock_import):
        """Test Python module execution with arguments."""
        # Arrange
        mock_module = Mock()
        mock_module.process = Mock(return_value="Processed")
        mock_import.return_value = mock_module

        args = ["arg1", "arg2"]
        kwargs = {"key": "value"}

        # Act
        result = execute_python_module("test_module", "process", args=args, kwargs=kwargs)

        # Assert
        assert result == "Processed"
        mock_module.process.assert_called_once_with("arg1", "arg2", key="value")

    @patch("app.jobs.importlib.import_module")
    def test_execute_python_module_import_error(self, mock_import):
        """Test Python module import error."""
        # Arrange
        mock_import.side_effect = ImportError("Module not found")

        # Act & Assert
        with pytest.raises(ImportError):
            execute_python_module("nonexistent_module")

    @patch("app.jobs.importlib.import_module")
    def test_execute_python_module_missing_function(self, mock_import):
        """Test Python module with missing function."""
        # Arrange
        mock_module = Mock(spec=[])  # Empty spec, no attributes
        mock_import.return_value = mock_module

        # Act & Assert
        with pytest.raises(AttributeError):
            execute_python_module("test_module", "nonexistent_function")

    @patch("app.jobs.importlib.import_module")
    def test_execute_python_module_execution_error(self, mock_import):
        """Test Python module execution error."""
        # Arrange
        mock_module = Mock()
        mock_module.main = Mock(side_effect=ValueError("Invalid argument"))
        mock_import.return_value = mock_module

        # Act & Assert
        with pytest.raises(ValueError):
            execute_python_module("test_module")


class TestJobWrapper:
    """Tests for job wrapper function."""

    @patch("app.jobs.execute_bash_script")
    def test_job_wrapper_bash(self, mock_execute):
        """Test job wrapper for Bash jobs."""
        # Arrange
        mock_execute.return_value = {
            "success": True,
            "returncode": 0,
            "stdout": "Output",
            "stderr": "",
        }

        # Act
        result = job_wrapper(job_type="bash", script_path="/path/to/script.sh", args=["arg1"])

        # Assert
        assert result["success"] is True
        mock_execute.assert_called_once_with("/path/to/script.sh", ["arg1"])

    @patch("app.jobs.execute_python_module")
    def test_job_wrapper_python(self, mock_execute):
        """Test job wrapper for Python jobs."""
        # Arrange
        mock_execute.return_value = "Result"

        # Act
        result = job_wrapper(
            job_type="python", module_name="test_module", function_name="main", args=[], kwargs={}
        )

        # Assert
        assert result["success"] is True
        assert result["result"] == "Result"
        mock_execute.assert_called_once()

    def test_job_wrapper_invalid_type(self):
        """Test job wrapper with invalid job type."""
        # Act
        result = job_wrapper(job_type="invalid")

        # Assert
        assert result["success"] is False
        assert "error" in result
        assert "Unknown job type" in result["error"]

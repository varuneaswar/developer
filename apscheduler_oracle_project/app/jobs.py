"""
Job execution logic for Bash and Python scripts.
Provides functions to execute Bash scripts and Python modules dynamically.
"""

import importlib.util
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

from app.logger import get_logger

logger = get_logger("jobs")


def execute_bash_job(script_path: str, args: List[str] = None) -> Dict[str, Any]:
    """
    Execute a Bash script with optional arguments.

    Args:
        script_path: Path to the Bash script
        args: List of arguments to pass to the script (optional)

    Returns:
        Dict containing execution results with keys:
            - success: bool
            - returncode: int
            - stdout: str
            - stderr: str
            - error: str (if exception occurred)

    Raises:
        FileNotFoundError: If script file doesn't exist
    """
    script_path = Path(script_path)

    if not script_path.exists():
        error_msg = f"Bash script not found: {script_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    if not script_path.is_file():
        error_msg = f"Path is not a file: {script_path}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Prepare command
    cmd = ["bash", str(script_path)]
    if args:
        cmd.extend(args)

    logger.info(f"Executing Bash script: {script_path} with args: {args}")

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=300  # 5 minutes timeout
        )

        success = result.returncode == 0

        if success:
            logger.info(f"Bash script executed successfully: {script_path}")
        else:
            logger.error(f"Bash script failed with return code {result.returncode}: {script_path}")
            logger.error(f"Error output: {result.stderr}")

        return {
            "success": success,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }

    except subprocess.TimeoutExpired as e:
        error_msg = f"Bash script timed out: {script_path}"
        logger.error(error_msg)
        return {
            "success": False,
            "returncode": -1,
            "stdout": "",
            "stderr": str(e),
            "error": "Timeout",
        }

    except Exception as e:
        error_msg = f"Error executing Bash script {script_path}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"success": False, "returncode": -1, "stdout": "", "stderr": "", "error": str(e)}


def execute_python_job(script_path: str, args: List[str] = None) -> Dict[str, Any]:
    """
    Execute a Python script dynamically by importing and calling its main() function.

    Args:
        script_path: Path to the Python script
        args: List of arguments to pass to the main function (optional)

    Returns:
        Dict containing execution results with keys:
            - success: bool
            - result: Any (return value from main())
            - error: str (if exception occurred)

    Raises:
        FileNotFoundError: If script file doesn't exist
        ValueError: If path is not a file or not a Python file
    """
    script_path = Path(script_path)

    if not script_path.exists():
        error_msg = f"Python script not found: {script_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    if not script_path.is_file():
        error_msg = f"Path is not a file: {script_path}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    if script_path.suffix != ".py":
        error_msg = f"Not a Python file: {script_path}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(f"Executing Python script: {script_path} with args: {args}")

    try:
        # Import the module dynamically
        spec = importlib.util.spec_from_file_location(script_path.stem, script_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load spec for {script_path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[script_path.stem] = module
        spec.loader.exec_module(module)

        # Check if main() function exists
        if not hasattr(module, "main"):
            error_msg = f"Python script does not have a main() function: {script_path}"
            logger.error(error_msg)
            raise AttributeError(error_msg)

        # Call main() function with args
        main_func = getattr(module, "main")
        if args:
            result = main_func(*args)
        else:
            result = main_func()

        logger.info(f"Python script executed successfully: {script_path}")

        return {"success": True, "result": result}

    except Exception as e:
        error_msg = f"Error executing Python script {script_path}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"success": False, "result": None, "error": str(e)}

    finally:
        # Clean up module from sys.modules
        if script_path.stem in sys.modules:
            del sys.modules[script_path.stem]


def execute_job(job_type: str, script_path: str, args: List[str] = None) -> Dict[str, Any]:
    """
    Execute a job based on its type.

    Args:
        job_type: Type of job ('bash' or 'python')
        script_path: Path to the script
        args: List of arguments to pass to the script (optional)

    Returns:
        Dict containing execution results

    Raises:
        ValueError: If job_type is not supported
    """
    job_type = job_type.lower()

    if job_type == "bash":
        return execute_bash_job(script_path, args)
    elif job_type == "python":
        return execute_python_job(script_path, args)
    else:
        error_msg = f"Unsupported job type: {job_type}. Supported types: 'bash', 'python'"
        logger.error(error_msg)
        raise ValueError(error_msg)

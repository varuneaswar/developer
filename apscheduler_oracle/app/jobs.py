"""
Job execution module for Bash and Python scripts.

This module provides functions to execute Bash scripts and Python modules
with arguments, supporting dynamic imports and subprocess execution.
"""

import subprocess
import importlib
import sys
from typing import List, Any, Optional
from .logger import setup_logger


logger = setup_logger('jobs')


def execute_bash_script(script_path: str, args: Optional[List[str]] = None) -> dict:
    """
    Execute a Bash script with optional arguments.
    
    Args:
        script_path: Path to the Bash script
        args: Optional list of arguments to pass to the script
    
    Returns:
        dict: Execution result with returncode, stdout, and stderr
    
    Raises:
        FileNotFoundError: If the script file doesn't exist
        Exception: For other execution errors
    """
    logger.info(f"Executing Bash script: {script_path} with args: {args}")
    
    try:
        command = ['bash', script_path]
        if args:
            command.extend(args)
        
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes timeout
        )
        
        execution_result = {
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'success': result.returncode == 0
        }
        
        if result.returncode == 0:
            logger.info(f"Bash script executed successfully: {script_path}")
        else:
            logger.error(f"Bash script failed with code {result.returncode}: {script_path}")
            logger.error(f"Error output: {result.stderr}")
        
        return execution_result
        
    except subprocess.TimeoutExpired:
        logger.error(f"Bash script timed out: {script_path}")
        return {
            'returncode': -1,
            'stdout': '',
            'stderr': 'Script execution timed out',
            'success': False
        }
    except FileNotFoundError:
        logger.error(f"Bash script not found: {script_path}")
        raise
    except Exception as e:
        logger.error(f"Error executing Bash script {script_path}: {str(e)}")
        raise


def execute_python_module(
    module_name: str,
    function_name: str = 'main',
    args: Optional[List[Any]] = None,
    kwargs: Optional[dict] = None
) -> Any:
    """
    Dynamically import and execute a Python module's function.
    
    Args:
        module_name: Name of the Python module to import
        function_name: Name of the function to execute (default: 'main')
        args: Optional positional arguments for the function
        kwargs: Optional keyword arguments for the function
    
    Returns:
        Any: Result returned by the executed function
    
    Raises:
        ImportError: If the module cannot be imported
        AttributeError: If the function doesn't exist in the module
        Exception: For execution errors
    """
    logger.info(f"Executing Python module: {module_name}.{function_name}")
    
    try:
        # Import the module
        module = importlib.import_module(module_name)
        
        # Get the function
        if not hasattr(module, function_name):
            raise AttributeError(
                f"Module '{module_name}' has no function '{function_name}'"
            )
        
        func = getattr(module, function_name)
        
        # Execute the function with arguments
        args = args or []
        kwargs = kwargs or {}
        
        result = func(*args, **kwargs)
        
        logger.info(f"Python module executed successfully: {module_name}.{function_name}")
        return result
        
    except ImportError as e:
        logger.error(f"Failed to import module {module_name}: {str(e)}")
        raise
    except AttributeError as e:
        logger.error(str(e))
        raise
    except Exception as e:
        logger.error(
            f"Error executing Python module {module_name}.{function_name}: {str(e)}"
        )
        raise


def job_wrapper(job_type: str, **kwargs) -> dict:
    """
    Wrapper function for job execution.
    
    This function is called by the scheduler and routes to the appropriate
    execution function based on job type.
    
    Args:
        job_type: Type of job ('bash' or 'python')
        **kwargs: Job-specific parameters
    
    Returns:
        dict: Execution result
    """
    logger.info(f"Job wrapper called with type: {job_type}")
    
    try:
        if job_type == 'bash':
            script_path = kwargs.get('script_path')
            args = kwargs.get('args', [])
            return execute_bash_script(script_path, args)
            
        elif job_type == 'python':
            module_name = kwargs.get('module_name')
            function_name = kwargs.get('function_name', 'main')
            args = kwargs.get('args', [])
            kwargs_dict = kwargs.get('kwargs', {})
            result = execute_python_module(module_name, function_name, args, kwargs_dict)
            return {'success': True, 'result': result}
            
        else:
            raise ValueError(f"Unknown job type: {job_type}")
            
    except Exception as e:
        logger.error(f"Job execution failed: {str(e)}")
        return {'success': False, 'error': str(e)}

# APScheduler Oracle Job Scheduling Module

A Python module for job scheduling using APScheduler with Oracle as the job store backend. This module provides a REST API for managing scheduled jobs and supports both Bash script and Python module execution.

## Features

- **Scheduler Integration**: APScheduler with Oracle database backend via SQLAlchemy
- **Job Types**: Support for Bash scripts and Python modules
- **Date Triggers**: Schedule jobs to run at specific dates and times
- **REST API**: Flask-based API for job management (CRUD operations)
- **Job Control**: Pause, resume, and remove scheduled jobs
- **Robust Logging**: Comprehensive logging for monitoring and debugging
- **Error Handling**: Graceful error handling and timeout management

## Project Structure

```
apscheduler_oracle/
├── app/
│   ├── __init__.py       # Package initialization
│   ├── config.py         # Configuration (Oracle URI, API settings)
│   ├── logger.py         # Modular logging system
│   ├── jobs.py           # Job execution logic for Bash/Python
│   ├── scheduler.py      # APScheduler setup with Oracle backend
│   └── api.py            # REST API endpoints
├── tests/                # Unit tests
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

## Requirements

- Python 3.8+
- Oracle Database 11g or higher
- Oracle Instant Client (for cx_Oracle)

## Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd apscheduler_oracle
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up Oracle Instant Client:**
   - Download and install Oracle Instant Client for your platform
   - Set the appropriate environment variables (e.g., `LD_LIBRARY_PATH` on Linux)

4. **Configure environment variables:**
   ```bash
   export ORACLE_USER=your_user
   export ORACLE_PASSWORD=your_password
   export ORACLE_HOST=localhost
   export ORACLE_PORT=1521
   export ORACLE_SERVICE=XEPDB1
   export API_HOST=0.0.0.0
   export API_PORT=5000
   ```

## Configuration

The module uses environment variables for configuration. Available options:

### Database Configuration
- `ORACLE_USER`: Oracle database username (default: 'scheduler_user')
- `ORACLE_PASSWORD`: Oracle database password (default: 'scheduler_password')
- `ORACLE_HOST`: Oracle database host (default: 'localhost')
- `ORACLE_PORT`: Oracle database port (default: '1521')
- `ORACLE_SERVICE`: Oracle service name (default: 'XEPDB1')

### API Configuration
- `API_HOST`: API server host (default: '0.0.0.0')
- `API_PORT`: API server port (default: '5000')
- `API_DEBUG`: Enable debug mode (default: 'False')

### Scheduler Configuration
- `SCHEDULER_JOBSTORE_TABLENAME`: Job store table name (default: 'apscheduler_jobs')
- `SCHEDULER_TIMEZONE`: Scheduler timezone (default: 'UTC')

### Logging Configuration
- `LOG_LEVEL`: Logging level (default: 'INFO')
- `LOG_FILE`: Log file path (default: 'scheduler.log')

## Usage

### Starting the API Server

```python
from app.api import run_api_server

run_api_server()
```

Or run directly:
```bash
python -m app.api
```

### API Endpoints

#### 1. Health Check
```bash
GET /health
```

#### 2. Schedule a Job
```bash
POST /api/schedule-job
Content-Type: application/json

# Bash job example:
{
    "job_id": "my_bash_job_1",
    "job_type": "bash",
    "run_date": "2024-12-31T23:59:59",
    "script_path": "/path/to/script.sh",
    "args": ["arg1", "arg2"]
}

# Python job example:
{
    "job_id": "my_python_job_1",
    "job_type": "python",
    "run_date": "2024-12-31T23:59:59",
    "module_name": "mymodule",
    "function_name": "main",
    "args": ["arg1"],
    "kwargs": {"key": "value"}
}
```

#### 3. List All Jobs
```bash
GET /api/jobs
```

#### 4. Get Job Details
```bash
GET /api/jobs/{job_id}
```

#### 5. Remove a Job
```bash
DELETE /api/remove-job/{job_id}
```

#### 6. Pause a Job
```bash
POST /api/jobs/{job_id}/pause
```

#### 7. Resume a Job
```bash
POST /api/jobs/{job_id}/resume
```

### Python API Usage

```python
from datetime import datetime, timedelta
from app.scheduler import get_scheduler_manager

# Get scheduler manager
scheduler = get_scheduler_manager()

# Start the scheduler
scheduler.start()

# Schedule a Bash job
scheduler.add_bash_job(
    job_id='backup_job',
    script_path='/path/to/backup.sh',
    run_date=datetime.now() + timedelta(hours=1),
    args=['--full', '--compress']
)

# Schedule a Python job
scheduler.add_python_job(
    job_id='data_processing',
    module_name='data_processor',
    function_name='process',
    run_date=datetime.now() + timedelta(days=1),
    args=[],
    kwargs={'mode': 'production'}
)

# List all jobs
jobs = scheduler.get_all_jobs()
print(jobs)

# Pause a job
scheduler.pause_job('backup_job')

# Resume a job
scheduler.resume_job('backup_job')

# Remove a job
scheduler.remove_job('backup_job')

# Shutdown the scheduler
scheduler.shutdown()
```

## Testing

Run tests using pytest:

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest --cov=app tests/

# Run specific test file
pytest tests/test_jobs.py

# Run with verbose output
pytest -v tests/
```

## Job Execution

### Bash Scripts
- Scripts are executed using Python's `subprocess` module
- A 5-minute timeout is enforced by default
- Return code, stdout, and stderr are captured
- Scripts must be executable or run through `bash`

### Python Modules
- Modules are dynamically imported using `importlib`
- The specified function (default: `main`) is called with provided arguments
- Both positional and keyword arguments are supported
- Module must be in Python's import path

## Logging

The module provides comprehensive logging:

- **Console Output**: All logs are output to stdout
- **File Output**: Logs are written to `scheduler.log` (configurable)
- **Log Levels**: DEBUG, INFO, WARNING, ERROR, CRITICAL
- **Components**: Separate loggers for API, scheduler, and jobs

## Error Handling

- API errors return appropriate HTTP status codes
- Job execution failures are logged with details
- Scheduler errors are captured and logged
- Database connection issues are handled gracefully

## Security Considerations

1. **Database Credentials**: Store credentials securely (environment variables, secrets manager)
2. **Script Execution**: Validate script paths to prevent unauthorized execution
3. **API Access**: Consider adding authentication middleware for production use
4. **Input Validation**: All API inputs are validated before processing

## Troubleshooting

### Oracle Connection Issues
- Verify Oracle Instant Client is installed correctly
- Check environment variables for database connection
- Ensure Oracle service is running and accessible
- Verify user has necessary permissions

### Job Execution Failures
- Check log files for detailed error messages
- Verify script paths and permissions
- Ensure Python modules are importable
- Check for timeout issues with long-running jobs

### API Issues
- Verify Flask is running on the correct host/port
- Check firewall rules for API port
- Review API logs for request/response details

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

[Specify your license here]

## Support

For issues and questions:
- Create an issue in the repository
- Check the logs for detailed error messages
- Review the documentation and examples

## Changelog

### Version 1.0.0
- Initial release
- APScheduler integration with Oracle
- REST API for job management
- Support for Bash and Python jobs
- Comprehensive logging and error handling

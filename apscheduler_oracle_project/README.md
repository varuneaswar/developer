# APScheduler Oracle Integration

A comprehensive Python module for dynamic job scheduling with Oracle database backend using APScheduler. This module provides REST API endpoints for managing scheduled Bash and Python script executions.

## Features

- **Dynamic Job Scheduling**: Schedule Bash and Python scripts with date/time-based triggers
- **Oracle Database Backend**: Persistent job storage using Oracle database
- **REST API**: Full-featured Flask-based API for job management
- **Job Management**: Add, remove, pause, resume, and list scheduled jobs
- **Flexible Configuration**: Environment-based configuration with `.env` support
- **Comprehensive Logging**: Detailed logging for auditing and debugging
- **Robust Testing**: Unit tests for all components with mocked database

## Project Structure

```
apscheduler_oracle_project/
│
├── app/
│   ├── __init__.py                 # Module initializer
│   ├── config.py                   # Configuration settings
│   ├── jobs.py                     # Bash and Python job execution logic
│   ├── scheduler.py                # Scheduler setup and job handling
│   ├── api.py                      # REST API for job management
│   ├── logger.py                   # Logging setup
│
├── tests/
│   ├── __init__.py
│   ├── test_jobs.py                # Unit tests for job execution
│   ├── test_scheduler.py           # Unit tests for scheduler logic
│   ├── test_api.py                 # Unit tests for API endpoints
│
├── requirements.txt                # Dependencies
├── README.md                       # Documentation
├── main.py                         # Entry point to start API and scheduler
├── .env.example                    # Environment variables template
├── .gitignore                      # Git ignore rules
```

## Prerequisites

- Python 3.8 or higher
- Oracle Database 12c or higher
- Oracle Instant Client (for cx_Oracle)

## Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd apscheduler_oracle_project
```

### 2. Install Oracle Instant Client

Download and install Oracle Instant Client from [Oracle's website](https://www.oracle.com/database/technologies/instant-client/downloads.html).

Set the `LD_LIBRARY_PATH` (Linux/Mac) or `PATH` (Windows) environment variable:

```bash
# Linux/Mac
export LD_LIBRARY_PATH=/path/to/instantclient:$LD_LIBRARY_PATH

# Windows
set PATH=C:\path\to\instantclient;%PATH%
```

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Copy the example environment file and update with your Oracle database credentials:

```bash
cp .env.example .env
```

Edit `.env` file:

```env
# Oracle Database Configuration
ORACLE_DB_URI=oracle+cx_oracle://username:password@host:port/service_name

# Flask Configuration
FLASK_HOST=0.0.0.0
FLASK_PORT=5000
FLASK_DEBUG=False

# Logging Configuration
LOG_LEVEL=INFO
LOG_FILE=logs/scheduler.log

# Scheduler Configuration
SCHEDULER_TIMEZONE=UTC
```

### 5. Setup Oracle Database

Create the necessary tables for APScheduler:

```sql
-- APScheduler will automatically create the required tables
-- Ensure your Oracle user has the necessary privileges:
GRANT CREATE TABLE TO username;
GRANT CREATE SEQUENCE TO username;
```

## Usage

### Starting the Application

```bash
python main.py
```

The application will:
1. Initialize the scheduler with Oracle database backend
2. Start the Flask API server on the configured host and port
3. Begin accepting job scheduling requests

### API Endpoints

#### 1. Health Check

Check if the service is running.

```http
GET /api/health
```

**Response:**
```json
{
  "status": "healthy",
  "scheduler_running": true
}
```

#### 2. Schedule a Job

Schedule a new Bash or Python script execution.

```http
POST /api/schedule-job
Content-Type: application/json

{
  "job_id": "unique_job_id",
  "job_type": "bash",
  "script_path": "/path/to/script.sh",
  "run_date": "2024-12-31T23:59:59",
  "args": ["arg1", "arg2"],
  "replace_existing": false
}
```

**Parameters:**
- `job_id` (required): Unique identifier for the job
- `job_type` (required): Either "bash" or "python"
- `script_path` (required): Full path to the script file
- `run_date` (required): ISO 8601 datetime string (YYYY-MM-DDTHH:MM:SS)
- `args` (optional): List of arguments to pass to the script
- `replace_existing` (optional): Replace existing job with same ID (default: false)

**Response:**
```json
{
  "message": "Job scheduled successfully",
  "job": {
    "job_id": "unique_job_id",
    "name": "bash:/path/to/script.sh",
    "next_run_time": "2024-12-31T23:59:59+00:00",
    "job_type": "bash",
    "script_path": "/path/to/script.sh",
    "args": ["arg1", "arg2"]
  }
}
```

#### 3. List All Jobs

Get a list of all scheduled jobs.

```http
GET /api/jobs
```

**Response:**
```json
{
  "count": 2,
  "jobs": [
    {
      "job_id": "job_1",
      "name": "bash:/path/to/script.sh",
      "next_run_time": "2024-12-31T23:59:59+00:00",
      "trigger": "date[2024-12-31 23:59:59 UTC]",
      "executor": "default",
      "pending": false
    }
  ]
}
```

#### 4. Get Job Details

Get details of a specific job.

```http
GET /api/job/{job_id}
```

**Response:**
```json
{
  "job": {
    "job_id": "job_1",
    "name": "bash:/path/to/script.sh",
    "next_run_time": "2024-12-31T23:59:59+00:00",
    "trigger": "date[2024-12-31 23:59:59 UTC]",
    "executor": "default",
    "pending": false
  }
}
```

#### 5. Remove a Job

Remove a scheduled job.

```http
DELETE /api/remove-job/{job_id}
```

**Response:**
```json
{
  "message": "Job job_1 removed successfully"
}
```

#### 6. Pause a Job

Pause a scheduled job (prevents execution).

```http
PUT /api/pause-job/{job_id}
```

**Response:**
```json
{
  "message": "Job job_1 paused successfully"
}
```

#### 7. Resume a Job

Resume a paused job.

```http
PUT /api/resume-job/{job_id}
```

**Response:**
```json
{
  "message": "Job job_1 resumed successfully"
}
```

### Example Scripts

#### Bash Script Example

Create a simple Bash script:

```bash
#!/bin/bash
# test_script.sh

echo "Job started at: $(date)"
echo "Arguments: $@"
echo "Job completed successfully"
exit 0
```

Schedule it:

```bash
curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "bash_test_job",
    "job_type": "bash",
    "script_path": "/path/to/test_script.sh",
    "run_date": "2024-12-31T10:00:00",
    "args": ["arg1", "arg2"]
  }'
```

#### Python Script Example

Create a simple Python script:

```python
# test_script.py

def main(arg1=None, arg2=None):
    """Main function that will be called by the scheduler."""
    print(f"Job started with arguments: {arg1}, {arg2}")
    # Your logic here
    return "Job completed successfully"
```

Schedule it:

```bash
curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "python_test_job",
    "job_type": "python",
    "script_path": "/path/to/test_script.py",
    "run_date": "2024-12-31T10:00:00",
    "args": ["value1", "value2"]
  }'
```

## Testing

Run the test suite:

```bash
# Install test dependencies
pip install pytest pytest-cov pytest-mock

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=app --cov-report=html

# Run specific test file
pytest tests/test_jobs.py -v
```

## Logging

Logs are written to both console and file (configurable via `LOG_FILE`).

Default log location: `logs/scheduler.log`

Log format:
```
2024-12-28 10:00:00 - apscheduler_oracle - INFO - Job scheduled: job_1
```

## Configuration

All configuration is managed through environment variables or the `.env` file:

| Variable | Description | Default |
|----------|-------------|---------|
| `ORACLE_DB_URI` | Oracle database connection string | Required |
| `FLASK_HOST` | Flask server host | 0.0.0.0 |
| `FLASK_PORT` | Flask server port | 5000 |
| `FLASK_DEBUG` | Flask debug mode | False |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR) | INFO |
| `LOG_FILE` | Log file path | logs/scheduler.log |
| `SCHEDULER_TIMEZONE` | Scheduler timezone | UTC |

## Error Handling

The API returns appropriate HTTP status codes:

- `200`: Success
- `400`: Bad request (validation error)
- `404`: Resource not found
- `500`: Internal server error

Error response format:
```json
{
  "error": "Error message description"
}
```

## Security Considerations

1. **Database Credentials**: Store sensitive credentials in `.env` file (never commit to version control)
2. **Script Paths**: Validate and sanitize script paths to prevent directory traversal attacks
3. **API Authentication**: Consider adding authentication/authorization for production use
4. **Network Security**: Use HTTPS in production environments
5. **Input Validation**: All inputs are validated before processing

## Performance Considerations

1. **Thread Pool**: Default executor uses 10 worker threads (configurable)
2. **Database Connection Pool**: Managed by SQLAlchemy
3. **Job Coalescing**: Disabled by default to ensure all scheduled jobs run
4. **Max Instances**: Limited to 3 concurrent instances per job

## Troubleshooting

### Oracle Connection Issues

```
Error: DPI-1047: Cannot locate a 64-bit Oracle Client library
```

**Solution**: Install Oracle Instant Client and set `LD_LIBRARY_PATH`/`PATH` correctly.

### Import Errors

```
Error: No module named 'cx_Oracle'
```

**Solution**: Install dependencies: `pip install -r requirements.txt`

### Job Not Executing

1. Check job status: `GET /api/job/{job_id}`
2. Verify script path exists and is executable
3. Check logs for detailed error messages
4. Ensure run_date is in the future

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

This project is licensed under the MIT License.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review the logs
3. Open an issue on GitHub

## Acknowledgments

- [APScheduler](https://apscheduler.readthedocs.io/) - Advanced Python Scheduler
- [Flask](https://flask.palletsprojects.com/) - Web framework
- [cx_Oracle](https://oracle.github.io/python-cx_Oracle/) - Oracle database driver

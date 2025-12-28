# API Usage Examples

This document provides practical examples of using the APScheduler Oracle REST API.

## Starting the API Server

```bash
# Set environment variables
export ORACLE_USER=scheduler_user
export ORACLE_PASSWORD=scheduler_password
export ORACLE_HOST=localhost
export ORACLE_PORT=1521
export ORACLE_SERVICE=XEPDB1

# Run the server
python run.py
```

Or programmatically:

```python
from app.api import run_api_server

run_api_server()
```

## Health Check

Check if the API is running:

```bash
curl http://localhost:5000/health
```

Response:
```json
{
    "status": "healthy",
    "service": "apscheduler-oracle",
    "timestamp": "2024-12-31T12:00:00.000000+00:00"
}
```

## Scheduling Jobs

### Schedule a Bash Job

```bash
curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "backup_job_001",
    "job_type": "bash",
    "run_date": "2024-12-31T23:59:59",
    "script_path": "/opt/scripts/backup.sh",
    "args": ["--full", "--compress"]
  }'
```

Response:
```json
{
    "message": "Bash job scheduled successfully",
    "job_id": "backup_job_001",
    "run_date": "2024-12-31T23:59:59"
}
```

### Schedule a Python Job

```bash
curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "data_processing_001",
    "job_type": "python",
    "run_date": "2024-12-31T22:00:00",
    "module_name": "data_processor",
    "function_name": "process_daily_data",
    "args": ["2024-12-31"],
    "kwargs": {
      "mode": "production",
      "notify": true
    }
  }'
```

Response:
```json
{
    "message": "Python job scheduled successfully",
    "job_id": "data_processing_001",
    "run_date": "2024-12-31T22:00:00"
}
```

## Listing Jobs

### List All Jobs

```bash
curl http://localhost:5000/api/jobs
```

Response:
```json
{
    "jobs": [
        {
            "id": "backup_job_001",
            "name": "Bash: /opt/scripts/backup.sh",
            "next_run_time": "2024-12-31T23:59:59",
            "trigger": "date[2024-12-31 23:59:59 UTC]"
        },
        {
            "id": "data_processing_001",
            "name": "Python: data_processor.process_daily_data",
            "next_run_time": "2024-12-31T22:00:00",
            "trigger": "date[2024-12-31 22:00:00 UTC]"
        }
    ],
    "count": 2
}
```

### Get Specific Job Details

```bash
curl http://localhost:5000/api/jobs/backup_job_001
```

Response:
```json
{
    "id": "backup_job_001",
    "name": "Bash: /opt/scripts/backup.sh",
    "next_run_time": "2024-12-31T23:59:59",
    "trigger": "date[2024-12-31 23:59:59 UTC]"
}
```

## Managing Jobs

### Pause a Job

```bash
curl -X POST http://localhost:5000/api/jobs/backup_job_001/pause
```

Response:
```json
{
    "message": "Job paused successfully",
    "job_id": "backup_job_001"
}
```

### Resume a Job

```bash
curl -X POST http://localhost:5000/api/jobs/backup_job_001/resume
```

Response:
```json
{
    "message": "Job resumed successfully",
    "job_id": "backup_job_001"
}
```

### Remove a Job

```bash
curl -X DELETE http://localhost:5000/api/remove-job/backup_job_001
```

Response:
```json
{
    "message": "Job removed successfully",
    "job_id": "backup_job_001"
}
```

## Error Handling

### Missing Required Fields

Request:
```bash
curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "test_job"
  }'
```

Response (400):
```json
{
    "error": "Missing required fields: job_type, run_date"
}
```

### Invalid Date Format

Request:
```bash
curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "test_job",
    "job_type": "bash",
    "run_date": "invalid-date",
    "script_path": "/path/to/script.sh"
  }'
```

Response (400):
```json
{
    "error": "Invalid run_date format: ..."
}
```

### Job Not Found

Request:
```bash
curl http://localhost:5000/api/jobs/nonexistent_job
```

Response (404):
```json
{
    "error": "Job not found: nonexistent_job"
}
```

## Python Client Example

```python
import requests
from datetime import datetime, timedelta

# Base URL
BASE_URL = "http://localhost:5000"

# Schedule a Bash job
def schedule_bash_job():
    url = f"{BASE_URL}/api/schedule-job"
    run_time = datetime.now() + timedelta(hours=1)
    
    payload = {
        "job_id": "my_bash_job",
        "job_type": "bash",
        "run_date": run_time.isoformat(),
        "script_path": "/path/to/script.sh",
        "args": ["arg1", "arg2"]
    }
    
    response = requests.post(url, json=payload)
    return response.json()

# List all jobs
def list_jobs():
    url = f"{BASE_URL}/api/jobs"
    response = requests.get(url)
    return response.json()

# Get job details
def get_job(job_id):
    url = f"{BASE_URL}/api/jobs/{job_id}"
    response = requests.get(url)
    return response.json()

# Pause a job
def pause_job(job_id):
    url = f"{BASE_URL}/api/jobs/{job_id}/pause"
    response = requests.post(url)
    return response.json()

# Resume a job
def resume_job(job_id):
    url = f"{BASE_URL}/api/jobs/{job_id}/resume"
    response = requests.post(url)
    return response.json()

# Remove a job
def remove_job(job_id):
    url = f"{BASE_URL}/api/remove-job/{job_id}"
    response = requests.delete(url)
    return response.json()

# Example usage
if __name__ == "__main__":
    # Schedule a job
    result = schedule_bash_job()
    print(f"Scheduled: {result}")
    
    # List all jobs
    jobs = list_jobs()
    print(f"Total jobs: {jobs['count']}")
    
    # Pause the job
    pause_result = pause_job("my_bash_job")
    print(f"Paused: {pause_result}")
    
    # Resume the job
    resume_result = resume_job("my_bash_job")
    print(f"Resumed: {resume_result}")
    
    # Remove the job
    remove_result = remove_job("my_bash_job")
    print(f"Removed: {remove_result}")
```

## Testing with curl

Complete workflow example:

```bash
# 1. Check health
curl http://localhost:5000/health

# 2. Schedule a job
curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "test_job_001",
    "job_type": "bash",
    "run_date": "2024-12-31T23:59:59",
    "script_path": "/bin/echo",
    "args": ["Hello World"]
  }'

# 3. List all jobs
curl http://localhost:5000/api/jobs

# 4. Get job details
curl http://localhost:5000/api/jobs/test_job_001

# 5. Pause the job
curl -X POST http://localhost:5000/api/jobs/test_job_001/pause

# 6. Resume the job
curl -X POST http://localhost:5000/api/jobs/test_job_001/resume

# 7. Remove the job
curl -X DELETE http://localhost:5000/api/remove-job/test_job_001

# 8. Verify removal
curl http://localhost:5000/api/jobs/test_job_001
```

## Notes

- All timestamps should be in ISO 8601 format
- The `run_date` field supports timezone information (e.g., `2024-12-31T23:59:59+00:00`)
- Job IDs must be unique
- Bash scripts should have execute permissions
- Python modules must be in the Python path

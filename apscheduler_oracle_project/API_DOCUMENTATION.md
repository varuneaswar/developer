# API Documentation

## APScheduler Oracle Integration REST API

This document provides detailed API documentation for the APScheduler Oracle Integration module.

---

## Base URL
```
http://localhost:5000/api
```

---

## Endpoints

### 1. Health Check

Check the health status of the scheduler service.

**Endpoint:** `GET /api/health`

**Response:**
```json
{
  "status": "healthy",
  "scheduler_running": true
}
```

**Status Codes:**
- `200 OK` - Service is healthy

---

### 2. Schedule a Job

Schedule a new Bash or Python script for execution at a specific date/time.

**Endpoint:** `POST /api/schedule-job`

**Headers:**
```
Content-Type: application/json
```

**Request Body:**
```json
{
  "job_id": "string (required)",
  "job_type": "bash|python (required)",
  "script_path": "string (required)",
  "run_date": "string (required, ISO 8601 format)",
  "args": ["string", "..."] (optional),
  "replace_existing": false (optional)
}
```

**Parameters:**
- `job_id`: Unique identifier for the job
- `job_type`: Type of job - either "bash" or "python"
- `script_path`: Absolute path to the script file
- `run_date`: ISO 8601 datetime string (YYYY-MM-DDTHH:MM:SS)
- `args`: Optional array of string arguments to pass to the script
- `replace_existing`: Optional boolean to replace existing job with same ID

**Example Request:**
```bash
curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "data_backup_job",
    "job_type": "bash",
    "script_path": "/opt/scripts/backup.sh",
    "run_date": "2024-12-31T23:59:59",
    "args": ["--full", "--compress"]
  }'
```

**Success Response (200 OK):**
```json
{
  "message": "Job scheduled successfully",
  "job": {
    "job_id": "data_backup_job",
    "name": "bash:/opt/scripts/backup.sh",
    "next_run_time": "2024-12-31T23:59:59+00:00",
    "job_type": "bash",
    "script_path": "/opt/scripts/backup.sh",
    "args": ["--full", "--compress"]
  }
}
```

**Error Responses:**
- `400 Bad Request` - Missing required fields or invalid parameters
  ```json
  {
    "error": "Missing required fields: run_date"
  }
  ```
- `500 Internal Server Error` - Server error
  ```json
  {
    "error": "Internal server error: <error message>"
  }
  ```

---

### 3. List All Jobs

Get a list of all scheduled jobs.

**Endpoint:** `GET /api/jobs`

**Response (200 OK):**
```json
{
  "count": 2,
  "jobs": [
    {
      "job_id": "data_backup_job",
      "name": "bash:/opt/scripts/backup.sh",
      "next_run_time": "2024-12-31T23:59:59+00:00",
      "trigger": "date[2024-12-31 23:59:59 UTC]",
      "executor": "default",
      "pending": false
    },
    {
      "job_id": "report_generator",
      "name": "python:/opt/scripts/generate_report.py",
      "next_run_time": "2025-01-01T00:00:00+00:00",
      "trigger": "date[2025-01-01 00:00:00 UTC]",
      "executor": "default",
      "pending": false
    }
  ]
}
```

**Status Codes:**
- `200 OK` - Successfully retrieved jobs list
- `500 Internal Server Error` - Server error

---

### 4. Get Job Details

Get details of a specific job by ID.

**Endpoint:** `GET /api/job/{job_id}`

**Path Parameters:**
- `job_id`: The unique identifier of the job

**Example Request:**
```bash
curl -X GET http://localhost:5000/api/job/data_backup_job
```

**Success Response (200 OK):**
```json
{
  "job": {
    "job_id": "data_backup_job",
    "name": "bash:/opt/scripts/backup.sh",
    "next_run_time": "2024-12-31T23:59:59+00:00",
    "trigger": "date[2024-12-31 23:59:59 UTC]",
    "executor": "default",
    "pending": false
  }
}
```

**Error Response:**
- `404 Not Found` - Job not found
  ```json
  {
    "error": "Job not found: data_backup_job"
  }
  ```

---

### 5. Remove a Job

Remove a scheduled job from the system.

**Endpoint:** `DELETE /api/remove-job/{job_id}`

**Path Parameters:**
- `job_id`: The unique identifier of the job to remove

**Example Request:**
```bash
curl -X DELETE http://localhost:5000/api/remove-job/data_backup_job
```

**Success Response (200 OK):**
```json
{
  "message": "Job data_backup_job removed successfully"
}
```

**Error Responses:**
- `404 Not Found` - Job not found
  ```json
  {
    "error": "Job not found: data_backup_job"
  }
  ```
- `500 Internal Server Error` - Failed to remove job

---

### 6. Pause a Job

Pause a scheduled job (prevents execution without removing it).

**Endpoint:** `PUT /api/pause-job/{job_id}`

**Path Parameters:**
- `job_id`: The unique identifier of the job to pause

**Example Request:**
```bash
curl -X PUT http://localhost:5000/api/pause-job/data_backup_job
```

**Success Response (200 OK):**
```json
{
  "message": "Job data_backup_job paused successfully"
}
```

**Error Responses:**
- `404 Not Found` - Job not found
- `500 Internal Server Error` - Failed to pause job

---

### 7. Resume a Job

Resume a previously paused job.

**Endpoint:** `PUT /api/resume-job/{job_id}`

**Path Parameters:**
- `job_id`: The unique identifier of the job to resume

**Example Request:**
```bash
curl -X PUT http://localhost:5000/api/resume-job/data_backup_job
```

**Success Response (200 OK):**
```json
{
  "message": "Job data_backup_job resumed successfully"
}
```

**Error Responses:**
- `404 Not Found` - Job not found
- `500 Internal Server Error` - Failed to resume job

---

## Error Handling

All API endpoints return consistent error responses:

**Error Response Format:**
```json
{
  "error": "Error description message"
}
```

**HTTP Status Codes:**
- `200 OK` - Request successful
- `400 Bad Request` - Invalid request parameters or validation error
- `404 Not Found` - Resource not found
- `500 Internal Server Error` - Server error

---

## Complete API Usage Example

### Scenario: Schedule a Python Data Processing Job

1. **Schedule the job:**
```bash
curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "daily_data_processing",
    "job_type": "python",
    "script_path": "/opt/scripts/process_data.py",
    "run_date": "2025-01-01T02:00:00",
    "args": ["--dataset", "sales", "--format", "csv"]
  }'
```

2. **Verify the job was scheduled:**
```bash
curl -X GET http://localhost:5000/api/job/daily_data_processing
```

3. **Pause the job temporarily:**
```bash
curl -X PUT http://localhost:5000/api/pause-job/daily_data_processing
```

4. **Resume the job:**
```bash
curl -X PUT http://localhost:5000/api/resume-job/daily_data_processing
```

5. **List all jobs:**
```bash
curl -X GET http://localhost:5000/api/jobs
```

6. **Remove the job:**
```bash
curl -X DELETE http://localhost:5000/api/remove-job/daily_data_processing
```

---

## Script Requirements

### Bash Scripts
- Must be executable (`chmod +x script.sh`)
- Should use proper exit codes (0 for success, non-zero for failure)
- Arguments are passed as positional parameters ($1, $2, etc.)

**Example Bash Script:**
```bash
#!/bin/bash
echo "Processing with arguments: $@"
# Your logic here
exit 0
```

### Python Scripts
- Must contain a `main()` function
- Arguments are passed as function parameters
- Return value from `main()` is captured in job execution results

**Example Python Script:**
```python
def main(*args):
    """Main function called by scheduler."""
    print(f"Processing with arguments: {args}")
    # Your logic here
    return "Success"
```

---

## Best Practices

1. **Job IDs**: Use descriptive, unique IDs for jobs
2. **Script Paths**: Use absolute paths for script locations
3. **Error Handling**: Implement proper error handling in scripts
4. **Logging**: Use logging in scripts for debugging
5. **Testing**: Test scripts independently before scheduling
6. **Timezone**: All times are in UTC by default (configurable)
7. **Monitoring**: Regularly check job status using the list endpoint

---

## Integration Examples

### Python Integration
```python
import requests
import json
from datetime import datetime, timedelta

# Schedule a job
url = "http://localhost:5000/api/schedule-job"
run_time = (datetime.now() + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S')

payload = {
    "job_id": "my_job",
    "job_type": "python",
    "script_path": "/path/to/script.py",
    "run_date": run_time,
    "args": ["arg1", "arg2"]
}

response = requests.post(url, json=payload)
print(response.json())
```

### cURL Integration
```bash
#!/bin/bash
# Schedule multiple jobs

BASE_URL="http://localhost:5000/api"

# Job 1: Backup
curl -X POST "$BASE_URL/schedule-job" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "backup_job",
    "job_type": "bash",
    "script_path": "/opt/backup.sh",
    "run_date": "2025-01-01T00:00:00"
  }'

# Job 2: Report
curl -X POST "$BASE_URL/schedule-job" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "report_job",
    "job_type": "python",
    "script_path": "/opt/report.py",
    "run_date": "2025-01-01T06:00:00"
  }'

# List all jobs
curl -X GET "$BASE_URL/jobs"
```

---

## Troubleshooting

### Common Issues

1. **Job not executing:**
   - Check if script path is correct and accessible
   - Verify script has execute permissions (for Bash)
   - Check scheduler logs for errors
   - Ensure run_date is in the future

2. **400 Bad Request:**
   - Verify all required fields are provided
   - Check datetime format (ISO 8601)
   - Ensure job_type is either 'bash' or 'python'

3. **404 Not Found:**
   - Verify job_id is correct
   - Check if job exists using `/api/jobs` endpoint

4. **500 Internal Server Error:**
   - Check server logs
   - Verify Oracle database connection
   - Ensure scheduler is running properly

---

## API Rate Limits

Currently, there are no rate limits imposed on the API. For production use, consider implementing:
- Request rate limiting
- Authentication/Authorization
- API key validation

---

## Security Considerations

1. **Authentication**: Implement authentication before production use
2. **Authorization**: Add role-based access control
3. **Input Validation**: All inputs are validated server-side
4. **Script Execution**: Only execute scripts from trusted locations
5. **HTTPS**: Use HTTPS in production environments
6. **Database Security**: Secure Oracle database credentials

---

For more information, see the main [README.md](README.md) file.

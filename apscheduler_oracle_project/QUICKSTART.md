# Quick Start Guide

Get started with APScheduler Oracle Integration in 5 minutes!

## Prerequisites

- Python 3.8+
- Oracle Database 12c+
- Oracle Instant Client

## Installation

### 1. Install Oracle Instant Client

**Linux:**
```bash
# Download from Oracle website
wget https://download.oracle.com/otn_software/linux/instantclient/instantclient-basic-linux.zip
unzip instantclient-basic-linux.zip
export LD_LIBRARY_PATH=/path/to/instantclient:$LD_LIBRARY_PATH
```

**macOS:**
```bash
brew install instantclient-basic
```

### 2. Install Python Dependencies

```bash
cd apscheduler_oracle_project
pip install -r requirements.txt
```

### 3. Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your Oracle credentials
nano .env
```

Update `.env`:
```env
ORACLE_DB_URI=oracle+cx_oracle://your_user:your_password@localhost:1521/XEPDB1
FLASK_HOST=0.0.0.0
FLASK_PORT=5000
```

## Quick Test

### 1. Start the Server

```bash
python main.py
```

You should see:
```
2024-12-28 10:00:00 - apscheduler_oracle - INFO - Starting APScheduler Oracle Integration
2024-12-28 10:00:00 - apscheduler_oracle - INFO - Scheduler started successfully
 * Running on http://0.0.0.0:5000
```

### 2. Test Health Check

```bash
curl http://localhost:5000/api/health
```

Expected response:
```json
{
  "status": "healthy",
  "scheduler_running": true
}
```

### 3. Schedule Your First Job

**Option A: Bash Script**

Create a test script:
```bash
cat > /tmp/test.sh << 'EOF'
#!/bin/bash
echo "Hello from scheduled job!"
echo "Current time: $(date)"
EOF

chmod +x /tmp/test.sh
```

Schedule it:
```bash
# Schedule for 1 minute from now
RUN_TIME=$(date -u -d '+1 minute' '+%Y-%m-%dT%H:%M:%S')

curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d "{
    \"job_id\": \"test_job_1\",
    \"job_type\": \"bash\",
    \"script_path\": \"/tmp/test.sh\",
    \"run_date\": \"$RUN_TIME\"
  }"
```

**Option B: Python Script**

Create a test script:
```bash
cat > /tmp/test.py << 'EOF'
from datetime import datetime

def main():
    print("Hello from scheduled Python job!")
    print(f"Current time: {datetime.now()}")
    return "Success"
EOF
```

Schedule it:
```bash
# Schedule for 1 minute from now
RUN_TIME=$(date -u -d '+1 minute' '+%Y-%m-%dT%H:%M:%S')

curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d "{
    \"job_id\": \"test_job_2\",
    \"job_type\": \"python\",
    \"script_path\": \"/tmp/test.py\",
    \"run_date\": \"$RUN_TIME\"
  }"
```

### 4. Check Scheduled Jobs

```bash
curl http://localhost:5000/api/jobs
```

### 5. Monitor Execution

Watch the server logs to see your job execute:
```bash
tail -f logs/scheduler.log
```

## Common Tasks

### Schedule a Job with Arguments

```bash
curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "backup_job",
    "job_type": "bash",
    "script_path": "/opt/backup.sh",
    "run_date": "2025-01-01T00:00:00",
    "args": ["--full", "--compress"]
  }'
```

### Pause a Job

```bash
curl -X PUT http://localhost:5000/api/pause-job/backup_job
```

### Resume a Job

```bash
curl -X PUT http://localhost:5000/api/resume-job/backup_job
```

### Remove a Job

```bash
curl -X DELETE http://localhost:5000/api/remove-job/backup_job
```

### Get Job Details

```bash
curl http://localhost:5000/api/job/backup_job
```

## Using the Example Scripts

The project includes example scripts you can use for testing:

### Bash Example

```bash
# Schedule the example Bash script
RUN_TIME=$(date -u -d '+1 minute' '+%Y-%m-%dT%H:%M:%S')

curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d "{
    \"job_id\": \"example_bash\",
    \"job_type\": \"bash\",
    \"script_path\": \"$(pwd)/examples/example_bash_job.sh\",
    \"run_date\": \"$RUN_TIME\",
    \"args\": [\"arg1\", \"arg2\"]
  }"
```

### Python Example

```bash
# Schedule the example Python script
RUN_TIME=$(date -u -d '+1 minute' '+%Y-%m-%dT%H:%M:%S')

curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d "{
    \"job_id\": \"example_python\",
    \"job_type\": \"python\",
    \"script_path\": \"$(pwd)/examples/example_python_job.py\",
    \"run_date\": \"$RUN_TIME\",
    \"args\": [\"value1\", \"value2\"]
  }"
```

## Testing

Run the test suite:

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=app --cov-report=html

# Run specific test file
pytest tests/test_jobs.py -v
```

## Troubleshooting

### Oracle Connection Error

```
Error: Cannot connect to Oracle database
```

**Solution:**
1. Verify Oracle database is running
2. Check ORACLE_DB_URI in .env
3. Ensure Oracle Instant Client is installed
4. Verify LD_LIBRARY_PATH is set correctly

### Script Not Executing

**Check:**
1. Script path is absolute
2. Script has execute permissions (Bash)
3. Python script has main() function
4. Run date is in the future
5. Check scheduler logs

### Import Errors

```
ModuleNotFoundError: No module named 'apscheduler'
```

**Solution:**
```bash
pip install -r requirements.txt
```

## Next Steps

1. Read the [API Documentation](API_DOCUMENTATION.md) for detailed endpoint information
2. Review the [README.md](README.md) for comprehensive documentation
3. Check example scripts in the `examples/` directory
4. Customize configuration in `.env` file
5. Implement your own job scripts
6. Set up monitoring and alerting
7. Add authentication for production use

## Production Checklist

Before deploying to production:

- [ ] Configure proper Oracle database credentials
- [ ] Set up HTTPS/SSL
- [ ] Implement authentication/authorization
- [ ] Configure proper logging levels
- [ ] Set up log rotation
- [ ] Implement monitoring and alerting
- [ ] Test backup and recovery procedures
- [ ] Set up firewall rules
- [ ] Review and harden security settings
- [ ] Document operational procedures

## Getting Help

- Check [README.md](README.md) for detailed documentation
- Review [API_DOCUMENTATION.md](API_DOCUMENTATION.md) for API reference
- Check logs in `logs/scheduler.log`
- Review test files in `tests/` for usage examples

## Example Use Cases

### Daily Backup Job

```bash
curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "daily_backup",
    "job_type": "bash",
    "script_path": "/opt/scripts/backup.sh",
    "run_date": "2025-01-02T02:00:00"
  }'
```

### Weekly Report Generation

```bash
curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "weekly_report",
    "job_type": "python",
    "script_path": "/opt/scripts/generate_report.py",
    "run_date": "2025-01-06T09:00:00",
    "args": ["--format", "pdf", "--email", "team@example.com"]
  }'
```

### Data Processing Job

```bash
curl -X POST http://localhost:5000/api/schedule-job \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "data_processing",
    "job_type": "python",
    "script_path": "/opt/scripts/process_data.py",
    "run_date": "2025-01-01T00:00:00",
    "args": ["--dataset", "sales", "--year", "2024"]
  }'
```

Happy Scheduling! 🚀

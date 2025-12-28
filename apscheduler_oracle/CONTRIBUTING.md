# Contributing to APScheduler Oracle Job Scheduling Module

Thank you for your interest in contributing to this project! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Python 3.8 or higher
- Oracle Database 11g or higher (for integration testing)
- Oracle Instant Client
- Git

### Setting Up Development Environment

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd apscheduler_oracle
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables:**
   ```bash
   export ORACLE_USER=your_user
   export ORACLE_PASSWORD=your_password
   export ORACLE_HOST=localhost
   export ORACLE_PORT=1521
   export ORACLE_SERVICE=XEPDB1
   ```

## Code Style and Quality

### Code Formatting

We use **Black** for code formatting with a line length of 100 characters:

```bash
black app/ tests/ --line-length=100
```

### Linting

We use **flake8** for linting:

```bash
flake8 app/ tests/ --max-line-length=100
```

### Type Checking

We use **mypy** for static type checking:

```bash
mypy app/ --ignore-missing-imports
```

### Running All Quality Checks

```bash
# Format code
black app/ tests/ --line-length=100

# Run linter
flake8 app/ tests/ --max-line-length=100

# Run type checker
mypy app/ --ignore-missing-imports

# Run tests with coverage
pytest --cov=app --cov-report=term-missing tests/
```

## Testing

### Running Tests

```bash
# Run all tests
pytest tests/

# Run with verbose output
pytest -v tests/

# Run with coverage
pytest --cov=app --cov-report=term-missing tests/

# Run specific test file
pytest tests/test_api.py

# Run specific test class
pytest tests/test_api.py::TestScheduleJobEndpoint

# Run specific test
pytest tests/test_api.py::TestScheduleJobEndpoint::test_schedule_bash_job_success
```

### Writing Tests

- Place tests in the `tests/` directory
- Follow the naming convention: `test_*.py`
- Use descriptive test names that explain what is being tested
- Use the Arrange-Act-Assert pattern
- Mock external dependencies (database, file system, etc.)

Example:
```python
def test_schedule_bash_job_success(self, client, mock_scheduler):
    """Test scheduling a Bash job successfully."""
    # Arrange
    mock_scheduler.add_bash_job.return_value = 'test_job_1'
    payload = {...}
    
    # Act
    response = client.post('/api/schedule-job', json=payload)
    
    # Assert
    assert response.status_code == 201
```

## Code Organization

### Module Structure

```
apscheduler_oracle/
├── app/
│   ├── __init__.py      # Package initialization
│   ├── config.py        # Configuration management
│   ├── logger.py        # Logging setup
│   ├── jobs.py          # Job execution logic
│   ├── scheduler.py     # Scheduler management
│   └── api.py           # REST API endpoints
├── tests/               # Unit tests
├── requirements.txt     # Dependencies
├── pyproject.toml      # Project configuration
└── README.md           # Documentation
```

### Coding Guidelines

1. **Docstrings**: Use Google-style docstrings for all functions and classes
2. **Type Hints**: Use type hints for function parameters and return values
3. **Error Handling**: Always handle exceptions appropriately and log errors
4. **Logging**: Use the logger module for all logging
5. **Configuration**: Use environment variables for configuration
6. **Constants**: Define constants in config.py

## Making Changes

### Workflow

1. **Create a branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes:**
   - Write code following the style guidelines
   - Add or update tests
   - Update documentation

3. **Run quality checks:**
   ```bash
   black app/ tests/ --line-length=100
   flake8 app/ tests/ --max-line-length=100
   pytest --cov=app tests/
   ```

4. **Commit your changes:**
   ```bash
   git add .
   git commit -m "Description of changes"
   ```

5. **Push to your branch:**
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request:**
   - Provide a clear description of changes
   - Reference any related issues
   - Ensure all tests pass

### Commit Messages

Follow these guidelines for commit messages:

- Use present tense ("Add feature" not "Added feature")
- Use imperative mood ("Move cursor to..." not "Moves cursor to...")
- First line should be 50 characters or less
- Reference issues and pull requests liberally

Examples:
- `Add support for cron triggers`
- `Fix issue with job removal`
- `Update documentation for API endpoints`

## Adding Features

### New API Endpoints

1. Add the endpoint in `app/api.py`
2. Add corresponding tests in `tests/test_api.py`
3. Update API documentation in README.md
4. Ensure proper error handling and logging

### New Job Types

1. Add execution logic in `app/jobs.py`
2. Add scheduling support in `app/scheduler.py`
3. Add tests in `tests/test_jobs.py`
4. Update documentation

## Reporting Issues

When reporting issues, please include:

- Python version
- Oracle database version
- Operating system
- Steps to reproduce
- Expected behavior
- Actual behavior
- Error messages and logs

## Security

- Never commit credentials or sensitive data
- Use environment variables for configuration
- Validate all user inputs
- Report security vulnerabilities privately

## Questions?

If you have questions or need help:

- Check the README.md for documentation
- Review existing issues and pull requests
- Create a new issue with your question

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.

#!/bin/bash
# Example Bash script for testing job execution

echo "================================"
echo "Bash Job Execution Started"
echo "================================"
echo "Timestamp: $(date)"
echo "Script: $0"
echo "Arguments: $@"
echo "Working Directory: $(pwd)"
echo "================================"

# Process arguments
if [ $# -gt 0 ]; then
    echo "Processing arguments:"
    for arg in "$@"; do
        echo "  - $arg"
    done
fi

# Simulate some work
echo "Performing job tasks..."
sleep 2

echo "Job completed successfully!"
echo "================================"
exit 0

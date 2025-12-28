"""
Example Python script for testing job execution.
This script must have a main() function that will be called by the scheduler.
"""
import time
from datetime import datetime


def main(*args):
    """
    Main function called by the scheduler.
    
    Args:
        *args: Variable length argument list passed from scheduler
    
    Returns:
        str: Status message
    """
    print("=" * 50)
    print("Python Job Execution Started")
    print("=" * 50)
    print(f"Timestamp: {datetime.now()}")
    print(f"Arguments: {args}")
    print("=" * 50)
    
    # Process arguments
    if args:
        print("Processing arguments:")
        for i, arg in enumerate(args, 1):
            print(f"  Arg {i}: {arg}")
    
    # Simulate some work
    print("Performing job tasks...")
    time.sleep(2)
    
    print("Job completed successfully!")
    print("=" * 50)
    
    return "Job execution completed"


if __name__ == "__main__":
    # For testing purposes
    result = main("test_arg1", "test_arg2")
    print(f"Result: {result}")

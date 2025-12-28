#!/usr/bin/env python3
"""
Main entry point for the APScheduler Oracle application.

This script starts the Flask API server with the scheduler.
"""

import sys
import os

# Add the parent directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.api import run_api_server

if __name__ == '__main__':
    run_api_server()

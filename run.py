#!/usr/bin/env python3
"""
NPI Provider Suppression Rule Engine

A streamlined entry point that delegates to the refactored CLI module.
"""

import sys
import os

# Add current directory to Python path to ensure imports work
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Import the main function
try:
    from src.cli import main
except ImportError as e:
    print(f"Error importing CLI module: {e}")
    print(f"Current directory: {current_dir}")
    print(f"Python path: {sys.path}")
    sys.exit(1)

if __name__ == '__main__':
    main()
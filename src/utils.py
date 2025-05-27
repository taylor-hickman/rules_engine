"""
Utility functions for the NPI Suppression Rule Engine.

This module provides backward compatibility by importing from the new structure.
"""

# Import from new modules for backward compatibility
from .utils.logging_config import setup_logging, get_logger
from .utils.csv_analyzer import analyze_csv_universe
from .validation.npi import NPIValidator

__all__ = ['setup_logging', 'get_logger', 'analyze_csv_universe', 'NPIValidator']
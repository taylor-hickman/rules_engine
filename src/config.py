"""
Configuration module for backward compatibility.

This module imports from the new structure to maintain compatibility.
"""

from .core.config import DatabaseConfig, RuleConfig, ProcessingConfig, AppConfig
from .core.exceptions import ConfigurationError

# For backward compatibility, import old config classes if needed
import os
import yaml
from typing import Dict, Any

class RulesConfiguration:
    """Legacy rules configuration loader."""
    
    @staticmethod
    def load_from_yaml(yaml_path: str) -> Dict[str, Any]:
        """Load rules from YAML file."""
        with open(yaml_path, 'r') as f:
            config = yaml.safe_load(f)
        return config

    @staticmethod
    def validate_configuration(config: Dict[str, Any]) -> bool:
        """Validate configuration structure."""
        return 'rules' in config

__all__ = [
    'DatabaseConfig', 
    'RuleConfig', 
    'ProcessingConfig', 
    'AppConfig',
    'ConfigurationError',
    'RulesConfiguration'
]
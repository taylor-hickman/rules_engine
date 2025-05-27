"""Simplified configuration management using dataclasses."""

import os
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from pathlib import Path
import yaml
from dotenv import load_dotenv

from .exceptions import ConfigurationError
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    host: str
    username: str
    password: str
    port: int = 1025
    logmech: str = 'TD2'
    encryptdata: bool = True
    
    @classmethod
    def from_env(cls) -> 'DatabaseConfig':
        """Create configuration from environment variables."""
        load_dotenv()
        
        required_vars = ['DB_HOST', 'DB_USERNAME', 'DB_PASSWORD']
        missing = [var for var in required_vars if not os.getenv(var)]
        
        if missing:
            raise ConfigurationError(f"Missing required environment variables: {', '.join(missing)}")
        
        return cls(
            host=os.getenv('DB_HOST'),
            username=os.getenv('DB_USERNAME'),
            password=os.getenv('DB_PASSWORD'),
            port=int(os.getenv('DB_PORT', '1025')),
            logmech=os.getenv('DB_LOGMECH', 'TD2'),
            encryptdata=os.getenv('DB_ENCRYPTDATA', 'true').lower() == 'true'
        )
    
    def to_connection_params(self) -> Dict[str, Any]:
        """Convert to teradatasql connection parameters."""
        return {
            'host': self.host,
            'user': self.username,
            'password': self.password,
            'logmech': self.logmech,
            'encryptdata': self.encryptdata
        }


@dataclass
class RuleConfig:
    """Rule processing configuration."""
    rules: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    @classmethod
    def from_yaml(cls, path: Path) -> 'RuleConfig':
        """Load rules from YAML file."""
        try:
            with open(path, 'r') as f:
                data = yaml.safe_load(f)
            
            if not isinstance(data, dict) or 'rules' not in data:
                raise ConfigurationError("Invalid rules file format")
            
            return cls(rules=data['rules'])
        except Exception as e:
            raise ConfigurationError(f"Failed to load rules from {path}: {str(e)}")
    
    def get_rule_names(self) -> list[str]:
        """Get list of rule names."""
        return list(self.rules.keys())
    
    def get_rule(self, name: str) -> Dict[str, Any]:
        """Get a specific rule configuration."""
        if name not in self.rules:
            raise ConfigurationError(f"Rule '{name}' not found")
        return self.rules[name]


@dataclass
class ProcessingConfig:
    """Processing configuration."""
    batch_size: int = 10000
    dry_run: bool = False
    verbose: bool = False
    output_dir: Path = field(default_factory=lambda: Path('./reports'))
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.batch_size <= 0:
            raise ConfigurationError("Batch size must be positive")
        
        # Ensure output directory exists
        self.output_dir = Path(self.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class AppConfig:
    """Main application configuration."""
    database: DatabaseConfig
    rules: RuleConfig
    processing: ProcessingConfig
    
    @classmethod
    def from_args(cls, args) -> 'AppConfig':
        """Create configuration from command line arguments."""
        return cls(
            database=DatabaseConfig.from_env(),
            rules=RuleConfig.from_yaml(Path(args.rules)),
            processing=ProcessingConfig(
                batch_size=args.batch_size,
                dry_run=args.dry_run,
                verbose=args.verbose,
                output_dir=Path(args.output)
            )
        )
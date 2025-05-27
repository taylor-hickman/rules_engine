"""Rule definition and management."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any, List

from ..core.exceptions import RuleProcessingError
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


class RuleLevel(Enum):
    """Defines the level at which a rule operates."""
    NPI_LEVEL = "npi"
    SPECIALTY_LEVEL = "specialty"


@dataclass
class SuppressionRule:
    """Container for a single suppression rule definition."""
    rule_id: str
    name: str
    description: str
    sql_query: str
    level: RuleLevel
    enabled: bool = True
    
    @property
    def is_specialty_level(self) -> bool:
        """Returns True if rule operates at specialty level."""
        return self.level == RuleLevel.SPECIALTY_LEVEL
    
    @property
    def is_npi_level(self) -> bool:
        """Returns True if rule operates at NPI level."""
        return self.level == RuleLevel.NPI_LEVEL


@dataclass
class RuleExecutionResult:
    """Results from executing a single rule."""
    rule_id: str
    records_matched: int
    execution_time_seconds: float
    table_name: str
    success: bool
    error_message: Optional[str] = None


class RuleLoader:
    """Loads and validates suppression rules from configuration."""
    
    @staticmethod
    def load_rules_from_config(config: Dict[str, Any]) -> Dict[str, SuppressionRule]:
        """
        Loads suppression rules from configuration dictionary.
        
        Args:
            config: Configuration dictionary containing rules
            
        Returns:
            Dictionary mapping rule IDs to SuppressionRule objects
        """
        rules = {}
        
        for rule_id, rule_config in config.items():
            try:
                # Validate required fields
                required_fields = ['name', 'description', 'sql_query', 'level']
                missing = [f for f in required_fields if f not in rule_config]
                if missing:
                    raise RuleProcessingError(
                        f"Rule '{rule_id}' missing required fields: {missing}"
                    )
                
                # Determine rule level
                level_str = rule_config['level'].lower()
                if level_str == 'npi':
                    level = RuleLevel.NPI_LEVEL
                elif level_str == 'specialty':
                    level = RuleLevel.SPECIALTY_LEVEL
                else:
                    raise RuleProcessingError(
                        f"Rule '{rule_id}' has invalid level: {level_str}"
                    )
                
                # Create rule object
                rule = SuppressionRule(
                    rule_id=rule_id,
                    name=rule_config['name'],
                    description=rule_config['description'],
                    sql_query=rule_config['sql_query'],
                    level=level,
                    enabled=rule_config.get('enabled', True)
                )
                
                if rule.enabled:
                    rules[rule_id] = rule
                    logger.debug(f"Loaded rule: {rule_id} ({rule.name})")
                else:
                    logger.debug(f"Skipped disabled rule: {rule_id}")
                    
            except Exception as e:
                logger.error(f"Failed to load rule '{rule_id}': {str(e)}")
                raise RuleProcessingError(f"Failed to load rule '{rule_id}': {str(e)}")
        
        logger.info(f"Loaded {len(rules)} suppression rules")
        return rules
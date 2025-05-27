"""
Engine module for backward compatibility.

This module imports from the new structure to maintain compatibility.
"""

from .processing.engine import SuppressionRuleEngine, ProcessingStatistics
from .processing.rules import SuppressionRule, RuleLevel, RuleExecutionResult

__all__ = [
    'SuppressionRuleEngine',
    'ProcessingStatistics', 
    'SuppressionRule',
    'RuleLevel',
    'RuleExecutionResult'
]
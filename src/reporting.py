"""
Reporting module for backward compatibility.

This module imports from the new structure to maintain compatibility.
"""

from .reporting.generators import (
    ReportOrchestrator,
    MasterTableReportGenerator,
    RuleImpactReportGenerator,
    RuleCombinationReportGenerator,
    DatabaseImpactReportGenerator,
    SummaryReportGenerator
)
from .reporting.metrics import ProcessingMetrics, MetricsFormatter
from .reporting.base import BaseReportGenerator

__all__ = [
    'ReportOrchestrator',
    'MasterTableReportGenerator',
    'RuleImpactReportGenerator', 
    'RuleCombinationReportGenerator',
    'DatabaseImpactReportGenerator',
    'SummaryReportGenerator',
    'ProcessingMetrics',
    'MetricsFormatter',
    'BaseReportGenerator'
]
"""Consolidated metrics calculation and formatting."""

from dataclasses import dataclass
from typing import Dict, Any

from ..utils.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class ProcessingMetrics:
    """Container for all processing metrics."""
    # Universe metrics
    total_npis: int = 0
    practitioner_npis: int = 0
    facility_npis: int = 0
    ancillary_npis: int = 0
    uncategorized_npis: int = 0
    
    # Rule processing metrics
    total_rules: int = 0
    rules_executed: int = 0
    total_combinations: int = 0
    
    # Suppression metrics
    suppressed_npis: int = 0
    unsuppressed_npis: int = 0
    suppressed_combinations: int = 0
    unsuppressed_combinations: int = 0
    
    # Database impact metrics
    practitioners_to_suppress: int = 0
    practices_to_suppress: int = 0
    facilities_to_suppress: int = 0
    
    @property
    def suppression_rate(self) -> float:
        """Calculate NPI suppression rate."""
        if self.practitioner_npis == 0:
            return 0.0
        return round(self.suppressed_npis / self.practitioner_npis * 100, 2)
    
    @property
    def unsuppression_rate(self) -> float:
        """Calculate NPI unsuppression rate."""
        if self.practitioner_npis == 0:
            return 0.0
        return round(self.unsuppressed_npis / self.practitioner_npis * 100, 2)
    
    @property
    def combination_suppression_rate(self) -> float:
        """Calculate combination suppression rate."""
        if self.total_combinations == 0:
            return 0.0
        return round(self.suppressed_combinations / self.total_combinations * 100, 2)
    
    @property
    def combination_unsuppression_rate(self) -> float:
        """Calculate combination unsuppression rate."""
        if self.total_combinations == 0:
            return 0.0
        return round(self.unsuppressed_combinations / self.total_combinations * 100, 2)
    
    @property
    def non_practitioner_count(self) -> int:
        """Count of non-practitioner NPIs."""
        return self.facility_npis + self.ancillary_npis + self.uncategorized_npis
    
    @property
    def non_practitioner_percentage(self) -> float:
        """Percentage of non-practitioner NPIs."""
        if self.total_npis == 0:
            return 0.0
        return round(self.non_practitioner_count / self.total_npis * 100, 2)


class MetricsFormatter:
    """Formats metrics for display and reporting."""
    
    @staticmethod
    def format_summary_report(metrics: ProcessingMetrics) -> str:
        """Format comprehensive summary report."""
        lines = [
            "=" * 70,
            "NPI PROVIDER SUPPRESSION PROCESSING SUMMARY",
            "=" * 70,
            "",
            "Universe Processing:",
            f"  Total NPIs: {metrics.total_npis:,}",
            f"  Practitioners: {metrics.practitioner_npis:,} ({MetricsFormatter._percentage(metrics.practitioner_npis, metrics.total_npis):.1f}%)",
            f"  Non-practitioners: {metrics.non_practitioner_count:,} ({metrics.non_practitioner_percentage:.1f}%)",
            "",
            "Rule Processing:",
            f"  Rules executed: {metrics.rules_executed} of {metrics.total_rules}",
            f"  Total combinations evaluated: {metrics.total_combinations:,}",
            "",
            "Suppression Results:",
            f"  NPIs to suppress: {metrics.suppressed_npis:,} ({metrics.suppression_rate:.1f}%)",
            f"  NPIs to unsuppress: {metrics.unsuppressed_npis:,} ({metrics.unsuppression_rate:.1f}%)",
            "",
            "Database Impact:",
            f"  Practitioners to suppress: {metrics.practitioners_to_suppress:,}",
            f"  Practices to suppress: {metrics.practices_to_suppress:,}",
            f"  Facilities to suppress: {metrics.facilities_to_suppress:,}",
            "",
            "=" * 70
        ]
        
        return "\n".join(lines)
    
    @staticmethod
    def format_metrics_dict(metrics: ProcessingMetrics) -> Dict[str, Any]:
        """Convert metrics to dictionary format."""
        return {
            'universe': {
                'total_npis': metrics.total_npis,
                'practitioner_npis': metrics.practitioner_npis,
                'non_practitioner_npis': metrics.non_practitioner_count,
                'non_practitioner_percentage': metrics.non_practitioner_percentage
            },
            'processing': {
                'rules_executed': metrics.rules_executed,
                'total_rules': metrics.total_rules,
                'total_combinations': metrics.total_combinations
            },
            'suppression': {
                'suppressed_npis': metrics.suppressed_npis,
                'unsuppressed_npis': metrics.unsuppressed_npis,
                'suppression_rate': metrics.suppression_rate,
                'unsuppression_rate': metrics.unsuppression_rate
            },
            'database_impact': {
                'practitioners_to_suppress': metrics.practitioners_to_suppress,
                'practices_to_suppress': metrics.practices_to_suppress,
                'facilities_to_suppress': metrics.facilities_to_suppress
            }
        }
    
    @staticmethod
    def _percentage(count: int, total: int) -> float:
        """Calculate percentage safely."""
        return round(count / total * 100, 2) if total > 0 else 0.0
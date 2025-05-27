"""Report generation orchestration."""

from pathlib import Path
from typing import Dict, Optional

from ..reporting.generators import ReportOrchestrator
from ..processing.engine import SuppressionRuleEngine
from ..validation.universe import UniverseValidator
from ..core.exceptions import ReportGenerationError
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


class ReportGenerationOrchestrator:
    """Orchestrates comprehensive report generation."""
    
    def __init__(self, rule_engine: SuppressionRuleEngine):
        self.rule_engine = rule_engine
        self.report_orchestrator = ReportOrchestrator(rule_engine)
    
    def generate_all_reports(
        self, 
        output_directory: Path, 
        universe_validator: UniverseValidator, 
        batch_size: int
    ) -> Dict[str, Path]:
        """
        Generates comprehensive suite of analysis reports.
        
        Args:
            output_directory: Directory for report output
            universe_validator: Validator instance for universe report
            batch_size: Batch size for large dataset processing
            
        Returns:
            Dictionary mapping report types to generated file paths
        """
        logger.info("STEP 6: Generating comprehensive analysis reports")
        
        output_dir = Path(output_directory)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Generate core analysis reports
            report_paths = self.report_orchestrator.generate_all_reports(
                str(output_dir), batch_size
            )
            
            # Generate universe validation report
            universe_report_path = output_dir / 'universe_validation_report.csv'
            self.report_orchestrator.generate_universe_validation_report(
                str(universe_report_path), universe_validator
            )
            report_paths['universe_validation'] = str(universe_report_path)
            
            # Convert to Path objects
            path_dict = {
                report_type: Path(file_path) 
                for report_type, file_path in report_paths.items()
            }
            
            self._log_report_summary(path_dict)
            return path_dict
            
        except Exception as e:
            logger.error(f"Report generation failed: {str(e)}")
            raise ReportGenerationError(f"Failed to generate reports: {str(e)}")
    
    def _log_report_summary(self, report_paths: Dict[str, Path]) -> None:
        """Logs summary of generated reports with file sizes."""
        logger.info("Report generation completed successfully")
        
        for report_type, file_path in report_paths.items():
            if file_path.exists():
                file_size = file_path.stat().st_size
                logger.info(
                    f"  {report_type}: {file_path.name} ({file_size:,} bytes)"
                )
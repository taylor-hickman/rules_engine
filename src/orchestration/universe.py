"""Universe processing orchestration."""

from pathlib import Path
from typing import Optional, Any
import argparse

from ..core.exceptions import UniverseValidationError
from ..validation.universe import UniverseValidator
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


class UniverseProcessingOrchestrator:
    """Orchestrates universe loading and provider type categorization."""
    
    def __init__(self, connection_manager):
        self.connection_manager = connection_manager
        self.universe_validator: Optional[UniverseValidator] = None
        self.validation_results: Optional[Any] = None
    
    def process_universe_data(self, args: argparse.Namespace) -> Optional[Any]:
        """
        Processes universe data from specified source.
        
        Args:
            args: Command line arguments containing universe source specification
            
        Returns:
            Universe validation results or None if processing failed
        """
        logger.info("STEP 1: Universe loading and provider type categorization")
        
        try:
            self.universe_validator = UniverseValidator(self.connection_manager)
            
            if args.csv_universe:
                logger.info(f"Processing CSV universe: {args.csv_universe}")
                self.validation_results = self.universe_validator.validate_csv_universe(
                    args.csv_universe, args.csv_npi_column
                )
            else:
                logger.info(f"Processing Teradata universe: {args.teradata_universe}")
                self.validation_results = self.universe_validator.validate_teradata_universe(
                    args.teradata_universe
                )
            
            self._log_universe_summary()
            return self.validation_results
            
        except Exception as e:
            logger.error(f"Universe processing failed: {str(e)}")
            raise UniverseValidationError(f"Failed to process universe: {str(e)}")
    
    def create_practitioner_universe(self) -> Optional[str]:
        """
        Creates filtered practitioner universe table.
        
        Returns:
            Table name for practitioner universe or None if creation failed
        """
        if not self.validation_results or not self.universe_validator:
            raise UniverseValidationError("No validation results available")
        
        if not self.validation_results.practitioner_npis:
            raise UniverseValidationError("No practitioner NPIs found in universe")
        
        try:
            logger.info("Creating practitioner-only universe for rule processing")
            table_name = self.universe_validator.create_practitioner_universe_table(
                self.validation_results
            )
            logger.info(f"Practitioner universe table created: {table_name}")
            return table_name
            
        except Exception as e:
            logger.error(f"Failed to create practitioner universe: {str(e)}")
            raise UniverseValidationError(f"Failed to create practitioner universe: {str(e)}")
    
    def generate_universe_report(self, output_dir: Path) -> Optional[Path]:
        """
        Generates universe validation report.
        
        Args:
            output_dir: Directory for output reports
            
        Returns:
            Path to generated report or None if generation failed
        """
        if not self.validation_results or not self.universe_validator:
            logger.warning("No validation results available for report generation")
            return None
        
        try:
            report_path = output_dir / 'universe_validation_report.csv'
            self.universe_validator.generate_universe_report(
                str(report_path), self.validation_results
            )
            return report_path
            
        except Exception as e:
            logger.error(f"Failed to generate universe report: {str(e)}")
            return None
    
    def cleanup(self) -> None:
        """Cleanup universe processing resources."""
        if self.universe_validator:
            try:
                self.universe_validator.cleanup()
                logger.debug("Universe processing resources cleaned up")
            except Exception as e:
                logger.warning(f"Error during universe cleanup: {str(e)}")
    
    def _log_universe_summary(self) -> None:
        """Logs summary of universe processing results."""
        if not self.validation_results:
            return
        
        counts = self.validation_results.provider_type_counts
        logger.info("=" * 70)
        logger.info("UNIVERSE PROCESSING SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Total NPIs processed: {counts.total:,}")
        logger.info("Provider Type Distribution:")
        logger.info(f"  Practitioners: {counts.practitioners:,} ({counts.practitioner_percentage:.1f}%) -> Rule processing")
        logger.info(f"  Facilities: {counts.facilities:,} ({counts.facility_percentage:.1f}%) -> Provider type suppression")
        logger.info(f"  Ancillary: {counts.ancillary:,} ({counts.ancillary_percentage:.1f}%) -> Provider type suppression")
        logger.info(f"  Uncategorized: {counts.uncategorized:,} ({counts.uncategorized_percentage:.1f}%) -> Provider type suppression")
        logger.info("Processing Impact:")
        logger.info(f"  NPIs entering rule pipeline: {counts.practitioners:,} ({counts.practitioner_percentage:.1f}%)")
        logger.info(f"  NPIs suppressed by provider type: {counts.non_practitioner_count:,} ({counts.non_practitioner_percentage:.1f}%)")
        logger.info("=" * 70)
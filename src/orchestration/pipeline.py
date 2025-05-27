"""Main processing pipeline orchestration."""

from pathlib import Path
from typing import Optional, Dict, Any
import argparse

from .universe import UniverseProcessingOrchestrator
from .rules import RuleProcessingOrchestrator
from .reports import ReportGenerationOrchestrator
from ..core.config import AppConfig
from ..core.connections import ConnectionManager
from ..connection_manager import SharedConnectionManager  # Keep for compatibility
from ..utils.logging_config import get_logger, setup_logging

logger = get_logger(__name__)


class ProcessingPipeline:
    """Main orchestrator for the entire processing pipeline."""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.connection_manager: Optional[SharedConnectionManager] = None
        self.universe_orchestrator: Optional[UniverseProcessingOrchestrator] = None
        self.rule_orchestrator: Optional[RuleProcessingOrchestrator] = None
        self.report_orchestrator: Optional[ReportGenerationOrchestrator] = None
    
    def initialize(self) -> bool:
        """Initialize all components."""
        try:
            # Setup logging
            setup_logging(
                level='DEBUG' if self.config.processing.verbose else 'INFO',
                log_file=str(self.config.processing.output_dir / 'processing.log')
            )
            
            # Initialize connection manager
            logger.info("Initializing shared database connection manager")
            self.connection_manager = SharedConnectionManager(
                self.config.database.to_connection_params()
            )
            self.connection_manager.initialize()
            
            # Initialize orchestrators
            self.universe_orchestrator = UniverseProcessingOrchestrator(
                self.connection_manager
            )
            self.rule_orchestrator = RuleProcessingOrchestrator(
                self.connection_manager, 
                self.config.processing.batch_size
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize pipeline: {str(e)}")
            return False
    
    def execute(self, args: argparse.Namespace) -> bool:
        """
        Execute the complete processing pipeline.
        
        Args:
            args: Command line arguments
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Step 1: Process universe data
            universe_results = self.universe_orchestrator.process_universe_data(args)
            if not universe_results:
                return False
            
            # Generate universe report
            self.universe_orchestrator.generate_universe_report(
                self.config.processing.output_dir
            )
            
            # Step 2: Create practitioner universe
            practitioner_table = self.universe_orchestrator.create_practitioner_universe()
            if not practitioner_table:
                return False
            
            # Step 3-5: Execute suppression rules
            master_table = self.rule_orchestrator.execute_suppression_rules(
                self.config.rules.rules,
                universe_results,
                practitioner_table
            )
            if not master_table:
                return False
            
            # Step 6: Generate reports
            if self.rule_orchestrator.rule_engine:
                self.report_orchestrator = ReportGenerationOrchestrator(
                    self.rule_orchestrator.rule_engine
                )
                
                report_paths = self.report_orchestrator.generate_all_reports(
                    self.config.processing.output_dir,
                    self.universe_orchestrator.universe_validator,
                    self.config.processing.batch_size
                )
                
                # Log final statistics
                self._log_final_statistics()
            
            return True
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            return False
        
        finally:
            self.cleanup()
    
    def cleanup(self) -> None:
        """Clean up all resources."""
        logger.info("Cleaning up processing resources")
        
        if self.universe_orchestrator:
            self.universe_orchestrator.cleanup()
        
        if self.rule_orchestrator:
            self.rule_orchestrator.cleanup()
        
        if self.connection_manager:
            self.connection_manager.cleanup()
    
    def _log_final_statistics(self) -> None:
        """Log final processing statistics."""
        stats = self.rule_orchestrator.get_processing_statistics()
        if not stats:
            return
        
        logger.info("=" * 70)
        logger.info("FINAL PROCESSING STATISTICS")
        logger.info("=" * 70)
        logger.info(f"Total NPIs processed: {stats.get('total_npis', 0):,}")
        logger.info(f"NPIs suppressed: {stats.get('suppressed_npis', 0):,}")
        logger.info(f"NPIs unsuppressed: {stats.get('unsuppressed_npis', 0):,}")
        logger.info(f"Processing time: {stats.get('processing_time', 'N/A')}")
        logger.info("=" * 70)
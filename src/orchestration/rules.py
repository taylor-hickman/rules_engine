"""Rule processing orchestration."""

from typing import Optional, Dict, Any

from ..processing.engine import SuppressionRuleEngine
from ..core.exceptions import RuleProcessingError
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


class RuleProcessingOrchestrator:
    """Orchestrates suppression rule loading and execution."""
    
    def __init__(self, connection_manager, batch_size: int):
        self.connection_manager = connection_manager
        self.batch_size = batch_size
        self.rule_engine: Optional[SuppressionRuleEngine] = None
    
    def execute_suppression_rules(
        self, 
        config: Dict[str, Any], 
        universe_results: Any, 
        practitioner_table: str
    ) -> Optional[str]:
        """
        Executes all suppression rules against practitioner universe.
        
        Args:
            config: Rules configuration dictionary
            universe_results: Results from universe validation
            practitioner_table: Name of practitioner universe table
            
        Returns:
            Master results table name or None if execution failed
        """
        try:
            logger.info("STEP 3: Initializing suppression rule engine")
            self.rule_engine = SuppressionRuleEngine(
                self.connection_manager, 
                self.batch_size
            )
            
            logger.info("STEP 4: Loading suppression rules from configuration")
            self.rule_engine.load_rules_from_configuration(config)
            
            logger.info("STEP 5: Executing suppression rules with unsuppression tracking")
            master_table = self.rule_engine.execute_all_rules(
                config, universe_results, practitioner_table
            )
            
            logger.info(f"Rule execution completed successfully: {master_table}")
            return master_table
            
        except Exception as e:
            logger.error(f"Rule processing failed: {str(e)}")
            raise RuleProcessingError(f"Failed to process rules: {str(e)}")
    
    def get_processing_statistics(self) -> Optional[Dict[str, Any]]:
        """Get processing statistics from the rule engine."""
        if not self.rule_engine:
            return None
        
        try:
            return self.rule_engine.get_processing_statistics()
        except Exception as e:
            logger.warning(f"Failed to get processing statistics: {str(e)}")
            return None
    
    def cleanup(self) -> None:
        """Cleans up rule processing resources."""
        if self.rule_engine:
            try:
                self.rule_engine.cleanup()
                logger.debug("Rule processing resources cleaned up")
            except Exception as e:
                logger.warning(f"Error during rule cleanup: {str(e)}")
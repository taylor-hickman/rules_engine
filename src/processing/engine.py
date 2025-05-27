"""
NPI Suppression Rule Engine

Executes provider suppression rules against practitioner NPIs and tracks both 
suppression and unsuppression outcomes.
"""

import time
import uuid
from typing import Dict, Any, Optional
from dataclasses import dataclass
import teradatasql

from .rules import SuppressionRule, RuleExecutionResult, RuleLoader
from .tables import TableManager
from ..core.exceptions import RuleProcessingError
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class ProcessingStatistics:
    """Statistics from master results processing."""
    total_combinations: int
    suppressed_combinations: int
    unsuppressed_combinations: int
    unique_npis: int
    suppressed_npis: int
    unsuppressed_npis: int
    
    @property
    def combination_suppression_rate(self) -> float:
        """Percentage of combinations suppressed."""
        return (self.suppressed_combinations / self.total_combinations * 100) if self.total_combinations > 0 else 0.0
    
    @property
    def combination_unsuppression_rate(self) -> float:
        """Percentage of combinations that remain unsuppressed."""
        return (self.unsuppressed_combinations / self.total_combinations * 100) if self.total_combinations > 0 else 0.0
    
    @property
    def npi_suppression_rate(self) -> float:
        """Percentage of unique NPIs suppressed."""
        return (self.suppressed_npis / self.unique_npis * 100) if self.unique_npis > 0 else 0.0
    
    @property
    def npi_unsuppression_rate(self) -> float:
        """Percentage of unique NPIs that remain unsuppressed."""
        return (self.unsuppressed_npis / self.unique_npis * 100) if self.unique_npis > 0 else 0.0


class SuppressionRuleEngine:
    """
    Main engine for executing NPI suppression rules.
    
    Manages rule execution, result aggregation, and comprehensive tracking
    of both suppression and unsuppression outcomes.
    """
    
    def __init__(self, connection: teradatasql.TeradataConnection, batch_size: int = 10000):
        """
        Initialize the suppression rule engine.
        
        Args:
            connection: Persistent database connection
            batch_size: Batch size for large dataset processing
        """
        self.connection = connection
        self.batch_size = batch_size
        self.session_id = str(uuid.uuid4()).replace('-', '')[:8]
        
        # Component initialization
        self.table_manager = TableManager(self.connection)
        
        # Rule management
        self.rules: Dict[str, SuppressionRule] = {}
        self.rule_execution_results: Dict[str, RuleExecutionResult] = {}
        
        # Result tracking
        self.practitioner_universe_table: Optional[str] = None
        self.base_combinations_table: Optional[str] = None
        self.master_results_table: Optional[str] = None
        self.processing_statistics: Optional[ProcessingStatistics] = None
        
        # Database impact tracking
        self.database_impact: Dict[str, Any] = {}
    
    def load_rules_from_configuration(self, config: Dict[str, Any]) -> None:
        """
        Loads suppression rules from configuration dictionary.
        
        Args:
            config: Configuration dictionary containing rules
        """
        self.rules = RuleLoader.load_rules_from_config(config)
        logger.info(f"Loaded {len(self.rules)} suppression rules")
    
    def execute_all_rules(
        self, 
        config: Dict[str, Any],
        universe_results: Any,
        practitioner_universe_table: str
    ) -> str:
        """
        Executes all loaded suppression rules against practitioner universe.
        
        Args:
            config: Rules configuration (unused but kept for compatibility)
            universe_results: Universe validation results (unused but kept for compatibility)
            practitioner_universe_table: Table containing practitioner NPIs
            
        Returns:
            Name of master results table containing all outcomes
        """
        try:
            # Store practitioner universe table for rule execution
            self.practitioner_universe_table = practitioner_universe_table
            
            # Create base combinations table
            logger.info("Creating base NPI-specialty combinations")
            self.base_combinations_table = self._create_base_combinations_table(
                practitioner_universe_table
            )
            
            # Execute each rule
            logger.info(f"Executing {len(self.rules)} suppression rules")
            for rule_id, rule in self.rules.items():
                self._execute_single_rule(rule)
            
            # Create master results table
            logger.info("Creating master results table with comprehensive outcomes")
            self.master_results_table = self._create_master_results_table()
            
            # Calculate processing statistics
            self._calculate_processing_statistics()
            
            # Calculate database impact
            self._calculate_database_impact()
            
            logger.info("Rule execution completed successfully")
            return self.master_results_table
            
        except Exception as e:
            logger.error(f"Rule execution failed: {str(e)}")
            raise RuleProcessingError(f"Failed to execute rules: {str(e)}")
    
    def get_processing_statistics(self) -> Optional[Dict[str, Any]]:
        """Returns processing statistics as a dictionary."""
        if not self.processing_statistics:
            return None
        
        return {
            'total_npis': self.processing_statistics.unique_npis,
            'suppressed_npis': self.processing_statistics.suppressed_npis,
            'unsuppressed_npis': self.processing_statistics.unsuppressed_npis,
            'total_combinations': self.processing_statistics.total_combinations,
            'suppressed_combinations': self.processing_statistics.suppressed_combinations,
            'unsuppressed_combinations': self.processing_statistics.unsuppressed_combinations,
            'suppression_rate': self.processing_statistics.npi_suppression_rate,
            'unsuppression_rate': self.processing_statistics.npi_unsuppression_rate
        }
    
    def cleanup(self) -> None:
        """Cleans up all resources and tables."""
        logger.info("Cleaning up rule engine resources")
        
        if self.table_manager:
            self.table_manager.cleanup_all_tables()
    
    def _create_base_combinations_table(self, practitioner_universe_table: str) -> str:
        """Creates base table with all NPI-specialty combinations."""
        columns = [
            {'name': 'npi', 'type': 'VARCHAR(10)'},
            {'name': 'specialty_name', 'type': 'VARCHAR(200)'},
            {'name': 'concat_key', 'type': 'VARCHAR(250)'}
        ]
        
        table_name = self.table_manager.create_volatile_table(
            'base_combinations', columns, primary_index='npi,specialty_name'
        )
        
        # Populate with NPI-specialty combinations from practitioner data
        insert_sql = f"""
        INSERT INTO {table_name} (npi, specialty_name, concat_key)
        SELECT DISTINCT 
            CAST(A.NPI AS VARCHAR(10)) as npi,
            CAST(SP.SpecialtyName AS VARCHAR(200)) as specialty_name,
            TRIM(CAST(A.NPI AS VARCHAR(10)) || '-' || CAST(SP.SpecialtyName AS VARCHAR(200))) as concat_key
        FROM PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_practitioners p
        JOIN {practitioner_universe_table} A ON A.npi = p.NationalProviderID
        JOIN PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_PRACTITIONERSPECIALTIES PRSP ON p.PractitionerID = PRSP.PractitionerID
        JOIN PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_PRACTITIONEREDUCATION PE ON PRSP.PractitionerID = PE.PractitionerID
        JOIN PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_PRACTITIONERPRODUCTS PRPROD ON PRSP.PRACTITIONERID = PRPROD.PRACTITIONERID
        JOIN PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_PRACTITIONERPRODUCTSPECIALTIES PRPRODSP ON PRPRODSP.PRACTITIONERPRODUCTRECID = PRPROD.PRACTITIONERPRODUCTRECID
        JOIN PROVIDERDATASERVICE_CORE_V.PROV_SPAYER_SPECIALTIES SP ON PRSP.SPECIALTYID = SP.SPECIALTYID
        WHERE A.NPI IS NOT NULL AND SP.SpecialtyName IS NOT NULL
        """
        
        cursor = self.connection.cursor()
        cursor.execute(insert_sql)
        
        # Get count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        logger.info(f"Created base combinations table with {count:,} NPI-specialty pairs")
        return table_name
    
    def _execute_single_rule(self, rule: SuppressionRule) -> None:
        """Executes a single suppression rule."""
        logger.info(f"Executing rule: {rule.rule_id} - {rule.name}")
        start_time = time.time()
        
        try:
            # Create rule result table based on rule level
            if rule.is_specialty_level:
                columns = [
                    {'name': 'npi', 'type': 'VARCHAR(10)'},
                    {'name': 'specialty_name', 'type': 'VARCHAR(200)'},
                    {'name': 'concat_key', 'type': 'VARCHAR(250)'}
                ]
            else:
                columns = [
                    {'name': 'npi', 'type': 'VARCHAR(10)'}
                ]
            
            result_table = self.table_manager.create_volatile_table(
                f"rule_{rule.rule_id}", columns
            )
            
            # Format rule query by replacing template variables
            formatted_query = rule.sql_query.replace('{npi_universe_table}', self.practitioner_universe_table)
            formatted_query = formatted_query.replace('{base_table}', self.base_combinations_table)
            
            # Build appropriate INSERT statement based on rule level
            cursor = self.connection.cursor()
            if rule.is_specialty_level:
                # Specialty-level rules return npi, specialty_name - we need to add concat_key
                insert_sql = f"""
                INSERT INTO {result_table} (npi, specialty_name, concat_key)
                SELECT DISTINCT 
                    CAST(rule_results.npi AS VARCHAR(10)) as npi,
                    CAST(rule_results.specialty_name AS VARCHAR(200)) as specialty_name,
                    TRIM(CAST(rule_results.npi AS VARCHAR(10)) || '-' || CAST(rule_results.specialty_name AS VARCHAR(200))) as concat_key
                FROM ({formatted_query}) AS rule_results
                WHERE rule_results.npi IS NOT NULL AND rule_results.specialty_name IS NOT NULL
                """
            else:
                # NPI-level rules return just npi
                insert_sql = f"""
                INSERT INTO {result_table} (npi)
                {formatted_query}
                """
            cursor.execute(insert_sql)
            
            # Get matched count
            cursor.execute(f"SELECT COUNT(*) FROM {result_table}")
            matched_count = cursor.fetchone()[0]
            
            execution_time = time.time() - start_time
            
            # Store execution result
            self.rule_execution_results[rule.rule_id] = RuleExecutionResult(
                rule_id=rule.rule_id,
                records_matched=matched_count,
                execution_time_seconds=execution_time,
                table_name=result_table,
                success=True
            )
            
            logger.info(
                f"Rule {rule.rule_id} completed: {matched_count:,} records matched "
                f"in {execution_time:.2f} seconds"
            )
            
        except Exception as e:
            logger.error(f"Rule {rule.rule_id} failed: {str(e)}")
            self.rule_execution_results[rule.rule_id] = RuleExecutionResult(
                rule_id=rule.rule_id,
                records_matched=0,
                execution_time_seconds=time.time() - start_time,
                table_name='',
                success=False,
                error_message=str(e)
            )
    
    def _create_master_results_table(self) -> str:
        """Creates master results table combining all rule outcomes."""
        # Build column list for master table
        base_columns = [
            {'name': 'npi', 'type': 'VARCHAR(10)'},
            {'name': 'specialty_name', 'type': 'VARCHAR(200)'},
            {'name': 'concat_key', 'type': 'VARCHAR(250)'},
            {'name': 'rule_combination_key', 'type': 'VARCHAR(1000)'},
            {'name': 'suppression_flag', 'type': 'CHAR(1)'},
            {'name': 'unsuppression_flag', 'type': 'CHAR(1)'}
        ]
        
        # Add columns for each successful rule only
        successful_rule_ids = []
        for rule_id, rule in self.rules.items():
            result = self.rule_execution_results.get(rule_id)
            if result and result.success:
                successful_rule_ids.append(rule_id)
                base_columns.append({
                    'name': f"rule_{rule_id}_flag",
                    'type': 'CHAR(1)'
                })
        
        master_table = self.table_manager.create_volatile_table(
            'master_results', base_columns, primary_index='npi,specialty_name'
        )
        
        # Build dynamic SQL to populate master table - only include successful rules
        rule_case_statements = []
        rule_joins = []
        successful_rules = []
        failed_rules = []
        
        for rule_id, rule in self.rules.items():
            result = self.rule_execution_results.get(rule_id)
            if result and result.success:
                successful_rules.append(rule_id)
                rule_alias = f"r_{rule_id}"
                rule_case_statements.append(
                    f"CASE WHEN {rule_alias}.npi IS NOT NULL THEN 'Y' ELSE 'N' END AS rule_{rule_id}_flag"
                )
                
                if rule.is_specialty_level:
                    rule_joins.append(
                        f"LEFT JOIN {result.table_name} {rule_alias} "
                        f"ON b.concat_key = {rule_alias}.concat_key"
                    )
                else:
                    rule_joins.append(
                        f"LEFT JOIN {result.table_name} {rule_alias} "
                        f"ON b.npi = {rule_alias}.npi"
                    )
            else:
                failed_rules.append(rule_id)
        
        logger.info(f"Master results will include {len(successful_rules)} successful rules: {successful_rules}")
        if failed_rules:
            logger.warning(f"Excluding {len(failed_rules)} failed rules: {failed_rules}")
        
        # Generate rule combination key - only for successful rules
        rule_flags = [f"rule_{rule_id}_flag" for rule_id in sorted(successful_rules)]
        combination_key = " || '-' || ".join(rule_flags) if rule_flags else "'no_rules'"
        
        # Build suppression logic
        if rule_flags:
            suppression_conditions = ' OR '.join([f"{flag} = 'Y'" for flag in rule_flags])
            unsuppression_conditions = ' AND '.join([f"{flag} = 'N'" for flag in rule_flags])
            suppression_logic = f"CASE WHEN {suppression_conditions} THEN 'Y' ELSE 'N' END"
            unsuppression_logic = f"CASE WHEN {unsuppression_conditions} THEN 'Y' ELSE 'N' END"
            rule_columns = ', ' + ', '.join(rule_case_statements)
        else:
            suppression_logic = "'N'"
            unsuppression_logic = "'Y'"
            rule_columns = ""
        
        # Build and execute insert
        insert_sql = f"""
        INSERT INTO {master_table}
        SELECT 
            b.npi,
            b.specialty_name,
            b.concat_key,
            {combination_key} as rule_combination_key,
            {suppression_logic} as suppression_flag,
            {unsuppression_logic} as unsuppression_flag{rule_columns}
        FROM {self.base_combinations_table} b
        {' '.join(rule_joins)}
        """
        
        cursor = self.connection.cursor()
        cursor.execute(insert_sql)
        
        # Get record count
        cursor.execute(f"SELECT COUNT(*) FROM {master_table}")
        count = cursor.fetchone()[0]
        
        logger.info(f"Master results table created with {count:,} records")
        return master_table
    
    def _calculate_processing_statistics(self) -> None:
        """Calculates comprehensive processing statistics."""
        cursor = self.connection.cursor()
        
        # Get combination counts
        cursor.execute(f"""
        SELECT 
            COUNT(*) as total_combinations,
            SUM(CASE WHEN suppression_flag = 'Y' THEN 1 ELSE 0 END) as suppressed,
            SUM(CASE WHEN suppression_flag = 'N' THEN 1 ELSE 0 END) as unsuppressed
        FROM {self.master_results_table}
        """)
        
        combo_stats = cursor.fetchone()
        
        # Get unique NPI counts
        cursor.execute(f"""
        SELECT 
            COUNT(DISTINCT npi) as unique_npis,
            COUNT(DISTINCT CASE WHEN suppression_flag = 'Y' THEN npi END) as suppressed_npis,
            COUNT(DISTINCT CASE WHEN suppression_flag = 'N' THEN npi END) as unsuppressed_npis
        FROM {self.master_results_table}
        """)
        
        npi_stats = cursor.fetchone()
        
        self.processing_statistics = ProcessingStatistics(
            total_combinations=combo_stats[0],
            suppressed_combinations=combo_stats[1],
            unsuppressed_combinations=combo_stats[2],
            unique_npis=npi_stats[0],
            suppressed_npis=npi_stats[1],
            unsuppressed_npis=npi_stats[2]
        )
        
        logger.info(f"Processing statistics calculated:")
        logger.info(f"  Combinations - Total: {self.processing_statistics.total_combinations:,}, "
                   f"Suppressed: {self.processing_statistics.suppressed_combinations:,} "
                   f"({self.processing_statistics.combination_suppression_rate:.1f}%)")
        logger.info(f"  NPIs - Total: {self.processing_statistics.unique_npis:,}, "
                   f"Suppressed: {self.processing_statistics.suppressed_npis:,} "
                   f"({self.processing_statistics.npi_suppression_rate:.1f}%)")
    
    def _calculate_database_impact(self) -> None:
        """Calculates the impact on Spayer database tables."""
        cursor = self.connection.cursor()
        
        # NPIs to suppress
        cursor.execute(f"""
        SELECT COUNT(DISTINCT npi)
        FROM {self.master_results_table}
        WHERE suppression_flag = 'Y'
        """)
        npis_count = cursor.fetchone()[0]
        
        # NPI-specialty combinations to suppress
        cursor.execute(f"""
        SELECT COUNT(*)
        FROM {self.master_results_table}
        WHERE suppression_flag = 'Y'
        """)
        combinations_count = cursor.fetchone()[0]
        
        self.database_impact = {
            'npis_to_suppress': npis_count,
            'combinations_to_suppress': combinations_count
        }
        
        logger.info("Database impact calculated:")
        logger.info(f"  NPIs to suppress: {npis_count:,}")
        logger.info(f"  NPI-specialty combinations to suppress: {combinations_count:,}")
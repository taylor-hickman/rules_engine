"""
Report generation modules for NPI suppression analysis.
"""

from datetime import datetime
from typing import Dict
from pathlib import Path

from .base import BaseReportGenerator
from .metrics import ProcessingMetrics, MetricsFormatter
from ..core.constants import REPORT_FILES
from ..core.exceptions import ReportGenerationError
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


class MasterTableReportGenerator(BaseReportGenerator):
    """Generates comprehensive master table reports with all rule results."""
    
    def __init__(self, rule_engine, output_dir: Path):
        super().__init__(output_dir)
        self.rule_engine = rule_engine
        self.connection = rule_engine.connection
        self.cursor = rule_engine.connection.cursor()
    
    def generate(self) -> Path:
        """Generate master suppression results report."""
        logger.info("Generating master suppression results report")
        
        # Get column names dynamically
        column_query = f"SELECT * FROM {self.rule_engine.master_results_table} SAMPLE 1"
        self.cursor.execute(column_query)
        columns = [desc[0] for desc in self.cursor.description]
        
        # Generate data rows
        def data_generator():
            query = f"SELECT * FROM {self.rule_engine.master_results_table} ORDER BY npi, specialty_name"
            self.cursor.execute(query)
            
            while True:
                rows = self.cursor.fetchmany(self.rule_engine.batch_size)
                if not rows:
                    break
                for row in rows:
                    yield row
        
        return self.write_csv_report(
            REPORT_FILES['master'],
            columns,
            data_generator()
        )


class RuleImpactReportGenerator(BaseReportGenerator):
    """Generates rule-level impact analysis reports."""
    
    def __init__(self, rule_engine, output_dir: Path):
        super().__init__(output_dir)
        self.rule_engine = rule_engine
        self.connection = rule_engine.connection
        self.cursor = rule_engine.connection.cursor()
    
    def generate(self) -> Path:
        """Generate rule impact analysis report."""
        logger.info("Generating rule impact analysis report")
        
        headers = [
            'rule_id', 'rule_name', 'rule_level', 'npis_matched',
            'combinations_matched', 'execution_time_seconds', 'status'
        ]
        
        def data_generator():
            for rule_id, rule in self.rule_engine.rules.items():
                result = self.rule_engine.rule_execution_results.get(rule_id)
                
                if result:
                    # Get NPI count for this rule
                    npi_query = f"""
                    SELECT COUNT(DISTINCT npi) 
                    FROM {self.rule_engine.master_results_table}
                    WHERE rule_{rule_id}_flag = 'Y'
                    """
                    self.cursor.execute(npi_query)
                    npi_count = self.cursor.fetchone()[0]
                    
                    yield [
                        rule_id,
                        rule.name,
                        rule.level.value,
                        npi_count,
                        result.records_matched,
                        round(result.execution_time_seconds, 2),
                        'Success' if result.success else f'Failed: {result.error_message}'
                    ]
        
        return self.write_csv_report(
            REPORT_FILES['rule'],
            headers,
            data_generator()
        )


class RuleCombinationReportGenerator(BaseReportGenerator):
    """Generates rule combination analysis reports."""
    
    def __init__(self, rule_engine, output_dir: Path):
        super().__init__(output_dir)
        self.rule_engine = rule_engine
        self.connection = rule_engine.connection
        self.cursor = rule_engine.connection.cursor()
    
    def generate(self) -> Path:
        """Generate rule combination analysis report."""
        logger.info("Generating rule combination analysis report")
        
        # Get unique combinations
        query = f"""
        SELECT 
            rule_combination_key,
            COUNT(*) as combination_count,
            COUNT(DISTINCT npi) as unique_npis,
            suppression_flag
        FROM {self.rule_engine.master_results_table}
        GROUP BY rule_combination_key, suppression_flag
        ORDER BY combination_count DESC
        """
        
        headers = ['rule_combination', 'total_occurrences', 'unique_npis', 'suppression_flag']
        
        def data_generator():
            self.cursor.execute(query)
            while True:
                rows = self.cursor.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    yield row
        
        return self.write_csv_report(
            REPORT_FILES['combination'],
            headers,
            data_generator()
        )


class DatabaseImpactReportGenerator(BaseReportGenerator):
    """Generates database impact analysis reports."""
    
    def __init__(self, rule_engine, output_dir: Path):
        super().__init__(output_dir)
        self.rule_engine = rule_engine
        self.connection = rule_engine.connection
        self.cursor = rule_engine.connection.cursor()
    
    def generate(self) -> Path:
        """Generate database impact report."""
        logger.info("Generating database impact report")
        
        headers = ['entity_type', 'entity_id', 'entity_name', 'suppression_action']
        
        def data_generator():
            # Practitioners to suppress (simplified without prov_prac_xref_sk)
            query = f"""
            SELECT DISTINCT 
                'Practitioner' as entity_type,
                m.npi as entity_id,
                'NPI_' || m.npi as entity_name,
                'Suppress' as action
            FROM {self.rule_engine.master_results_table} m
            WHERE m.suppression_flag = 'Y'
            """
            
            self.cursor.execute(query)
            while True:
                rows = self.cursor.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    yield row
        
        return self.write_csv_report(
            REPORT_FILES['database'],
            headers,
            data_generator()
        )


class SummaryReportGenerator(BaseReportGenerator):
    """Generates executive summary reports."""
    
    def __init__(self, rule_engine, processing_metrics: ProcessingMetrics, output_dir: Path):
        super().__init__(output_dir)
        self.rule_engine = rule_engine
        self.processing_metrics = processing_metrics
    
    def generate(self) -> Path:
        """Generate summary text report."""
        logger.info("Generating summary report")
        
        # Gather execution details
        execution_summary = self._generate_execution_summary()
        
        # Format complete report
        report_content = f"""
NPI Provider Suppression Processing Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'=' * 70}

{MetricsFormatter.format_summary_report(self.processing_metrics)}

RULE EXECUTION DETAILS
{'=' * 70}
{execution_summary}

PROCESSING DETAILS
{'=' * 70}
Session ID: {self.rule_engine.session_id}
Batch Size: {self.rule_engine.batch_size:,}
Total Rules Loaded: {len(self.rule_engine.rules)}
Rules Successfully Executed: {sum(1 for r in self.rule_engine.rule_execution_results.values() if r.success)}

GENERATED REPORTS
{'=' * 70}
- Master Suppression Results: {REPORT_FILES['master']}
- Rule Impact Analysis: {REPORT_FILES['rule']}
- Rule Combination Analysis: {REPORT_FILES['combination']}
- Database Impact Report: {REPORT_FILES['database']}
- Universe Validation Report: {REPORT_FILES['universe']}

{'=' * 70}
End of Report
"""
        
        return self.write_text_report(REPORT_FILES['summary'], report_content)
    
    def _generate_execution_summary(self) -> str:
        """Generate rule execution summary."""
        lines = []
        
        for rule_id, rule in self.rule_engine.rules.items():
            result = self.rule_engine.rule_execution_results.get(rule_id)
            if result:
                status = "✓" if result.success else "✗"
                lines.append(
                    f"{status} {rule_id}: {rule.name} - "
                    f"{result.records_matched:,} matches in {result.execution_time_seconds:.2f}s"
                )
        
        return "\n".join(lines)


class ReportOrchestrator:
    """Orchestrates generation of all reports."""
    
    def __init__(self, rule_engine):
        self.rule_engine = rule_engine
    
    def generate_all_reports(self, output_directory: str, batch_size: int) -> Dict[str, str]:
        """
        Generate all analysis reports.
        
        Args:
            output_directory: Directory for output files
            batch_size: Batch size for processing
            
        Returns:
            Dictionary mapping report types to file paths
        """
        output_dir = Path(output_directory)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        report_paths = {}
        
        try:
            # Generate each report type
            generators = [
                ('master', MasterTableReportGenerator(self.rule_engine, output_dir)),
                ('rule_impact', RuleImpactReportGenerator(self.rule_engine, output_dir)),
                ('combination', RuleCombinationReportGenerator(self.rule_engine, output_dir)),
                ('database_impact', DatabaseImpactReportGenerator(self.rule_engine, output_dir))
            ]
            
            for report_type, generator in generators:
                try:
                    path = generator.generate()
                    report_paths[report_type] = str(path)
                except Exception as e:
                    logger.error(f"Failed to generate {report_type} report: {str(e)}")
            
            # Generate summary report
            if self.rule_engine.processing_statistics:
                # Convert to ProcessingMetrics
                metrics = ProcessingMetrics(
                    total_npis=self.rule_engine.processing_statistics.unique_npis,
                    practitioner_npis=self.rule_engine.processing_statistics.unique_npis,
                    suppressed_npis=self.rule_engine.processing_statistics.suppressed_npis,
                    unsuppressed_npis=self.rule_engine.processing_statistics.unsuppressed_npis,
                    total_combinations=self.rule_engine.processing_statistics.total_combinations,
                    suppressed_combinations=self.rule_engine.processing_statistics.suppressed_combinations,
                    unsuppressed_combinations=self.rule_engine.processing_statistics.unsuppressed_combinations,
                    practitioners_to_suppress=self.rule_engine.database_impact.get('practitioners_to_suppress', 0),
                    practices_to_suppress=self.rule_engine.database_impact.get('practices_to_suppress', 0),
                    facilities_to_suppress=self.rule_engine.database_impact.get('facilities_to_suppress', 0)
                )
                
                summary_gen = SummaryReportGenerator(self.rule_engine, metrics, output_dir)
                report_paths['summary'] = str(summary_gen.generate())
            
            return report_paths
            
        except Exception as e:
            logger.error(f"Report generation failed: {str(e)}")
            raise ReportGenerationError(f"Failed to generate reports: {str(e)}")
    
    def generate_universe_validation_report(self, output_path: str, universe_validator) -> None:
        """Generate universe validation report."""
        if universe_validator and universe_validator.validation_results:
            universe_validator.generate_universe_report(
                output_path, 
                universe_validator.validation_results
            )
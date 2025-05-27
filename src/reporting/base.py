"""Base classes for report generation."""

import csv
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict, Any, Iterator, Optional

from ..utils.logging_config import get_logger

logger = get_logger(__name__)


class BaseReportGenerator(ABC):
    """Base class for all report generators."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    @abstractmethod
    def generate(self) -> Path:
        """Generate the report and return the file path."""
        pass
    
    def write_csv_report(
        self, 
        filename: str, 
        headers: List[str], 
        data: Iterator[List[Any]],
        delimiter: str = ','
    ) -> Path:
        """
        Write CSV report with consistent formatting.
        
        Args:
            filename: Name of the output file
            headers: List of column headers
            data: Iterator of rows to write
            delimiter: CSV delimiter
            
        Returns:
            Path to the written file
        """
        output_path = self.output_dir / filename
        row_count = 0
        
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=delimiter)
                writer.writerow(headers)
                
                for row in data:
                    writer.writerow(row)
                    row_count += 1
                    
                    if row_count % 100000 == 0:
                        logger.debug(f"Written {row_count:,} rows to {filename}")
            
            logger.info(f"Report generated: {filename} ({row_count:,} rows)")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to write report {filename}: {str(e)}")
            raise
    
    def write_text_report(self, filename: str, content: str) -> Path:
        """Write text report with consistent formatting."""
        output_path = self.output_dir / filename
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logger.info(f"Report generated: {filename}")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to write report {filename}: {str(e)}")
            raise


class MetricsCalculator:
    """Base class for calculating and formatting metrics."""
    
    @staticmethod
    def calculate_percentage(count: int, total: int) -> float:
        """Calculate percentage with proper handling of edge cases."""
        if total == 0:
            return 0.0
        return round(count / total * 100, 2)
    
    @staticmethod
    def format_count_with_percentage(count: int, total: int) -> str:
        """Format count with percentage."""
        percentage = MetricsCalculator.calculate_percentage(count, total)
        return f"{count:,} ({percentage:.1f}%)"
    
    @staticmethod
    def format_metric_line(label: str, value: Any, indent: int = 2) -> str:
        """Format a metric line with consistent indentation."""
        return f"{' ' * indent}{label}: {value}"
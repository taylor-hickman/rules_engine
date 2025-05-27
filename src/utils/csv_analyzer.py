"""CSV universe analyzer for standalone analysis."""

import csv
from pathlib import Path
from typing import Dict, Any
import pandas as pd

from ..validation.npi import NPIValidator
from ..reporting.metrics import ProcessingMetrics, MetricsFormatter
from .logging_config import get_logger

logger = get_logger(__name__)


def analyze_csv_universe(csv_path: str, npi_column: str = 'npi') -> Dict[str, Any]:
    """
    Analyze CSV universe file without database connection.
    
    Args:
        csv_path: Path to CSV file
        npi_column: Column name containing NPIs
        
    Returns:
        Dictionary with analysis results
    """
    logger.info(f"Analyzing CSV universe: {csv_path}")
    
    try:
        # Read CSV file
        df = pd.read_csv(csv_path, dtype=str)
        
        if npi_column not in df.columns:
            raise ValueError(f"Column '{npi_column}' not found in CSV")
        
        logger.info(f"CSV loaded: {len(df):,} rows")
        
        # Validate NPIs
        validator = NPIValidator()
        valid_npis = set()
        invalid_count = 0
        
        for npi in df[npi_column]:
            clean_npi = validator.validate_and_clean(npi)
            if clean_npi:
                valid_npis.add(clean_npi)
            else:
                invalid_count += 1
        
        # Create metrics
        metrics = ProcessingMetrics(
            total_npis=len(valid_npis),
            practitioner_npis=len(valid_npis)  # Assume all are practitioners for CSV-only
        )
        
        # Log results
        logger.info("CSV Analysis Results:")
        logger.info(f"  Total rows: {len(df):,}")
        logger.info(f"  Valid NPIs: {len(valid_npis):,}")
        logger.info(f"  Invalid NPIs: {invalid_count:,}")
        logger.info(f"  Duplicates removed: {len(df) - invalid_count - len(valid_npis):,}")
        
        # Write analysis report
        output_path = Path(csv_path).parent / 'csv_analysis_report.txt'
        with open(output_path, 'w') as f:
            f.write("CSV Universe Analysis Report\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Source file: {csv_path}\n")
            f.write(f"Total rows: {len(df):,}\n")
            f.write(f"Valid NPIs: {len(valid_npis):,}\n")
            f.write(f"Invalid NPIs: {invalid_count:,}\n")
            f.write(f"Duplicates: {len(df) - invalid_count - len(valid_npis):,}\n")
        
        logger.info(f"Analysis report written to: {output_path}")
        
        return {
            'total_rows': len(df),
            'valid_npis': len(valid_npis),
            'invalid_npis': invalid_count,
            'duplicates': len(df) - invalid_count - len(valid_npis)
        }
        
    except Exception as e:
        logger.error(f"CSV analysis failed: {str(e)}")
        raise
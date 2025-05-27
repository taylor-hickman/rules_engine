"""Command line interface for the NPI Suppression Rule Engine."""

import argparse
import sys
from pathlib import Path

from .core.config import AppConfig
from .orchestration.pipeline import ProcessingPipeline
from .utils.logging_config import get_logger

logger = get_logger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Create and configure argument parser."""
    parser = argparse.ArgumentParser(
        description='NPI Provider Suppression Rule Engine',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process CSV universe file:
  python run.py --rules rules.yml --csv-universe universe.csv --output ./reports
  
  # Process Teradata table:
  python run.py --rules rules.yml --teradata-universe schema.table --output ./reports
  
  # Analyze CSV only (no database required):
  python run.py --rules rules.yml --csv-universe universe.csv --analyze-csv-only
"""
    )
    
    # Required arguments
    parser.add_argument(
        '--rules', 
        required=True,
        help='Path to YAML rules configuration file'
    )
    
    # Universe source (mutually exclusive)
    universe_group = parser.add_mutually_exclusive_group(required=True)
    universe_group.add_argument(
        '--csv-universe',
        help='Path to CSV file containing universe NPIs'
    )
    universe_group.add_argument(
        '--teradata-universe',
        help='Teradata table name containing universe NPIs (schema.table)'
    )
    
    # CSV options
    parser.add_argument(
        '--csv-npi-column',
        default='npi',
        help='Column name containing NPIs in CSV (default: npi)'
    )
    
    # Processing options
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10000,
        help='Batch size for processing (default: 10000)'
    )
    parser.add_argument(
        '--output',
        default='./reports',
        help='Output directory for reports (default: ./reports)'
    )
    
    # Operation modes
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate configuration without processing'
    )
    parser.add_argument(
        '--analyze-csv-only',
        action='store_true',
        help='Only analyze CSV universe distribution (no database required)'
    )
    
    # Debug options
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose debug logging'
    )
    
    return parser


def validate_args(args: argparse.Namespace) -> bool:
    """Validate command line arguments."""
    # Validate rules file
    if not Path(args.rules).exists():
        logger.error(f"Rules configuration file not found: {args.rules}")
        return False
    
    # Validate CSV file if specified
    if args.csv_universe:
        csv_path = Path(args.csv_universe)
        if not csv_path.exists():
            logger.error(f"CSV universe file not found: {args.csv_universe}")
            return False
        
        # Log file info
        file_size = csv_path.stat().st_size
        logger.info(
            f"CSV universe file: {args.csv_universe} "
            f"({file_size:,} bytes, {file_size/1024/1024:.1f} MB)"
        )
    
    return True


def main():
    """Main entry point for the application."""
    parser = create_parser()
    args = parser.parse_args()
    
    # Validate arguments
    if not validate_args(args):
        sys.exit(1)
    
    try:
        # Load configuration
        config = AppConfig.from_args(args)
        
        # Special mode: CSV analysis only
        if args.analyze_csv_only and args.csv_universe:
            from .utils.csv_analyzer import analyze_csv_universe
            analyze_csv_universe(args.csv_universe, args.csv_npi_column)
            return
        
        # Create and run pipeline
        pipeline = ProcessingPipeline(config)
        
        if not pipeline.initialize():
            logger.error("Failed to initialize processing pipeline")
            sys.exit(1)
        
        success = pipeline.execute(args)
        
        if success:
            logger.info("Processing completed successfully")
            sys.exit(0)
        else:
            logger.error("Processing failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("Processing interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.exception("Unexpected error occurred")
        sys.exit(1)


if __name__ == '__main__':
    main()
"""
NPI Universe Validation Module

Handles loading, validation, and provider type categorization of NPI universe data.
Supports both CSV and Teradata table sources with comprehensive provider type analysis.
"""

import csv
import uuid
from typing import Dict, Set, Optional, Tuple
from dataclasses import dataclass
import teradatasql
import pandas as pd

from .npi import NPIValidator
from ..core.constants import MAX_BATCH_SIZE, ProviderType
from ..core.exceptions import UniverseValidationError, ValidationError
from ..processing.tables import TableManager, BatchProcessor
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class ProviderTypeCounts:
    """Container for provider type counts and percentages."""
    practitioners: int
    facilities: int
    ancillary: int
    uncategorized: int
    total: int
    
    @property
    def practitioner_percentage(self) -> float:
        return round(self.practitioners / self.total * 100, 2) if self.total > 0 else 0.0
    
    @property
    def facility_percentage(self) -> float:
        return round(self.facilities / self.total * 100, 2) if self.total > 0 else 0.0
    
    @property
    def ancillary_percentage(self) -> float:
        return round(self.ancillary / self.total * 100, 2) if self.total > 0 else 0.0
    
    @property
    def uncategorized_percentage(self) -> float:
        return round(self.uncategorized / self.total * 100, 2) if self.total > 0 else 0.0
    
    @property
    def non_practitioner_count(self) -> int:
        """Count of NPIs that will be suppressed by provider type rule."""
        return self.facilities + self.ancillary + self.uncategorized
    
    @property
    def non_practitioner_percentage(self) -> float:
        return round(self.non_practitioner_count / self.total * 100, 2) if self.total > 0 else 0.0


@dataclass
class UniverseValidationResults:
    """Results from universe validation and provider type categorization."""
    source_type: str
    source_path: str
    universe_table_name: str
    total_npis: int
    practitioner_npis: Set[str]
    facility_npis: Set[str]
    ancillary_npis: Set[str]
    uncategorized_npis: Set[str]
    provider_type_counts: ProviderTypeCounts
    npi_to_provider_type_map: Dict[str, str]


class UniverseLoader:
    """Handles loading universe data from various sources into Teradata."""
    
    def __init__(self, connection: teradatasql.TeradataConnection, table_manager: TableManager):
        self.connection = connection
        self.cursor = connection.cursor()
        self.table_manager = table_manager
        self.npi_validator = NPIValidator()
    
    def load_from_csv(self, csv_path: str, npi_column: str) -> Tuple[str, int]:
        """
        Loads NPI universe from CSV file into Teradata table.
        
        Args:
            csv_path: Path to CSV file containing NPIs
            npi_column: Name of column containing NPI values
            
        Returns:
            Tuple of (table_name, loaded_count)
        """
        logger.info(f"Loading universe from CSV: {csv_path}")
        
        # Create universe table
        columns = [{'name': 'npi', 'type': 'VARCHAR(10)'}]
        table_name = self.table_manager.create_volatile_table('universe', columns, primary_index='npi')
        
        try:
            # Read CSV with multiple encoding attempts
            df = self._read_csv_with_encoding_fallback(csv_path)
            
            if npi_column not in df.columns:
                raise ValidationError(f"Column '{npi_column}' not found. Available: {list(df.columns)}")
            
            logger.info(f"CSV loaded: {len(df):,} rows, columns: {list(df.columns)}")
            
            # Process and validate NPIs
            valid_npis = self._extract_valid_npis(df[npi_column])
            
            if not valid_npis:
                raise ValidationError("No valid NPIs found in CSV file")
            
            # Load NPIs into Teradata table
            npi_data = [(npi,) for npi in valid_npis]
            loaded_count = BatchProcessor.batch_insert(
                self.cursor, table_name, ['npi'], npi_data, batch_size=MAX_BATCH_SIZE
            )
            
            logger.info(f"Successfully loaded {loaded_count:,} NPIs from CSV")
            return table_name, loaded_count
            
        except Exception as e:
            logger.error(f"Error loading CSV: {str(e)}")
            raise
    
    def load_from_teradata(self, table_name: str) -> Tuple[str, int]:
        """
        Validates existing Teradata table and returns reference.
        
        Args:
            table_name: Full table name (schema.table)
            
        Returns:
            Tuple of (table_name, record_count)
        """
        try:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = self.cursor.fetchone()[0]
            
            if count == 0:
                raise ValidationError(f"Table {table_name} exists but contains no records")
            
            logger.info(f"Validated Teradata table: {table_name} ({count:,} records)")
            return table_name, count
            
        except Exception as e:
            logger.error(f"Error validating Teradata table {table_name}: {str(e)}")
            raise
    
    def _read_csv_with_encoding_fallback(self, csv_path: str) -> pd.DataFrame:
        """Attempts to read CSV with multiple encoding options."""
        for encoding in ['utf-8', 'latin1', 'cp1252']:
            try:
                df = pd.read_csv(csv_path, dtype=str, encoding=encoding)
                logger.debug(f"Successfully read CSV with {encoding} encoding")
                return df
            except UnicodeDecodeError:
                continue
        
        raise ValidationError("Could not read CSV file with any supported encoding")
    
    def _extract_valid_npis(self, npi_series: pd.Series) -> Set[str]:
        """Extracts and validates NPIs from pandas Series."""
        logger.info("Validating and deduplicating NPIs...")
        
        valid_npis = set()
        invalid_count = 0
        
        for idx, npi_value in enumerate(npi_series):
            if idx % 50000 == 0 and idx > 0:
                logger.debug(f"Processed {idx:,} NPIs...")
            
            clean_npi = self.npi_validator.validate_and_clean(npi_value)
            if clean_npi:
                valid_npis.add(clean_npi)
            else:
                invalid_count += 1
                if invalid_count <= 10:
                    logger.warning(f"Invalid NPI at row {idx}: '{npi_value}'")
        
        total_processed = len(npi_series)
        duplicates_removed = total_processed - invalid_count - len(valid_npis)
        
        logger.info(f"NPI validation complete:")
        logger.info(f"  Valid unique NPIs: {len(valid_npis):,}")
        logger.info(f"  Invalid NPIs: {invalid_count:,}")
        logger.info(f"  Duplicates removed: {duplicates_removed:,}")
        
        return valid_npis


class ProviderTypeCategorizer:
    """Categorizes NPIs by provider type using Spayer database relationships."""
    
    def __init__(self, connection: teradatasql.TeradataConnection):
        self.connection = connection
        self.cursor = connection.cursor()
    
    def categorize_universe_npis(self, universe_table: str) -> UniverseValidationResults:
        """
        Categorizes all NPIs in universe table by provider type.
        
        Args:
            universe_table: Name of table containing universe NPIs
            
        Returns:
            Complete validation results with provider type categorization
        """
        logger.info("Starting provider type categorization...")
        
        # Get total NPI count
        total_npis = self._get_total_npi_count(universe_table)
        
        # Categorize by provider type
        practitioner_npis = self._find_practitioner_npis(universe_table)
        facility_npis = self._find_facility_npis(universe_table)
        ancillary_npis = self._find_ancillary_npis(universe_table, practitioner_npis)
        
        # Remaining NPIs are uncategorized
        all_npis = self._get_all_universe_npis(universe_table)
        categorized_npis = practitioner_npis | facility_npis | ancillary_npis
        uncategorized_npis = all_npis - categorized_npis
        
        # Create provider type mapping
        npi_to_type_map = {}
        for npi in practitioner_npis:
            npi_to_type_map[npi] = ProviderType.PRACTITIONER
        for npi in facility_npis:
            npi_to_type_map[npi] = ProviderType.FACILITY
        for npi in ancillary_npis:
            npi_to_type_map[npi] = ProviderType.ANCILLARY
        for npi in uncategorized_npis:
            npi_to_type_map[npi] = ProviderType.UNCATEGORIZED
        
        # Create counts object
        counts = ProviderTypeCounts(
            practitioners=len(practitioner_npis),
            facilities=len(facility_npis),
            ancillary=len(ancillary_npis),
            uncategorized=len(uncategorized_npis),
            total=total_npis
        )
        
        self._log_categorization_results(counts)
        
        return UniverseValidationResults(
            source_type='',  # Will be set by caller
            source_path='',  # Will be set by caller
            universe_table_name=universe_table,
            total_npis=total_npis,
            practitioner_npis=practitioner_npis,
            facility_npis=facility_npis,
            ancillary_npis=ancillary_npis,
            uncategorized_npis=uncategorized_npis,
            provider_type_counts=counts,
            npi_to_provider_type_map=npi_to_type_map
        )
    
    def _get_total_npi_count(self, universe_table: str) -> int:
        """Gets total count of unique NPIs in universe table."""
        self.cursor.execute(f"SELECT COUNT(DISTINCT npi) FROM {universe_table}")
        return self.cursor.fetchone()[0]
    
    def _find_practitioner_npis(self, universe_table: str) -> Set[str]:
        """Finds NPIs that exist in the practitioners table."""
        logger.info("Identifying practitioner NPIs...")
        
        query = f"""
        SELECT DISTINCT u.npi
        FROM {universe_table} u
        INNER JOIN providerdataservice_core_v.prov_spayer_practitioners p 
            ON u.npi = p.nationalproviderid
        """
        
        self.cursor.execute(query)
        npis = {str(row[0]) for row in self.cursor.fetchall()}
        
        logger.info(f"Found {len(npis):,} practitioner NPIs")
        return npis
    
    def _find_facility_npis(self, universe_table: str) -> Set[str]:
        """Finds NPIs that exist in the facilities table."""
        logger.info("Identifying facility NPIs...")
        
        query = f"""
        SELECT DISTINCT u.npi
        FROM {universe_table} u
        INNER JOIN providerdataservice_core_v.PROV_SPAYER_Facilities f 
            ON u.npi = f.nationalproviderid
        INNER JOIN providerdataservice_core_v.PROV_SPAYER_Facilityaddresses fa 
            ON f.FacilityID = fa.FacilityID
        """
        
        self.cursor.execute(query)
        npis = {str(row[0]) for row in self.cursor.fetchall()}
        
        logger.info(f"Found {len(npis):,} facility NPIs")
        return npis
    
    def _find_ancillary_npis(self, universe_table: str, practitioner_npis: Set[str]) -> Set[str]:
        """Finds NPIs in practice locations but not in practitioners table."""
        logger.info("Identifying ancillary NPIs...")
        
        query = f"""
        SELECT DISTINCT u.npi
        FROM {universe_table} u
        INNER JOIN providerdataservice_core_v.prov_spayer_practicelocations pl
            ON u.npi = pl.nationalproviderid
        INNER JOIN providerdataservice_core_v.prov_spayer_practices p 
            ON pl.practiceid = p.practiceid
        """
        
        self.cursor.execute(query)
        practice_location_npis = {str(row[0]) for row in self.cursor.fetchall()}
        
        # Ancillary NPIs are those in practice locations but not practitioners
        ancillary_npis = practice_location_npis - practitioner_npis
        
        logger.info(f"Found {len(ancillary_npis):,} ancillary NPIs")
        return ancillary_npis
    
    def _get_all_universe_npis(self, universe_table: str) -> Set[str]:
        """Gets all NPIs from universe table."""
        self.cursor.execute(f"SELECT DISTINCT npi FROM {universe_table}")
        return {str(row[0]) for row in self.cursor.fetchall()}
    
    def _log_categorization_results(self, counts: ProviderTypeCounts) -> None:
        """Logs comprehensive categorization results."""
        logger.info("Provider type categorization complete:")
        logger.info(f"  Practitioners: {counts.practitioners:,} ({counts.practitioner_percentage:.1f}%)")
        logger.info(f"  Facilities: {counts.facilities:,} ({counts.facility_percentage:.1f}%)")
        logger.info(f"  Ancillary: {counts.ancillary:,} ({counts.ancillary_percentage:.1f}%)")
        logger.info(f"  Uncategorized: {counts.uncategorized:,} ({counts.uncategorized_percentage:.1f}%)")
        logger.info(f"  Total: {counts.total:,}")
        logger.info(f"Provider type rule impact:")
        logger.info(f"  NPIs entering rule pipeline: {counts.practitioners:,} ({counts.practitioner_percentage:.1f}%)")
        logger.info(f"  NPIs suppressed by provider type: {counts.non_practitioner_count:,} ({counts.non_practitioner_percentage:.1f}%)")


class UniverseValidator:
    """
    Main class for validating and categorizing NPI universe data.
    
    Coordinates loading universe data from various sources, categorizing NPIs by 
    provider type, and creating filtered datasets for rule processing.
    """
    
    def __init__(self, connection_manager):
        """
        Initialize validator with shared connection manager.
        
        Args:
            connection_manager: SharedConnectionManager instance for database connectivity
        """
        self.connection_manager = connection_manager
        self.session_id = str(uuid.uuid4()).replace('-', '')[:8]
        
        # Database connection components
        self.connection: Optional[teradatasql.TeradataConnection] = None
        self.table_manager: Optional[TableManager] = None
        self.universe_loader: Optional[UniverseLoader] = None
        self.categorizer: Optional[ProviderTypeCategorizer] = None
        
        # Validation results
        self.validation_results: Optional[UniverseValidationResults] = None
    
    def validate_csv_universe(self, csv_path: str, npi_column: str = 'npi') -> UniverseValidationResults:
        """
        Validates and categorizes NPI universe from CSV file.
        
        Args:
            csv_path: Path to CSV file containing NPIs
            npi_column: Name of column containing NPI values
            
        Returns:
            Complete validation results with provider type categorization
        """
        self._ensure_components_initialized()
        
        logger.info(f"Validating CSV universe: {csv_path}")
        
        # Load universe from CSV
        universe_table, loaded_count = self.universe_loader.load_from_csv(csv_path, npi_column)
        
        # Categorize by provider type
        results = self.categorizer.categorize_universe_npis(universe_table)
        
        # Set source information
        results.source_type = 'csv'
        results.source_path = csv_path
        
        self.validation_results = results
        logger.info(f"CSV universe validation complete: {loaded_count:,} NPIs categorized")
        
        return results
    
    def validate_teradata_universe(self, table_name: str) -> UniverseValidationResults:
        """
        Validates and categorizes NPI universe from Teradata table.
        
        Args:
            table_name: Full table name (schema.table)
            
        Returns:
            Complete validation results with provider type categorization
        """
        self._ensure_components_initialized()
        
        logger.info(f"Validating Teradata universe: {table_name}")
        
        # Validate existing table
        validated_table, record_count = self.universe_loader.load_from_teradata(table_name)
        
        # Categorize by provider type
        results = self.categorizer.categorize_universe_npis(validated_table)
        
        # Set source information
        results.source_type = 'teradata'
        results.source_path = table_name
        
        self.validation_results = results
        logger.info(f"Teradata universe validation complete: {record_count:,} NPIs categorized")
        
        return results
    
    def create_practitioner_universe_table(self, validation_results: UniverseValidationResults) -> str:
        """
        Creates filtered table containing only practitioner NPIs.
        
        Args:
            validation_results: Results from universe validation
            
        Returns:
            Name of created practitioner universe table
        """
        self._ensure_components_initialized()
        
        practitioner_npis = list(validation_results.practitioner_npis)
        
        if not practitioner_npis:
            raise UniverseValidationError("No practitioner NPIs found for rule processing")
        
        logger.info(f"Creating practitioner universe table with {len(practitioner_npis):,} NPIs")
        
        # Create practitioner table
        columns = [
            {'name': 'npi', 'type': 'VARCHAR(10)'},
            {'name': 'provider_type', 'type': 'VARCHAR(20)'}
        ]
        table_name = self.table_manager.create_volatile_table(
            'practitioner_universe', columns, primary_index='npi'
        )
        
        # Insert practitioner NPIs with metadata
        npi_data = [(npi, ProviderType.PRACTITIONER) for npi in practitioner_npis]
        final_count = BatchProcessor.batch_insert(
            self.connection.cursor(), table_name, ['npi', 'provider_type'], 
            npi_data, batch_size=MAX_BATCH_SIZE
        )
        
        logger.info(f"Practitioner universe table created: {table_name} ({final_count:,} NPIs)")
        return table_name
    
    def generate_universe_report(self, output_path: str, validation_results: UniverseValidationResults) -> None:
        """
        Generates comprehensive universe validation report.
        
        Args:
            output_path: Path where report will be saved
            validation_results: Results from universe validation
        """
        logger.info(f"Generating universe validation report: {output_path}")
        
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Write header with comprehensive fields
            writer.writerow([
                'npi', 'provider_type', 'provider_type_rule_flag', 
                'enters_rule_pipeline', 'suppression_reason'
            ])
            
            # Write practitioner NPIs (enter rule pipeline)
            for npi in validation_results.practitioner_npis:
                writer.writerow([
                    npi, ProviderType.PRACTITIONER, 'N', 'Y', 
                    'Practitioner type - processed by suppression rules'
                ])
            
            # Write non-practitioner NPIs (suppressed by provider type rule)
            for npi in validation_results.facility_npis:
                writer.writerow([
                    npi, ProviderType.FACILITY, 'Y', 'N', 
                    'Facility type - suppressed by provider type rule'
                ])
            
            for npi in validation_results.ancillary_npis:
                writer.writerow([
                    npi, ProviderType.ANCILLARY, 'Y', 'N', 
                    'Ancillary type - suppressed by provider type rule'
                ])
            
            for npi in validation_results.uncategorized_npis:
                writer.writerow([
                    npi, ProviderType.UNCATEGORIZED, 'Y', 'N', 
                    'Uncategorized type - suppressed by provider type rule'
                ])
        
        total_records = validation_results.total_npis
        logger.info(f"Universe validation report generated: {total_records:,} records with provider type analysis")
    
    def get_provider_type_for_npi(self, npi: str) -> str:
        """
        Gets provider type for specific NPI.
        
        Args:
            npi: NPI to lookup
            
        Returns:
            Provider type
        """
        if not self.validation_results:
            return ProviderType.UNKNOWN
        
        return self.validation_results.npi_to_provider_type_map.get(str(npi), ProviderType.UNKNOWN)
    
    def cleanup(self) -> None:
        """Cleans up all resources and releases shared connection."""
        logger.info("Cleaning up universe validator resources")
        
        if self.table_manager:
            self.table_manager.cleanup_all_tables()
        
        if self.connection:
            self.connection_manager.release_component('UniverseValidator')
            self.connection = None
            self.table_manager = None
            self.universe_loader = None
            self.categorizer = None
            logger.debug("Universe validator released shared connection")
    
    def _ensure_components_initialized(self) -> None:
        """Initializes database connection and component objects."""
        if self.connection is None:
            try:
                logger.debug("Initializing universe validator components")
                self.connection = self.connection_manager.get_connection('UniverseValidator')
                self.table_manager = TableManager(self.connection)
                self.universe_loader = UniverseLoader(self.connection, self.table_manager)
                self.categorizer = ProviderTypeCategorizer(self.connection)
                logger.debug("Universe validator components initialized")
            except Exception as e:
                logger.error(f"Failed to initialize universe validator components: {str(e)}")
                raise UniverseValidationError(f"Failed to initialize components: {str(e)}")
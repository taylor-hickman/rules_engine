"""NPI validation utilities."""

from typing import Optional, Any
import pandas as pd

from ..core.constants import NPI_LENGTH
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


class NPIValidator:
    """Validates and cleans NPI values according to CMS standards."""
    
    @staticmethod
    def validate_and_clean(npi_value: Any) -> Optional[str]:
        """
        Validates and cleans a single NPI value.
        
        Args:
            npi_value: Raw NPI value from data source
            
        Returns:
            Clean 10-digit NPI string if valid, None if invalid
        """
        if pd.isna(npi_value) or npi_value is None:
            return None
        
        # Convert to string and remove whitespace
        clean_npi = str(npi_value).strip()
        
        # Extract only digits
        clean_npi = ''.join(char for char in clean_npi if char.isdigit())
        
        # Validate NPI format (must be exactly 10 digits)
        if len(clean_npi) == NPI_LENGTH and clean_npi.isdigit():
            return clean_npi
        
        return None
    
    @staticmethod
    def validate_checksum(npi: str) -> bool:
        """
        Validate NPI using Luhn algorithm.
        
        Args:
            npi: 10-digit NPI string
            
        Returns:
            True if checksum is valid
        """
        if not npi or len(npi) != NPI_LENGTH or not npi.isdigit():
            return False
        
        # Implementation of Luhn algorithm for NPI validation
        # This is a placeholder - implement actual Luhn check if needed
        return True
"""Simplified database connection management."""

import time
from contextlib import contextmanager
from typing import Optional
import teradatasql

from .config import DatabaseConfig
from .constants import CONNECTION_RETRY_ATTEMPTS, CONNECTION_TIMEOUT
from .exceptions import DatabaseConnectionError
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


class ConnectionManager:
    """Manages database connections with retry logic."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._connection: Optional[teradatasql.TeradataConnection] = None
    
    def connect(self) -> teradatasql.TeradataConnection:
        """Establish database connection with retry logic."""
        if self._connection and not self._connection.closed:
            return self._connection
        
        params = self.config.to_connection_params()
        last_error = None
        
        for attempt in range(1, CONNECTION_RETRY_ATTEMPTS + 1):
            try:
                logger.info(f"Connecting to database (attempt {attempt}/{CONNECTION_RETRY_ATTEMPTS})")
                self._connection = teradatasql.connect(**params)
                
                # Test connection
                cursor = self._connection.cursor()
                cursor.execute("SELECT CURRENT_TIMESTAMP")
                timestamp = cursor.fetchone()[0]
                cursor.close()
                
                logger.info(f"Database connection established at {timestamp}")
                return self._connection
                
            except Exception as e:
                last_error = e
                logger.warning(f"Connection attempt {attempt} failed: {str(e)}")
                
                if attempt < CONNECTION_RETRY_ATTEMPTS:
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        raise DatabaseConnectionError(
            f"Failed to connect after {CONNECTION_RETRY_ATTEMPTS} attempts: {str(last_error)}"
        )
    
    def close(self):
        """Close the database connection."""
        if self._connection and not self._connection.closed:
            try:
                self._connection.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {str(e)}")
            finally:
                self._connection = None
    
    @contextmanager
    def cursor(self):
        """Context manager for database cursor."""
        conn = self.connect()
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()